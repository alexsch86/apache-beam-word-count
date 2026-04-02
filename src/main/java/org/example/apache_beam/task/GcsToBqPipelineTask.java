package org.example.apache_beam.task;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class GcsToBqPipelineTask extends GenericTextAnalyzerTask {

    private static final Logger LOG = LoggerFactory.getLogger(GcsToBqPipelineTask.class);

    public static final String FILENAME_FIELD = "filename";
    public static final String WORD_FIELD = "word";
    public static final String COUNT_FIELD = "count";

    private final String inputPath;
    private final String outputTable;
    private final String processedPath;

    public GcsToBqPipelineTask(String inputPath, String outputTable, String processedPath) {
        this.inputPath = inputPath;
        this.outputTable = outputTable;
        this.processedPath = processedPath;
    }

    @Override
    public void setUpPipeline(Pipeline pipeline) {
        // 1. Match and Read files while preserving filename
        PCollection<KV<String, String>> fileLines = pipeline
                .apply("MatchFiles", FileIO.match().filepattern(inputPath))
                .apply("ReadMatches", FileIO.readMatches())
                .apply("ReadLinesWithFilenames", ParDo.of(new DoFn<FileIO.ReadableFile, KV<String, String>>() {
                    @ProcessElement
                    public void processElement(@Element FileIO.ReadableFile file, OutputReceiver<KV<String, String>> out) {
                        String fileName = file.getMetadata().resourceId().getFilename();
                        try (BufferedReader reader = new BufferedReader(new InputStreamReader(Channels.newInputStream(file.open())))) {
                            reader.lines().forEach(line -> out.output(KV.of(fileName, line)));
                        } catch (Exception e) {
                            LOG.error("Error reading file {}: {}", fileName, e.getMessage());
                        }
                    }
                }));

        // 2. Analyze words while keeping filename context
        PCollection<TableRow> tableRows = fileLines
                .apply("FilterEmpty", Filter.by(kv -> !kv.getValue().isEmpty()))
                .apply("ExtractWords", FlatMapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                        .via(kv -> {
                            String fileName = kv.getKey();
                            return Arrays.stream(kv.getValue().split("\\s+"))
                                    .map(word -> KV.of(fileName, word))
                                    .collect(Collectors.toList());
                        }))
                .apply("CleanWords", MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                        .via(kv -> KV.of(kv.getKey(), StringUtils.trim(kv.getValue().toLowerCase()))))
                .apply("RemovePunctuation", ParDo.of(new DoFn<KV<String, String>, KV<String, String>>() {
                    @ProcessElement
                    public void processElement(@Element KV<String, String> kv, OutputReceiver<KV<String, String>> out) {
                        String word = kv.getValue().replaceAll("[^a-zA-Z0-9]", "");
                        if (!word.isEmpty()) {
                            out.output(KV.of(kv.getKey(), word));
                        }
                    }
                }))
                // Count globally based on the KV(filename, word)
                .apply("CountPerFileAndWord", Count.perElement())
                .apply("MapToTableRow", MapElements.into(TypeDescriptor.of(TableRow.class))
                        .via(kvCount -> {
                            String fileName = kvCount.getKey().getKey();
                            String word = kvCount.getKey().getValue();
                            Long count = kvCount.getValue();
                            return new TableRow()
                                    .set(WORD_FIELD, word)
                                    .set(COUNT_FIELD, count)
                                    .set(FILENAME_FIELD, fileName);
                        }));

        // 3. Write to BigQuery with Clustering
        tableRows.apply("WriteToBQ", BigQueryIO.writeTableRows()
                .to(outputTable)
                .withSchema(getTableSchema())
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                // We use Clustering instead of Partitioning for the filename string
                .withClustering(new com.google.api.services.bigquery.model.Clustering()
                        .setFields(List.of(FILENAME_FIELD))));
    }

    private TableSchema getTableSchema() {
        return new TableSchema().setFields(Arrays.asList(
                new TableFieldSchema().setName(WORD_FIELD).setType("STRING").setMode("REQUIRED"),
                new TableFieldSchema().setName(COUNT_FIELD).setType("INTEGER").setMode("REQUIRED"),
                new TableFieldSchema().setName(FILENAME_FIELD).setType("STRING").setMode("REQUIRED")
        ));
    }
}