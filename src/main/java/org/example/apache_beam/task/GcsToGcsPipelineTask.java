package org.example.apache_beam.task;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.lang3.StringUtils;
import org.example.apache_beam.options.GcsToGcsOptions;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

public class GcsToGcsPipelineTask extends GenericTextAnalyzerTask {

    private static final Logger LOG = LoggerFactory.getLogger(GcsToGcsPipelineTask.class);

    private final String inputPath;

    private final String outputPath;

    private final String processedPath;

    public GcsToGcsPipelineTask(String inputPath, String outputPath,
                                String processedPath) {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        this.processedPath = processedPath;
    }

    @Override
    public void setUpPipeline(Pipeline pipeline) {
        // 1. WATCH and MATCH: Monitor GCS for new files
        PCollection<FileIO.ReadableFile> matchedFiles = watchAndMonitorFiles(pipeline);

        // 2. PROCESS: Read the contents of the matched files and analyze them
        PCollection<String> stringFinalResults = matchedFiles.apply("ReadLines", TextIO.readFiles())
//                .apply("ApplyWindow", Window.into(FixedWindows.of(Duration.standardMinutes(1)))) // used for slicing the infinite stream into 1-minute windows to allow aggregation
                .apply("FilterEmpty", Filter.by((String line) -> !line.isEmpty()))
                .apply("(2) Flatmap to a list of words", FlatMapElements.into(TypeDescriptors.strings())
                        .via(line -> Arrays.asList(line.split("\\s"))))
                .apply("(3) Lowercase all", MapElements.into(TypeDescriptors.strings())
                        .via(String::toLowerCase))
                .apply("(4) Trim whitespaces", MapElements.into(TypeDescriptors.strings())
                        .via(StringUtils::trim))
                .apply("Remove punctuation", ParDo.of(new RemovePunctuationFn()))
                .apply("(6) Count words", Count.perElement())
                .apply("Show each word count", MapElements.into(TypeDescriptors.strings())
                        .via(count -> count.getKey() + " -> " + count.getValue()));

        // Usually, you would write to BigQuery or a specific timestamped GCS path. This is the next step
        stringFinalResults.apply("WriteResults", TextIO.write().to(this.outputPath + "results/run"));

        // 3. MOVE the files to 'processed' folder after they are analyzed
        moveProcessedFiles(matchedFiles, stringFinalResults);
    }

    private @NonNull PCollection<FileIO.ReadableFile> watchAndMonitorFiles(Pipeline pipeline) {
        return pipeline
                .apply("WatchGCS", FileIO.match().filepattern(this.inputPath))   // streaming mode
                .apply("ReadMatches", FileIO.readMatches());
    }

    private static void moveProcessedFiles(PCollection<FileIO.ReadableFile> matchedFiles, PCollection<String> writeDone) {
        matchedFiles
                .apply("WaitForWriteDone", Wait.on(writeDone))
                .apply("MoveToProcessed", ParDo.of(new DoFn<FileIO.ReadableFile, Void>() {
                    @ProcessElement
                    public void processElement(@Element FileIO.ReadableFile file, ProcessContext c) {
                        String sourceStr = file.getMetadata().resourceId().toString();
                        String fileName = file.getMetadata().resourceId().getFilename();
                        String destinationStr = c.getPipelineOptions().as(GcsToGcsOptions.class).getProcessedPath() + fileName;

                        try {
                            ResourceId source = FileSystems.matchNewResource(sourceStr, false);
                            ResourceId dest = FileSystems.matchNewResource(destinationStr, false);

                            LOG.info("Relocating {} to {}", sourceStr, destinationStr);

                            // GCS Move = Copy + Delete
                            FileSystems.copy(Collections.singletonList(source), Collections.singletonList(dest));
                            FileSystems.delete(Collections.singletonList(source));
                        } catch (IOException e) {
                            LOG.error("Failed to move file {}: {}", sourceStr, e.getMessage());
                        }
                    }
                }));
    }
}
