package org.example.apache_beam.task;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.lang3.StringUtils;
import org.example.apache_beam.options.GcsToGcsOptions;
import org.joda.time.Duration;
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

    private final Integer pollInterval;

    public GcsToGcsPipelineTask(String inputPath, String outputPath,
                                String processedPath, Integer pollInterval) {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        this.processedPath = processedPath;
        this.pollInterval = pollInterval;
    }

    @Override
    protected PCollection<String> readLines(Pipeline pipeline) {

        // 1. WATCH and MATCH: Monitor GCS for new files
        PCollection<FileIO.ReadableFile> matchedFiles = pipeline
                .apply("WatchGCS", FileIO.match()
                        .filepattern(this.inputPath)
                        .continuously(Duration.standardSeconds(this.pollInterval), Watch.Growth.never()))   // streaming mode
                .apply("ReadMatches", FileIO.readMatches());

        // 2. PROCESS: Read the contents of the matched files
        PCollection<String> readLines = matchedFiles.apply("ReadLines", TextIO.readFiles());

        // 3. MOVE the files to 'processed' folder after they are read
        matchedFiles.apply("MoveToProcessed", ParDo.of(new DoFn<FileIO.ReadableFile, Void>() {
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

        return readLines;
    }

    @Override
    protected void applyTransformationAndWriteToOutput(PCollection<String> lines) {
        lines.apply("ApplyWindow", Window.into(FixedWindows.of(Duration.standardMinutes(1)))) // CRITICAL: Slice the infinite stream into 1-minute windows to allow aggregation
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
                        .via(count -> count.getKey() + " -> " + count.getValue()))
                // Note: In a streaming pipeline, TextIO.write() creates many small files.
                // Usually, you would write to BigQuery or a specific timestamped GCS path.
                .apply("WriteResults", TextIO.write().to(this.outputPath + "results/run")
                        .withWindowedWrites());
    }
}
