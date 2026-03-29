package org.example.apache_beam.task;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.checkerframework.checker.initialization.qual.Initialized;

import java.io.Serializable;
import java.net.URL;
import java.util.Arrays;
import java.util.Set;

import static java.util.Objects.isNull;

public class LocalBeamPipelineTask implements Serializable {

    private static final Set<String> STOP_WORDS = Set.of(".", "!", "?", ",", ";", ":", "\"", "'", "(", ")");

    private final String inputFilePath;

    private final String outputFilePath;

    public LocalBeamPipelineTask(@Initialized String inputFilePath,
                                 @Initialized String outputFilePath) {
        URL inputResource = getClass().getClassLoader().getResource(inputFilePath);
        if (isNull(inputResource)) {
            throw new IllegalArgumentException("File " + inputFilePath + " not found in resources!");
        }
        this.inputFilePath = inputResource.getPath();
        this.outputFilePath = outputFilePath;
    }

    public void setUpPipeline(Pipeline pipeline) {
        PCollection<String> lines = readLines(pipeline);
        applyTransformationAndWriteToOutput(lines);
    }

    public void runJob(Pipeline pipeline) {
        pipeline.run();
    }

    private PCollection<String> readLines(Pipeline pipeline) {
        return pipeline.apply("(1) Read all lines", TextIO.read().from(inputFilePath));
    }

    private void applyTransformationAndWriteToOutput(PCollection<String> lines) {
        lines.apply("(2) Flatmap to a list of words", FlatMapElements.into(TypeDescriptors.strings())
                        .via(line -> Arrays.asList(line.split("\\s"))))
                .apply("(3) Lowercase all", MapElements.into(TypeDescriptors.strings())
                        .via(String::toLowerCase))
                .apply("(4) Trim whitespaces", MapElements.into(TypeDescriptors.strings())
                        .via(StringUtils::trim))
                .apply("Remove punctuation", ParDo.of(new RemovePunctuationFn()))
                .apply("(6) Count words", Count.perElement())
                .apply("Show each word count", MapElements.into(TypeDescriptors.strings())
                        .via(count -> count.getKey() + " -> " + count.getValue()))
                .apply("Write to output file", TextIO.write().to(outputFilePath));
    }

    static class RemovePunctuationFn extends DoFn<String, String> {

        @ProcessElement
        public void processElement(@Element String word, OutputReceiver<String> out) {
            // remove all punctuation
            if (!word.isEmpty()) {
                for (String stopWord : STOP_WORDS) {
                    word = Strings.CI.remove(word, stopWord);
                }
                out.output(word.toLowerCase());
            }
        }
    }

}
