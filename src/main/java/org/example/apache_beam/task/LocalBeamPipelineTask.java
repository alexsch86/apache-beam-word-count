package org.example.apache_beam.task;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.initialization.qual.Initialized;

import java.net.URL;
import java.util.Arrays;

import static java.util.Objects.isNull;

public class LocalBeamPipelineTask extends GenericTextAnalyzerTask {

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

    @Override
    public void setUpPipeline(Pipeline pipeline) {
        PCollection<String> lines = readLines(pipeline);
        applyTransformationAndWriteToOutput(lines);
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

    private PCollection<String> readLines(Pipeline pipeline) {
        return pipeline.apply("(1) Read all lines", TextIO.read().from(inputFilePath));
    }

}
