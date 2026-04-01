package org.example.apache_beam.task;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.Strings;

import java.io.Serializable;
import java.util.Set;

public abstract class GenericTextAnalyzerTask implements Serializable {

    private static final Set<String> STOP_WORDS = Set.of(".", "!", "?", ",", ";", ":", "\"", "'", "(", ")");

    public void runJob(Pipeline pipeline) {
        pipeline.run();
    }

    public void setUpPipeline(Pipeline pipeline) {
        PCollection<String> lines = readLines(pipeline);
        applyTransformationAndWriteToOutput(lines);
    }

    protected abstract PCollection<String> readLines(Pipeline pipeline);

    protected abstract void applyTransformationAndWriteToOutput(PCollection<String> lines);

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
