package org.example.apache_beam.pipelines;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.example.apache_beam.options.GcsToBqOptions;
import org.example.apache_beam.task.GcsToBqPipelineTask;
import org.example.apache_beam.task.GenericTextAnalyzerTask;

public class GcsToBqTextAnalyzerPipeline {

    public static void main(String[] args) {
        GcsToBqOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(GcsToBqOptions.class);

        options.setStreaming(false);

        Pipeline pipeline = Pipeline.create(options);

        GenericTextAnalyzerTask task = new GcsToBqPipelineTask(
                options.getInputPath(),
                options.getOutputTable(),
                options.getProcessedPath()
        );

        task.setUpPipeline(pipeline);
        task.runJob(pipeline);
    }
}