package org.example.apache_beam.pipelines;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.example.apache_beam.options.GcsToGcsOptions;
import org.example.apache_beam.task.GcsToGcsPipelineTask;
import org.example.apache_beam.task.GenericTextAnalyzerTask;

public class GcsToGcsTextAnalyzerPipeline {

    public static void main(String[] args) {
        GcsToGcsOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(GcsToGcsOptions.class);
        // Flex Templates for watching buckets must be in streaming mode
        options.setStreaming(true);

        Pipeline pipeline = Pipeline.create(options);

        GenericTextAnalyzerTask task = new GcsToGcsPipelineTask(options.getInputPath(), options.getOutputPath(),
                options.getProcessedPath(), options.getPollInterval());
        task.setUpPipeline(pipeline);
        task.runJob(pipeline);
    }

}
