package org.example.apache_beam.runner;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.example.apache_beam.task.BeamPipelineTask;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class BeamTaskStartupRunner implements CommandLineRunner {

    private final BeamPipelineTask beamPipelineTask;

    public BeamTaskStartupRunner(@Autowired BeamPipelineTask beamPipelineTask) {
        this.beamPipelineTask = beamPipelineTask;
    }

    private Pipeline createPipeline() {
        PipelineOptions options = PipelineOptionsFactory.create();
        return Pipeline.create(options);
    }

    @Override
    public void run(String... args) {
        Pipeline pipeline = createPipeline();
        beamPipelineTask.setUpPipeline(pipeline);
        beamPipelineTask.runJob(pipeline);
    }
}
