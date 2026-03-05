package org.example.apache_beam.runner;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.example.apache_beam.task.BeamPipelineTask;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
public class BeamTaskStartupRunner implements CommandLineRunner {

    private final BeamPipelineTask beamPipelineTask;

    // This map will automatically be populated with all key-value pairs nested under 'cloud.options'
    private Map<String, String> options;

    public BeamTaskStartupRunner(@Autowired BeamPipelineTask beamPipelineTask) {
        this.beamPipelineTask = beamPipelineTask;
    }

    public void setOptions(Map<String, String> options) {
        this.options = options;
    }

    private String[] buildArgsFromConfig() {
        if (options == null || options.isEmpty()) {
            return new String[0];
        }

        List<String> argsList = new ArrayList<>();
        options.forEach((key, value) -> {
            if (value != null) {
                // Formats as --key=value
                argsList.add("--" + key + "=" + value);
            }
        });
        return argsList.toArray(new String[0]);
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
