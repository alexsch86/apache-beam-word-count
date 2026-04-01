package org.example.apache_beam.pipelines;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.example.apache_beam.options.TextToTextAnalyzerLocalOptions;
import org.example.apache_beam.task.GenericTextAnalyzerTask;
import org.example.apache_beam.task.LocalBeamPipelineTask;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LocalTextAnalyzerToTextPipeline {

    public static void main(String[] args) {
        String[] pipelineArgs = args;

        // Check if a specific parameter file was passed via command line
        // Usage: java -jar ... --parameters-file=localParams.json
        for (String arg : args) {
            if (arg.startsWith("--parameters-file=")) {
                String filePath = arg.split("=")[1];
                pipelineArgs = loadArgsFromJson(filePath, args);
                break;
            }
        }

        TextToTextAnalyzerLocalOptions options = PipelineOptionsFactory.fromArgs(pipelineArgs)
                .withValidation()
                .as(TextToTextAnalyzerLocalOptions.class);
        resolveDynamicOutputPath(options);

        runPipeline(options);
    }

    private static void runPipeline(TextToTextAnalyzerLocalOptions options) {
        Pipeline pipeline = Pipeline.create(options);

        GenericTextAnalyzerTask task = new LocalBeamPipelineTask(options.getInputPath(), options.getOutputPath());
        task.setUpPipeline(pipeline);
        task.runJob(pipeline);
    }

    private static void resolveDynamicOutputPath(TextToTextAnalyzerLocalOptions options) {
        String input = options.getInputPath();
        String output = options.getOutputPath();

        // Regex to get the filename without extension
        // Pattern matches everything after the last slash until the last dot
        Pattern pattern = Pattern.compile("([^/\\\\]+)\\.[^.]+$");
        Matcher matcher = pattern.matcher(input);

        String fileName = "output"; // default fallback
        if (matcher.find()) {
            fileName = matcher.group(1);
        }

        // If the output path is a directory (ends in /), append the filename
        if (output.endsWith("/") || output.endsWith("\\")) {
            options.setOutputPath(output + fileName + ".txt");
        } else if (output.contains("{filename}")) { // Or you can support a placeholder like {filename} in your JSON
            options.setOutputPath(output.replace("{filename}", fileName));
        }
    }

    private static String[] loadArgsFromJson(String path, String[] originalArgs) {
        List<String> newArgs = new ArrayList<>();
        try {
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> map = mapper.readValue(new File(path), Map.class);

            map.forEach((key, value) -> newArgs.add("--" + key + "=" + value));

            // Append any original CLI args to allow manual overrides
            for (String arg : originalArgs) {
                if (!arg.startsWith("--parameters-file=")) {
                    newArgs.add(arg);
                }
            }
        } catch (Exception e) {
            System.err.println("Error loading config file: " + path + " -> exception " + e.getMessage());
            return originalArgs;
        }
        return newArgs.toArray(new String[0]);
    }

}
