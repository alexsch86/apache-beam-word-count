package org.example.apache_beam;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.example.apache_beam.task.BeamPipelineTask;

public class LocalTextAnalyzerToTextPipeline {

	public static void main(String[] args) {
		TextToTextAnalyzerLocalOptions options = PipelineOptionsFactory.fromArgs(args)
				.withValidation()
				.as(TextToTextAnalyzerLocalOptions.class);

		runPipeline(options);
	}

	private static void runPipeline(TextToTextAnalyzerLocalOptions options) {
		Pipeline pipeline = Pipeline.create(options);

		BeamPipelineTask task = new BeamPipelineTask(options.getInputPath(), options.getOutputPath());
		task.setUpPipeline(pipeline);
		task.runJob(pipeline);
	}

}
