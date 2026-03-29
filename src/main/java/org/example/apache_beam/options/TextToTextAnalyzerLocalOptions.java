package org.example.apache_beam.options;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

public interface TextToTextAnalyzerLocalOptions extends DataflowPipelineOptions {

    @Description("Path of the file to analyze text from")
    @Validation.Required
    String getInputPath();

    void setInputPath(String value);

    @Description("Path of the file to write to")
    @Validation.Required
    String getOutputPath();

    void setOutputPath(String value);

}
