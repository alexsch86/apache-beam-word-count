package org.example.apache_beam.options;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

public interface GcsToGcsOptions extends DataflowPipelineOptions {

    @Description("GCS Path to watch for new files (e.g. gs://bucket/input/*)")
    @Validation.Required
    String getInputPath();

    void setInputPath(String value);


    @Description("Output GCS path for results")
    @Validation.Required
    String getOutputPath();

    void setOutputPath(String value);


    @Description("Folder to move processed files to (e.g., gs://bucket/processed/)")
    @Validation.Required
    String getProcessedPath();
    void setProcessedPath(String value);


    @Description("How often to poll for new files in seconds")
    @org.apache.beam.sdk.options.Default.Integer(60)
    Integer getPollInterval();

    void setPollInterval(Integer value);

}
