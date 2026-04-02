package org.example.apache_beam.options;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.Validation.Required;

public interface GcsToBqOptions extends DataflowPipelineOptions {

    @Description("GCS Path to watch for new files (e.g. gs://bucket/input/*)")
    @Validation.Required
    String getInputPath();
    void setInputPath(String value);


    @Description("BigQuery table to write to (project:dataset.table)")
    @Required
    String getOutputTable();
    void setOutputTable(String value);


    @Description("Folder to move processed files to (e.g., gs://bucket/processed/)")
    @Validation.Required
    String getProcessedPath();
    void setProcessedPath(String value);

}