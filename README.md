

- To run locally with the direct runner and the **LocalTextAnalyzerToTextPipeline** pipeline, 
  - run a maven package command first, with the local-app profile:
    - _mvn clean package_ -P local-app
  - make an **Application** run configuration, by filling:
    - _org.example.apache_beam.pipelines.LocalTextAnalyzerToTextPipeline_  as main class
    - JDK version -> preferably __Java 21__
    - _--parameters-file=configs/localParams.json_ as CLI arguments, in section **Program arguments**
  - play then with different text files and worker configurations, in the JSON config file, below are the file sizes, for
samples located in the resources folder
    - 1,1K  LICENSE.txt
    - 5,4M  pg100.txt
    - 5,4K  pg1513-SMALL.txt
    - 166K  pg1513.txt
    - 594K  pg1661.txt
    - 560K  pg5614.txt


- Then, to run in cloud, with the GCS bucket to GCS bucket pipeline,
  - first run a maven package command first, with the gcs-to-gcs profile:
    - _mvn clean package_ -P gcs-to-gcs