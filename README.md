

- To run locally with the direct runner and the **LocalTextAnalyzerToTextPipeline** pipeline, 
  - run a maven package command first:
    - _mvn clean package_
  - make an **Application** run configuration, by filling:
    - _org.example.apache_beam.LocalTextAnalyzerToTextPipeline_  as main class
    - JDK version -> preferably __Java 21__
    - _--parameters-file=configs/localParams.json_ as CLI arguments, in section **Program arguments**
  - play then with different text files and worker configurations, below are the file sizes 
    - 1,1K  LICENSE.txt
    - 5,4M  pg100.txt
    - 5,4K  pg1513-SMALL.txt
    - 166K  pg1513.txt
    - 594K  pg1661.txt
    - 560K  pg5614.txt


- Then, to run in cloud, with the GCS bucket to GCS bucket pipeline,
