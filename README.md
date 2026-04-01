

- To run locally with the direct runner and the **LocalTextAnalyzerToTextPipeline** pipeline, 
  - run a maven package command first, with the local-app profile:
    ``_mvn clean package_ -P local-app``
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
  - first run a maven package command, with the gcs-to-gcs profile, to produce the FAT JAR:
        ``_mvn clean package_ -P gcs-to-gcs``
  - to build a docker image and upload it to Artifact Registry, managed by Cloud Build, make sure you have the PROJECT_ID set up:
    - export PROJECT_ID=$(gcloud config get-value project)
    - proceed to run (not before appending your name or something different in the image name of the cloud build yaml): 
    ``gcloud builds submit --config cloudbuilds/gcs-to-gcs-cloudbuild.yaml``
  - if you changed the Flex template JSON file _metadata/gcs-to-gcs-metadata.json_, then create a new template using:
    ``  gcloud dataflow flex-template build gs://$BUCKET_NAME/templates/gcs-to-gcs-pipeline.json \
            --image "$IMAGE_URI" \
            --sdk-language "JAVA" \
            --metadata-file metadata/gcs-to-gcs-metadata.json``
  - make sure for the flex template you have the BUCKET_NAME, IMAGE_URI set up
  - then, run the dataflow flex template and give the job a fancy name
  ``gcloud dataflow flex-template run "analyzer-gcs-to-gcs-job-$(date +%Y%m%d-%H%M%S)" \
            --template-file-gcs-location "gs://$BUCKET_NAME/templates/gcs-to-gcs-pipeline.json" \
            --region "$REGION" \
            --worker-machine-type "e2-standard-2" \
            --parameters "inputPath=gs://$BUCKET_NAME/inputfiles/*.txt,outputPath=gs://$BUCKET_NAME/results/,processedPath=gs://$BUCKET_NAME/processedfiles/"``
  - it is imperative to run with an available family of machines, like e2, which are generally not crowded. 
  Furthermore, try with low-traffic regions, like us-west1, europe-west1, etc


- 
    