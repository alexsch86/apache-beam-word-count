```markdown
# Apache Beam Pipeline Instructions

> **Note for Windows Users:** The `gcloud` commands below are formatted for Unix-like systems (Linux/macOS). If you are using **PowerShell**, remember to replace `$` with `$env:` and use backticks (`` ` ``) for line breaks instead of backslashes (`\`).

---

## 1. Local Execution (Direct Runner)
**Pipeline:** `LocalTextAnalyzerToTextPipeline`

### Build the Project
Run the Maven package command using the `local-app` profile:
```bash
mvn clean package -P local-app
```

### IDE Run Configuration (IntelliJ/Eclipse)
Create a new **Application** run configuration with the following settings:
* **Main Class:** `org.example.apache_beam.pipelines.LocalTextAnalyzerToTextPipeline`
* **JDK Version:** Java 21 (preferred)
* **Program Arguments:** `--parameters-file=configs/localParams.json`

### Sample Data Sizes (Resources Folder)
| File | Size |
| :--- | :--- |
| `LICENSE.txt` | 1.1K |
| `pg1513-SMALL.txt` | 5.4K |
| `pg1513.txt` | 166K |
| `pg5614.txt` | 560K |
| `pg1661.txt` | 594K |
| `pg100.txt` | 5.4M |

---

## 2. Cloud Execution: GCS to GCS
**Pipeline:** Batch processing with file movement.

### Step A: Build the FAT JAR
```bash
mvn clean package -P gcs-to-gcs
```

### Step B: Build and Upload Docker Image
Ensure your `PROJECT_ID` is exported, then submit the build (consider appending a unique suffix to the image name in the YAML):
```bash
export PROJECT_ID=$(gcloud config get-value project)

gcloud builds submit --config cloudbuilds/gcs-to-gcs-cloudbuild.yaml
```

### Step C: Update Flex Template (Optional)
Run this if you have modified `metadata/gcs-to-gcs-metadata.json`:
```bash
gcloud dataflow flex-template build gs://$BUCKET_NAME/templates/gcs-to-gcs-pipeline.json \
    --image "$IMAGE_URI" \
    --sdk-language "JAVA" \
    --metadata-file metadata/gcs-to-gcs-metadata.json
```

### Step D: Run the Dataflow Job
```bash
gcloud dataflow flex-template run "analyzer-gcs-to-gcs-job-$(date +%Y%m%d-%H%M%S)" \
    --template-file-gcs-location "gs://$BUCKET_NAME/templates/gcs-to-gcs-pipeline.json" \
    --region "$REGION" \
    --worker-machine-type "e2-standard-2" \
    --parameters "inputPath=gs://$BUCKET_NAME/inputfiles/*.txt,outputPath=gs://$BUCKET_NAME/results/,processedPath=gs://$BUCKET_NAME/processedfiles/"
```
* **Tip:** Use the `e2` machine family and low-traffic regions (e.g., `us-west1`, `europe-west1`) to avoid resource exhaustion.

---

## 3. Cloud Execution: GCS to BigQuery
**Pipeline:** Text analysis with results stored in BigQuery.

### Step A: Build the FAT JAR
```bash
mvn clean package -P gcs-to-bq
```

### Step B: Build and Upload Docker Image
```bash
gcloud builds submit --config cloudbuilds/gcs-to-bq-cloudbuild.yaml
```

### Step C: Update Flex Template (Optional)
If you modified `gcs-to-bq-metadata.json`, adapt the build command from the GCS-to-GCS section.

### Step D: Run the Dataflow Job
```bash
gcloud dataflow flex-template run "gcs-to-bq-analysis-job-$(date +%Y%m%d-%H%M%S)" \
    --template-file-gcs-location "gs://$BUCKET_NAME/templates/gcs-to-bq-pipeline.json" \
    --region "$REGION" \
    --staging-location "gs://$STAGING_BUCKET/staging" \
    --temp-location "gs://$STAGING_BUCKET/tmp" \
    --parameters "inputPath=gs://$BUCKET_NAME/inputfiles/*.txt,outputTable=$PROJECT_ID:$DATASET.$TABLENAME,processedPath=gs://$BUCKET_NAME/processedfiles/"
```

> **Warning:** Be cautious with the `region` parameter; if using an environment variable, ensure it is fully resolved to a literal value to avoid API errors.
```