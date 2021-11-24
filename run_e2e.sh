export PROJECT_ID="${PROJECT_ID:-demo-project-123}"
export REGION="${REGION:-us-central1}"
export BIGQUERY_DATASET="${BIGQUERY_DATASET:-demo_bq_dataset}"
export BUCKET_NAME="${BUCKET_NAME:-demo_bq_dataset}"

gradle -Pe2e -PshowOutput integrationTest
