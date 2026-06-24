#!/bin/bash
export $(cat .env | xargs)

gcloud functions deploy extract-load \
    --gen2 \
    --runtime=python312 \
    --region=$GCP_REGION \
    --entry-point=extract_load \
    --timeout=600s \
    --memory=512Mi \
    --no-allow-unauthenticated \
    --trigger-http \
    --run-service-account=$GCP_CLOUD_FUNCTION_SERVICE_ACCOUNT