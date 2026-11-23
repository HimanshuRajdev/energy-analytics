#!/usr/bin/env bash
# One-time GCP setup. Run from Cloud Shell or any terminal with gcloud logged in.
#   PROJECT=my-project bash deploy/gce_setup.sh
set -euo pipefail

PROJECT="${PROJECT:?set PROJECT=your-gcp-project-id}"
ZONE="${ZONE:-us-central1-a}"          # e2-micro is only free in us-west1, us-central1, us-east1
VM="${VM:-eia-airflow}"
MACHINE="${MACHINE:-e2-small}"         # 2 GB RAM, about $12/month. e2-micro is free but only 1 GB.
SA_NAME="eia-pipeline"
SA="${SA_NAME}@${PROJECT}.iam.gserviceaccount.com"

gcloud config set project "$PROJECT"
gcloud services enable bigquery.googleapis.com compute.googleapis.com

# Service account the VM runs as. No key file ever leaves Google.
gcloud iam service-accounts create "$SA_NAME" --display-name="EIA pipeline" || true
for role in roles/bigquery.dataEditor roles/bigquery.jobUser; do
  gcloud projects add-iam-policy-binding "$PROJECT" \
    --member="serviceAccount:${SA}" --role="$role" --condition=None >/dev/null
done

gcloud compute instances create "$VM" \
  --zone="$ZONE" --machine-type="$MACHINE" \
  --image-family=debian-12 --image-project=debian-cloud \
  --boot-disk-size=20GB \
  --service-account="$SA" --scopes=cloud-platform \
  --metadata-from-file=startup-script=deploy/vm_startup.sh

cat <<MSG

VM created. Wait about two minutes for Docker to install, then:

  gcloud compute ssh $VM --zone $ZONE
  git clone https://github.com/HimanshuRajdev/energy-analytics.git && cd energy-analytics
  cp .env.example .env && nano .env     # fill in keys (leave GOOGLE_APPLICATION_CREDENTIALS out)
  sudo docker compose up -d --build

Open the Airflow UI from your laptop with a tunnel:

  gcloud compute ssh $VM --zone $ZONE -- -L 8080:localhost:8080
  then visit http://localhost:8080

MSG
