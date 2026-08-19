# GCP / BigQuery Setup (Free Tier) — Reviewer Guide

At the data volumes in this project (a few thousand rows/day, a few weeks of
history), everything fits comfortably inside GCP's **always-free** tier:

- BigQuery: **1 TB of query processing/month free**, **10 GB storage free** — this project uses a tiny fraction of that.
- No Cloud Composer needed — Airflow is self-hosted via the provided `docker-compose.yml`, so **$0** orchestration cost.

Estimated real cost per intern for the full project: **$0**, as long as you set the safety cap below (recommended regardless, in case an intern writes an accidental `SELECT *` over a huge scanned range).

---

## Option A — One shared GCP project, one BigQuery dataset per intern (recommended)

Simplest to administer. Each intern gets their own dataset inside the same project so their tables don't collide, but you only manage one project/billing account.

### 1. Create the project (if you don't have one already)
1. Go to console.cloud.google.com → **New Project**.
2. Name it something like `intern-capstone-2026`.
3. Link a billing account (required to enable BigQuery, but nothing should actually be charged if you set the budget cap in step 5).

### 2. Enable the BigQuery API
```
gcloud services enable bigquery.googleapis.com --project=intern-capstone-2026
```
(or Console → APIs & Services → Enable APIs → search "BigQuery API" → Enable)

### 3. Create one dataset per intern
```bash
# repeat per intern, swap the handle
bq mk --location=US intern-capstone-2026:pipeline_intern_alice
bq mk --location=US intern-capstone-2026:pipeline_intern_bob
```
Each intern's `pipeline_config.yaml` should point at their own dataset (see `bigquery.dataset` field).

### 4. Create a per-intern service account with least-privilege access
```bash
INTERN=alice
PROJECT=intern-capstone-2026

gcloud iam service-accounts create pipeline-intern-$INTERN \
  --project=$PROJECT \
  --display-name="Intern $INTERN pipeline SA"

# Grant access scoped to BigQuery only, and ideally scoped to their dataset
# (dataset-level IAM is set separately below — project-level roles here are
# broader; tighten via dataset ACLs if you want per-dataset isolation)
gcloud projects add-iam-policy-binding $PROJECT \
  --member="serviceAccount:pipeline-intern-$INTERN@$PROJECT.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding $PROJECT \
  --member="serviceAccount:pipeline-intern-$INTERN@$PROJECT.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"

# Download the key — this file goes into airflow/config/gcp-service-account.json
gcloud iam service-accounts keys create \
  ./gcp-service-account-$INTERN.json \
  --iam-account=pipeline-intern-$INTERN@$PROJECT.iam.gserviceaccount.com
```

> `roles/bigquery.dataEditor` + `roles/bigquery.jobUser` is enough to read/write
> tables and run queries, without granting project-admin or billing-admin
> permissions. This is the least-privilege pattern referenced in the spec.

For tighter isolation (intern A can't touch intern B's dataset even by
guessing the name), also set dataset-level access instead of relying only on
project-level roles:
```bash
bq update --dataset \
  --source=<(bq show --format=prettyjson $PROJECT:pipeline_intern_$INTERN | \
    python3 -c "import json,sys; d=json.load(sys.stdin); d.setdefault('access', []).append({'role':'WRITER','userByEmail':'pipeline-intern-$INTERN@$PROJECT.iam.gserviceaccount.com'}); print(json.dumps(d))") \
  $PROJECT:pipeline_intern_$INTERN
```
(Simpler alternative: just use the Console UI → BigQuery → dataset → Sharing → Permissions.)

### 5. Set a hard budget cap + alert (do this even though real cost should be ~$0)
1. Console → Billing → **Budgets & alerts** → Create budget.
2. Scope it to the `intern-capstone-2026` project.
3. Set amount to something small, e.g. **$5/month**.
4. Set alert thresholds at 50%/90%/100% of budget, emailed to you.
5. (Optional, stricter) Set up a Cloud Function/Pub-Sub budget action that disables billing on the project if the cap is hit — overkill for this project's scale, but available if you want a hard stop instead of just an alert.

### 6. Distribute credentials to interns
Give each intern:
- Their `gcp-service-account-<handle>.json` key file (they place it at `airflow/config/gcp-service-account.json` per the docker-compose setup)
- Their dataset name (goes into their `pipeline_config.yaml`)
- The project ID

**Do not** give interns your personal/owner credentials or project-owner IAM roles.

---

## Option B — Separate free-tier project per intern

If you'd rather fully isolate interns (no shared billing account, no risk of
one intern's runaway query affecting another), have each intern create their
own GCP account and free-tier project individually, following steps 2–4
above under their own project. GCP's free tier (currently $300 credit for
new accounts, valid 90 days, plus the always-free BigQuery quota after that)
is more than sufficient for this project. This avoids you having to manage
billing/IAM centrally, at the cost of a bit more setup friction per intern.

---

## Cleanup after the project ends
```bash
# Delete each intern's dataset
bq rm -r -f intern-capstone-2026:pipeline_intern_alice

# Delete each intern's service account
gcloud iam service-accounts delete \
  pipeline-intern-alice@intern-capstone-2026.iam.gserviceaccount.com
```
Or simply delete the whole project if it was created solely for this purpose.
