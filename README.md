# Delphi — AI-Powered Engineering Effort Estimation

Delphi is a Retrieval-Augmented Generation (RAG) system that predicts engineering
effort (in days, broken down by team) for new feature epics, based on historical
engineering data and LLM reasoning.

Given a CSV of new epics (ID, title, description), Delphi:
1. Embeds each epic and retrieves similar historical epics via Vertex AI Vector Search
2. Enriches the retrieved epics with structured metadata from BigQuery
3. Builds a grounded prompt combining the new epic + historical context
4. Calls Gemini (via Vertex AI) to produce a structured effort estimate per team,
   with reasoning
5. Returns results as a downloadable CSV, with progress tracked in real time

## Architecture

```
 ┌──────────┐   upload CSV    ┌─────────┐   1 Cloud Task per batch   ┌─────────┐
 │  Browser │ ───────────────>│ app.py  │ ──────────────────────────>│worker.py│
 └──────────┘                 │ (web)   │                            │(worker) │
       ▲                      └────┬────┘                            └────┬────┘
       │        poll status        │                                      │
       │<───────────────────────────                                      │
       │                      Firestore (job/batch state) <───────────────┘
       │                                                                   │
       │                                                          Vertex AI Vector Search
       │                                                          BigQuery (historical epics)
       │                                                          Gemini (via Vertex AI)
```

- **`app.py`** — Flask web service. Validates the uploaded CSV, creates a job in
  Firestore, splits it into batches, and enqueues one Cloud Task per batch.
- **`worker.py`** — Flask service triggered by Cloud Tasks. Processes a single
  batch: retrieves historical context, builds the prompt, calls Gemini, and
  writes results back to Firestore. Uses a Firestore transaction so exactly one
  worker finalizes the job once all batches are done.
- **`estimation/logic.py`** — Core logic: embedding, vector search, BigQuery
  retrieval, prompt construction, and the Gemini call.
- Both services run from the same Docker image; the `SERVICE` env var picks
  which Flask app (`app` or `worker`) Gunicorn serves.

## Running locally

```bash
pip install -r requirements.txt

# Required env vars (see estimation/logic.py / app.py for the full list)
export GOOGLE_CLOUD_PROJECT=your-project-id
export MODEL_NAME=gemini-2.5-pro
export SECRET_KEY=some-random-string
export TASK_QUEUE=your-queue-name
export WORKER_SERVICE_URL=https://your-worker-url

python app.py      # web service
python worker.py   # worker service (run separately)
```

Upload a CSV with `Key`, `Feature`, and `Description` columns (Epic ID, title,
description) through the web UI to run an estimation.

## Notes on this repo

This is a cleaned-up demo version of a system originally built for internal
use. Company-specific branding, real historical engineering data, and
internal infrastructure identifiers have been removed or replaced with
placeholders/synthetic data so the code and architecture can be shared
publicly.
