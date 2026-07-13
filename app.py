"""
Main Flask app: handles CSV upload, job creation, and dispatches
batches to the worker service via Cloud Tasks for parallel processing.
"""
from flask import Flask, jsonify, render_template, request, send_file
import pandas as pd
from io import BytesIO

import logging
import uuid
import json
import os
from google.cloud import firestore
from google.cloud import tasks_v2

from estimation.constants import TEAM_ORDER

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Firestore client
db = firestore.Client()
app = Flask(__name__)
app.secret_key = os.environ["SECRET_KEY"]


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint."""
    return jsonify({"status": "healthy"}), 200


@app.route("/estimate", methods=["POST"])
def estimate():
    """
    Main estimation endpoint.
    Handles file upload and initiates batch processing via Cloud Tasks.
    """
    # ===================================================================
    # PART 1: Validate file was uploaded
    # ===================================================================
    if "file" not in request.files:
        return jsonify({"error": "No file uploaded"}), 400

    file = request.files["file"]

    if file.filename == "":
        return jsonify({"error": "No file selected"}), 400

    # ===================================================================
    # PART 2: Read and validate CSV file
    # ===================================================================
    try:
        new_req_df = pd.read_csv(file)
        required_columns = ["Key", "Feature", "Description"]
        all_errors = []

        # Missing columns
        missing_columns = [col for col in required_columns if col not in new_req_df.columns]
        if missing_columns:
            all_errors.extend([f"Column '{col}' is missing from the CSV." for col in missing_columns])

        # Remove fully empty rows
        new_req_df = new_req_df.dropna(how='all')
        new_req_df = new_req_df.reset_index(drop=True)

        # Empty values (only check columns that exist)
        for col in required_columns:
            if col not in missing_columns:
                empty_rows = new_req_df[
                    new_req_df[col].isna() |
                    (new_req_df[col].astype(str).str.strip() == "")
                ]

                if not empty_rows.empty:
                    row_numbers = (empty_rows.index + 2).tolist()
                    preview = row_numbers[:10]
                    more = len(row_numbers) - len(preview)

                    msg = f"'{col}' column has empty values in row(s): {', '.join(map(str, preview))}"
                    if more > 0:
                        msg += f" ... (+{more} more)"

                    all_errors.append(msg)

        if all_errors:
            return jsonify({"errors": all_errors}), 400

        logger.info(f"Received CSV with {len(new_req_df)} rows")

    except Exception as e:
        logger.error(f"Error reading CSV: {e}")
        return jsonify({"error": f"Error reading CSV: {str(e)}"}), 400

    # ===================================================================
    # PART 3: Create job in Firestore
    # ===================================================================
    job_id = str(uuid.uuid4())
    job_ref = db.collection("estimation_jobs").document(job_id)
    batch_size = int(os.environ.get("BATCH_SIZE", "5"))
    batch_count_total = -(-len(new_req_df) // batch_size)  # ceiling division

    try:
        job_ref.set({
            "status": "PENDING",
            "total_rows": len(new_req_df),
            "total_batches": batch_count_total,
            "completed_batches": 0,
            "results": [],
            "created_at": firestore.SERVER_TIMESTAMP,
            "filename": file.filename
        })
        logger.info(f"Created job {job_id} with {len(new_req_df)} rows")
    except Exception as e:
        logger.error(f"Error creating job in Firestore: {e}")
        return jsonify({"error": "Failed to create estimation job"}), 500

    # ===================================================================
    # PART 4: Save batches to Firestore and enqueue Cloud Tasks
    # (Cloud Tasks will trigger the worker service to process in parallel)
    # ===================================================================
    batch_count = 0

    try:
        for start in range(0, len(new_req_df), batch_size):
            batch_df = new_req_df.iloc[start:start + batch_size]
            batch_id = str(start)

            batch_ref = job_ref.collection("batches").document(batch_id)
            batch_ref.set({
                "data": batch_df.to_dict(orient="records"),
                "status": "PENDING"
            })

            try:
                enqueue_batch_task(job_id, batch_id)
                batch_count += 1
                logger.info(f"Enqueued batch {batch_id} to Cloud Tasks ({start}-{min(start + batch_size, len(new_req_df))})")
            except Exception as task_error:
                logger.error(f"Failed to enqueue batch {batch_id}: {task_error}")
                continue

        if batch_count == 0:
            logger.error(f"No batches were successfully enqueued for job {job_id}")
            return jsonify({"error": "Failed to enqueue any processing tasks"}), 500

        logger.info(f"Job {job_id} initialized with {batch_count} batches")

    except Exception as e:
        logger.error(f"Error creating batches: {e}")
        return jsonify({"error": "Failed to create batches"}), 500

    # ===================================================================
    # PART 5: Return response
    # ===================================================================
    logger.info(f"Returning job_id={job_id}, total_rows={len(new_req_df)}, batch_count={batch_count}")
    return jsonify({
        "job_id": job_id,
        "total_rows": len(new_req_df),
        "batch_count": batch_count
    })


def enqueue_batch_task(job_id, batch_id):
    try:
        client = tasks_v2.CloudTasksClient()

        project = os.environ.get("GOOGLE_CLOUD_PROJECT")
        location = os.environ.get("TASK_LOCATION", "us-central1")
        queue = os.environ.get("TASK_QUEUE")
        worker_url = os.environ.get("WORKER_SERVICE_URL")

        if not all([project, queue, worker_url]):
            raise ValueError("Missing required env vars: GOOGLE_CLOUD_PROJECT, TASK_QUEUE, WORKER_SERVICE_URL")

        parent = client.queue_path(project, location, queue)

        payload = json.dumps({"job_id": job_id, "batch_id": batch_id}).encode()

        task = {
            "http_request": {
                "http_method": tasks_v2.HttpMethod.POST,
                "url": f"{worker_url}/process-batch",
                "headers": {
                    "Content-Type": "application/json"
                },
                "body": payload
            }
        }

        client.create_task(request={"parent": parent, "task": task})
        logger.info(f"Successfully enqueued batch: job_id={job_id}, batch_id={batch_id}")

    except Exception as e:
        logger.error(f"Error enqueueing batch task: {e}")
        raise


@app.route("/download/<job_id>")
def download(job_id: str):
    """Download estimation results as CSV."""
    try:
        job_ref = db.collection("estimation_jobs").document(job_id)
        job_doc = job_ref.get()

        if not job_doc.exists:
            return jsonify({"error": "Job not found"}), 404

        job_data = job_doc.to_dict()
        if job_data.get("status") != "COMPLETED":
            return jsonify({"error": "Job not completed yet"}), 400

        df, _ = build_result_from_firestore(job_id)

        if df is None or df.empty:
            return jsonify({"error": "No results to download"}), 400

        buffer = BytesIO()
        df.to_csv(buffer, index=False, encoding="utf-8-sig")
        buffer.seek(0)

        return send_file(
            buffer,
            as_attachment=True,
            download_name=f"estimation_results_{job_id[:8]}.csv",
            mimetype="text/csv"
        )

    except Exception as e:
        logger.error(f"Error downloading results: {e}")
        return jsonify({"error": "Error preparing download"}), 500


@app.route("/poll/<job_id>", methods=["GET"])
def poll(job_id: str):
    """
    Poll job status.
    Returns status and results when complete.
    """
    try:
        job_ref = db.collection("estimation_jobs").document(job_id)
        doc = job_ref.get()

        if not doc.exists:
            return jsonify({"error": "Job not found"}), 404

        job_data = doc.to_dict()
        status = job_data.get("status")

        response = {
            "status": status,
            "total_rows": job_data.get("total_rows", 0),
            "total_batches": job_data.get("total_batches", 0),
            "completed_batches": job_data.get("completed_batches", 0),
            "current_stage": job_data.get("current_stage", "Initializing..."),
            "created_at": str(job_data.get("created_at", ""))
        }

        if status == "COMPLETED":
            _, preview_html = build_result_from_firestore(job_id)
            response.update({
                "results": job_data.get("results", []),
                "download_path": f"/download/{job_id}",
                "preview": preview_html
            })

        return jsonify(response)

    except Exception as e:
        logger.error(f"Error polling job: {e}")
        return jsonify({"error": "Error checking job status"}), 500


def build_result_from_firestore(job_id):
    batches = (
        db.collection("estimation_jobs")
        .document(job_id)
        .collection("batches")
        .stream()
    )

    all_rows = []
    for b in batches:
        all_rows.extend(b.to_dict().get("results", []))

    if not all_rows:
        return None, None

    df = pd.DataFrame(all_rows)
    # Ensure columns are in a consistent, predictable order
    cols = ["Scope ID", "Feature", "Impacted Domains"] + TEAM_ORDER + ["Reasoning"]
    df = df[cols]

    preview_html = df.head(20).to_html(classes="table table-striped", index=False)

    return df, preview_html


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    debug_mode = os.environ.get("FLASK_DEBUG", "false").lower() == "true"
    app.run(host="0.0.0.0", port=port, debug=debug_mode)
