"""
Worker service: processes a single batch of epics.
Triggered by Cloud Tasks (one task per batch), calls Gemini for
effort predictions, and updates job/batch status in Firestore.
"""
from flask import Flask, request, jsonify
from google.cloud import firestore

from estimation.logic import (
    build_prompt,
    call_gemini,
    firestore_safe,
    get_historical_context,
)
from estimation.constants import TEAM_ORDER

app = Flask(__name__)
db = firestore.Client()


def reorder_effort(effort_dict):
    return {team: effort_dict.get(team, 0) for team in TEAM_ORDER}


@app.route("/process-batch", methods=["POST"])
def process_batch():
    payload = request.get_json()
    job_id = payload["job_id"]
    batch_id = payload["batch_id"]

    job_ref = db.collection("estimation_jobs").document(job_id)
    batch_ref = job_ref.collection("batches").document(batch_id)

    batch_doc = batch_ref.get()
    if not batch_doc.exists:
        return jsonify({"error": "Batch not found"}), 404

    batch_data = batch_doc.to_dict()
    if batch_data.get("status") == "DONE":
        return jsonify({"message": "Already processed"}), 200

    rows = batch_data["data"]

    # Retrieve historical context for the batch, if enabled.
    # Uses the first row as a representative sample for the retrieval query.
    historical_context = None
    if rows and isinstance(rows[0], dict):
        first_row = rows[0]
        epic_title = first_row.get("Feature") or first_row.get("Epic Title", "")
        epic_description = first_row.get("Description") or first_row.get("Epic Description", "")

        if epic_title and epic_description:
            try:
                historical_context = get_historical_context(epic_title, epic_description)
                if historical_context:
                    app.logger.info(f"Retrieved historical context for batch {batch_id}")
            except Exception as e:
                app.logger.warning(f"Failed to get historical context: {e}")

    prompt = build_prompt(rows, historical_context=historical_context)

    try:
        raw = call_gemini(prompt)
    except Exception as e:
        app.logger.error(f"Gemini call failed for batch {batch_id}: {e}")
        return jsonify({"error": f"Gemini API error: {str(e)}"}), 500

    predictions = []
    for item in raw:
        if not isinstance(item, dict) or not item.get("Scope ID"):
            continue
        ordered_effort = reorder_effort(item.get("Predicted Effort (days)", {}))
        ordered_item = {
            "Scope ID": item.get("Scope ID"),
            "Feature": item.get("Feature"),
            "Impacted Domains": ", ".join(item.get("Impacted Domains", [])),
        }
        ordered_item.update(ordered_effort)
        ordered_item["Reasoning"] = item.get("Reasoning")
        predictions.append(ordered_item)

    batch_ref.update({
        "results": firestore_safe(predictions),
        "status": "DONE"
    })

    # Atomic counter so only one worker finalizes the job, without
    # streaming and re-checking every batch's status on each call.
    @firestore.transactional
    def try_complete(transaction, job_ref):
        job_snapshot = job_ref.get(transaction=transaction)
        job_data = job_snapshot.to_dict()
        completed = job_data.get("completed_batches", 0) + 1
        total_batches = job_data.get("total_batches", 0)
        transaction.update(job_ref, {"completed_batches": completed})
        if completed >= total_batches:
            transaction.update(job_ref, {"status": "COMPLETING"})
            return True
        return False

    transaction = db.transaction()
    should_finalize = try_complete(transaction, job_ref)

    if should_finalize:
        all_results = []
        for b in job_ref.collection("batches").stream():
            all_results.extend(b.to_dict().get("results", []))
        job_ref.update({
            "status": "COMPLETED",
            "results": firestore_safe(all_results)
        })

    return jsonify({"message": "Batch processed"}), 200
