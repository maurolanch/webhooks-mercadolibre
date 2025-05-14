from flask import Flask, request
from google.cloud import pubsub_v1
import os, json
import logging

logging.basicConfig(level=logging.INFO)

app = Flask(__name__)

PROJECT_ID = os.environ.get("PROJECT_ID")
TOPIC_ID = os.environ.get("TOPIC_ID")

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

@app.route("/", methods=["POST"])
def webhook():
    data = request.get_json()

    if not data:
        logging.warning("No JSON received")
        return {"error": "No JSON received"}, 400

    attempts = data.get("attempts", 0)
    if attempts > 1:
        logging.info(f"Webhook with ID {data.get('_id')} is a retry (attempts: {attempts}), skipping processing.")
        return {"status": "skipped", "reason": "retry attempt"}, 200

    logging.info(f"Received webhook: {json.dumps(data)}")

    try:
        publisher.publish(topic_path, json.dumps(data).encode("utf-8"))
        logging.info(f"Webhook with ID {data.get('_id')} published to Pub/Sub.")
    except Exception as e:
        logging.error(f"Error al publicar en Pub/Sub: {e}")

    return {"status": "ok"}, 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
