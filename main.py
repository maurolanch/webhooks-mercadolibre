from flask import Flask, request
from google.cloud import pubsub_v1
import os, json, logging

logging.basicConfig(level=logging.INFO)

app = Flask(__name__)

PROJECT_ID = os.environ.get("PROJECT_ID")
ORDERS_TOPIC_ID = os.environ.get("ORDERS_TOPIC_ID")
PAYMENTS_TOPIC_ID = os.environ.get("PAYMENTS_TOPIC_ID")

publisher = pubsub_v1.PublisherClient()
orders_topic_path = publisher.topic_path(PROJECT_ID, ORDERS_TOPIC_ID)
payments_topic_path = publisher.topic_path(PROJECT_ID, PAYMENTS_TOPIC_ID)

def publish_to_pubsub(topic_path, data):
    try:
        publisher.publish(topic_path, json.dumps(data).encode("utf-8"))
        logging.info(f"Published message with ID {data.get('_id')} to Pub/Sub topic {topic_path}.")
    except Exception as e:
        logging.error(f"Error publishing to Pub/Sub: {e}")

def process_webhook(data, topic_path):
    if not data:
        logging.warning("No JSON received")
        return {"error": "No JSON received"}, 400

    attempts = data.get("attempts", 0)
    if attempts > 1:
        logging.info(f"Webhook with ID {data.get('_id')} is a retry (attempts: {attempts}), skipping processing.")
        return {"status": "skipped", "reason": "retry attempt"}, 200

    logging.info(f"Received webhook: {json.dumps(data)}")
    publish_to_pubsub(topic_path, data)
    return {"status": "ok"}, 200

@app.route("/orders", methods=["POST"])
def webhook_orders():
    data = request.get_json()
    return process_webhook(data, orders_topic_path)

@app.route("/payments", methods=["POST"])
def webhook_payments():
    data = request.get_json()
    return process_webhook(data, payments_topic_path)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
