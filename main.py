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
        logging.info(f"Publicado en Pub/Sub topic: {topic_path}")
    except Exception as e:
        logging.error(f"Error al publicar en Pub/Sub: {e}")

@app.route("/", methods=["POST"])
def unified_webhook():
    data = request.get_json()

    if not data:
        logging.warning("No JSON recibido")
        return {"error": "No JSON recibido"}, 400

    topic = data.get("topic")
    attempts = data.get("attempts", 0)

    if attempts > 1:
        logging.info(f"Retry detectado. ID: {data.get('_id')}, attempts: {attempts}")
        return {"status": "skipped", "reason": "retry attempt"}, 200

    if topic == "orders":
        publish_to_pubsub(orders_topic_path, data)
    elif topic == "payments":
        publish_to_pubsub(payments_topic_path, data)
    else:
        logging.warning(f"Webhook desconocido: topic={topic}")
        return {"status": "ignored", "reason": "unknown topic"}, 200

    return {"status": "ok"}, 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
