from flask import Flask, request
from google.cloud import pubsub_v1
import os, json

import logging
logging.basicConfig(level=logging.INFO)


app = Flask(__name__)

PROJECT_ID = os.environ.get("PROJECT_ID", "lanch-pipeline-v3")
TOPIC_ID = "mercadolibre-webhooks"
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

@app.route("/", methods=["POST"])
def webhook():
    data = request.get_json()
    
    if not data:
        logging.warning("No JSON received")
        return {"error": "No JSON received"}, 400
    
    logging.info(f"Received webhook: {json.dumps(data)}")

    future = publisher.publish(topic_path, json.dumps(data).encode("utf-8"))
    future.result()

    return {"status": "ok"}, 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
