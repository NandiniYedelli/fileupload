from flask import Flask, request
from google.cloud import pubsub_v1
import json
import os

app = Flask(__name__)

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path("atlantean-stone-462107-f4", "my-topic-1")

@app.route("/", methods=["POST"])
def index():
    envelope = request.get_json()
    if not envelope:
        return "No JSON received", 400

    # Extract file metadata from CloudEvent
    event_data = envelope.get("message", {}).get("data")
    attributes = envelope.get("message", {}).get("attributes")

    print(f"Received event: {attributes}")

    # Publish to Pub/Sub
    future = publisher.publish(
        topic_path,
        data=b"File uploaded",
        **attributes
    )
    print("Published message ID:", future.result())
    return "OK", 200
