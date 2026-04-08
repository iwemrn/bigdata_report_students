import json
import os
import time

import pika


RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "admin")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "admin")

REPORT_REQUEST_QUEUE = os.getenv("REPORT_REQUEST_QUEUE", "report_requests")
REPORT_RESPONSE_QUEUE = os.getenv("REPORT_RESPONSE_QUEUE", "report_responses")


def get_connection(max_retries: int = 20, delay: int = 3) -> pika.BlockingConnection:
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)

    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            parameters = pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                port=RABBITMQ_PORT,
                credentials=credentials,
                heartbeat=60
            )
            return pika.BlockingConnection(parameters)
        except Exception as e:
            last_error = e
            print(f"RabbitMQ is not ready yet. Attempt {attempt}/{max_retries}. Error: {e}")
            time.sleep(delay)

    raise RuntimeError(f"Could not connect to RabbitMQ: {last_error}")


def declare_queues(channel) -> None:
    channel.queue_declare(queue=REPORT_REQUEST_QUEUE, durable=True)
    channel.queue_declare(queue=REPORT_RESPONSE_QUEUE, durable=True)


def publish_report_request(request_payload: dict) -> None:
    connection = None
    try:
        connection = get_connection()
        channel = connection.channel()
        declare_queues(channel)

        channel.basic_publish(
            exchange="",
            routing_key=REPORT_REQUEST_QUEUE,
            body=json.dumps(request_payload, ensure_ascii=False),
            properties=pika.BasicProperties(delivery_mode=2)
        )

        print(f"Report request published: {request_payload.get('request_id')}")
    finally:
        if connection and connection.is_open:
            connection.close()


def wait_for_report_response(request_id: str, timeout_seconds: int = 30) -> dict:
    connection = None
    deadline = time.time() + timeout_seconds

    try:
        connection = get_connection()
        channel = connection.channel()
        declare_queues(channel)

        while time.time() < deadline:
            method_frame, header_frame, body = channel.basic_get(
                queue=REPORT_RESPONSE_QUEUE,
                auto_ack=False
            )

            if method_frame:
                try:
                    payload = json.loads(body.decode("utf-8"))
                except Exception:
                    channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                    continue

                if payload.get("request_id") == request_id:
                    channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                    print(f"Matching report response received: {request_id}")
                    return payload
                else:
                    channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=True)
                    time.sleep(0.3)
            else:
                time.sleep(0.5)

        raise TimeoutError(f"Timeout while waiting for report response: {request_id}")

    finally:
        if connection and connection.is_open:
            connection.close()