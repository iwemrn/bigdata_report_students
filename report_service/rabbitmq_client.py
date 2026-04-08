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