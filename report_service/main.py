import json
import threading
import time
from datetime import datetime
from pathlib import Path

from fastapi import Depends, FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import and_, func, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

import models
import schemas
from database import SessionLocal, engine
from minio_client import MINIO_BUCKET, ensure_bucket_exists, upload_bytes_object
from rabbitmq_client import (
    REPORT_REQUEST_QUEUE,
    REPORT_RESPONSE_QUEUE,
    declare_queues,
    get_connection,
)


app = FastAPI(
    title="Students Report Service",
    description="Сервис формирования отчётов по студентам",
    version="1.0.0"
)

BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


def init_db(max_retries: int = 15, delay: int = 2) -> None:
    for attempt in range(1, max_retries + 1):
        try:
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            print("Report service connected to database.")
            return
        except OperationalError:
            print(f"Database is not ready yet. Attempt {attempt}/{max_retries}")
            time.sleep(delay)

    raise RuntimeError("Could not connect to PostgreSQL after several attempts.")


def init_minio(max_retries: int = 15, delay: int = 2) -> None:
    for attempt in range(1, max_retries + 1):
        try:
            ensure_bucket_exists()
            print("MinIO is ready. Bucket check completed.")
            return
        except Exception as e:
            print(f"MinIO is not ready yet. Attempt {attempt}/{max_retries}. Error: {e}")
            time.sleep(delay)

    raise RuntimeError("Could not connect to MinIO after several attempts.")


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def build_report(db: Session) -> schemas.StudentsReportResponse:
    total_students = db.query(func.count(models.Student.id)).scalar() or 0

    active_students = (
        db.query(func.count(models.Student.id))
        .filter(models.Student.status == "учится")
        .scalar()
        or 0
    )

    expelled_students = (
        db.query(func.count(models.Student.id))
        .filter(models.Student.status == "отчислен")
        .scalar()
        or 0
    )

    by_specialty_raw = (
        db.query(
            models.Specialty.id.label("specialty_id"),
            models.Specialty.name.label("specialty_name"),
            func.count(models.Student.id).label("active_students_count")
        )
        .outerjoin(
            models.Student,
            and_(
                models.Student.specialty_id == models.Specialty.id,
                models.Student.status == "учится"
            )
        )
        .group_by(models.Specialty.id, models.Specialty.name)
        .order_by(models.Specialty.id)
        .all()
    )

    by_specialty = [
        schemas.SpecialtyReportItem(
            specialty_id=row.specialty_id,
            specialty_name=row.specialty_name,
            active_students_count=row.active_students_count
        )
        for row in by_specialty_raw
    ]

    return schemas.StudentsReportResponse(
        total_students=total_students,
        active_students=active_students,
        expelled_students=expelled_students,
        by_specialty=by_specialty
    )


def build_and_upload_report() -> dict:
    db = SessionLocal()
    try:
        report = build_report(db)
        report_dict = report.model_dump()
        report_json = json.dumps(report_dict, ensure_ascii=False, indent=2).encode("utf-8")

        timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%SZ")
        object_name = f"students-report-{timestamp}.json"

        upload_bytes_object(
            object_name=object_name,
            content=report_json,
            content_type="application/json"
        )

        return {
            "bucket": MINIO_BUCKET,
            "object_name": object_name,
            "content_type": "application/json"
        }
    finally:
        db.close()


def process_report_request(body: bytes) -> dict:
    payload = json.loads(body.decode("utf-8"))
    request_id = payload.get("request_id")

    if not request_id:
        raise ValueError("request_id is required")

    report_file_info = build_and_upload_report()

    return {
        "request_id": request_id,
        "bucket": report_file_info["bucket"],
        "object_name": report_file_info["object_name"],
        "content_type": report_file_info["content_type"],
        "status": "done"
    }


def consume_report_requests_forever():
    while True:
        connection = None
        try:
            connection = get_connection()
            channel = connection.channel()
            declare_queues(channel)
            channel.basic_qos(prefetch_count=1)

            def callback(ch, method, properties, body):
                try:
                    response_payload = process_report_request(body)
                    channel.basic_publish(
                        exchange="",
                        routing_key=REPORT_RESPONSE_QUEUE,
                        body=json.dumps(response_payload, ensure_ascii=False),
                        properties=None
                    )
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    print(f"Report request processed successfully: {response_payload['request_id']}")
                except Exception as e:
                    print(f"Error while processing report request: {e}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

            channel.basic_consume(
                queue=REPORT_REQUEST_QUEUE,
                on_message_callback=callback
            )

            print("RabbitMQ consumer started. Waiting for report requests...")
            channel.start_consuming()

        except Exception as e:
            print(f"RabbitMQ consumer crashed, restarting in 5 seconds. Error: {e}")
            time.sleep(5)

        finally:
            try:
                if connection and connection.is_open:
                    connection.close()
            except Exception:
                pass


@app.on_event("startup")
def on_startup() -> None:
    init_db()
    init_minio()

    consumer_thread = threading.Thread(
        target=consume_report_requests_forever,
        daemon=True
    )
    consumer_thread.start()
    print("Background consumer thread started.")


@app.get("/health")
def healthcheck():
    return {"status": "ok"}


# Оставляем direct endpoints для отладки и самопроверки.
@app.get("/report/json", response_model=schemas.StudentsReportResponse)
def get_students_report_json(db: Session = Depends(get_db)):
    return build_report(db)


@app.get("/report", response_class=HTMLResponse)
def get_students_report_html(request: Request, db: Session = Depends(get_db)):
    report = build_report(db)

    return templates.TemplateResponse(
        request,
        "report.html",
        {
            "request": request,
            "report": report
        }
    )