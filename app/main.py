import asyncio
import json
import time
from pathlib import Path
from uuid import UUID, uuid4

from fastapi import Depends, FastAPI, Form, HTTPException, Request, status
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session, joinedload

import models
import schemas
from database import SessionLocal, engine
from minio_client import download_object_bytes
from rabbitmq_client import publish_report_request, wait_for_report_response


app = FastAPI(
    title="Students CRUD Service",
    description="Сервис учета студентов по специальностям",
    version="1.0.0"
)

BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
SEED_FILE = BASE_DIR / "seed" / "students_seed.json"

STATUS_OPTIONS = ["учится", "отчислен"]


def init_db(max_retries: int = 15, delay: int = 2) -> None:
    for attempt in range(1, max_retries + 1):
        try:
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            models.Base.metadata.create_all(bind=engine)
            print("Database connection established. Tables are ready.")
            return
        except OperationalError:
            print(f"Database is not ready yet. Attempt {attempt}/{max_retries}")
            time.sleep(delay)

    raise RuntimeError("Could not connect to PostgreSQL after several attempts.")


def seed_students_if_empty() -> None:
    db = SessionLocal()
    try:
        students_count = db.query(models.Student).count()
        if students_count > 0:
            print("Students table already contains data. Seed skipped.")
            return

        if not SEED_FILE.exists():
            print("Seed file not found. Seed skipped.")
            return

        with open(SEED_FILE, "r", encoding="utf-8") as f:
            seed_data = json.load(f)

        for item in seed_data:
            specialty = db.query(models.Specialty).filter(
                models.Specialty.id == item["specialty_id"]
            ).first()

            if specialty is None:
                print(f"Specialty {item['specialty_id']} not found. Record skipped.")
                continue

            student = models.Student(
                id=UUID(item["id"]),
                name=item["name"].strip(),
                status=item["status"].strip(),
                specialty_id=item["specialty_id"]
            )
            db.add(student)

        db.commit()
        print("Students seed data loaded successfully.")
    finally:
        db.close()


@app.on_event("startup")
def on_startup() -> None:
    init_db()
    seed_students_if_empty()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_specialty_or_404(db: Session, specialty_id: int):
    specialty = db.query(models.Specialty).filter(models.Specialty.id == specialty_id).first()
    if not specialty:
        raise HTTPException(status_code=404, detail="Specialty not found")
    return specialty


def get_student_or_404(db: Session, student_id: UUID):
    student = (
        db.query(models.Student)
        .options(joinedload(models.Student.specialty))
        .filter(models.Student.id == student_id)
        .first()
    )
    if not student:
        raise HTTPException(status_code=404, detail="Student not found")
    return student


def request_report_via_rabbitmq() -> dict:
    request_id = str(uuid4())

    request_payload = {
        "request_id": request_id,
        "report_type": "students_report",
        "format": "json"
    }

    publish_report_request(request_payload)
    response_payload = wait_for_report_response(request_id=request_id, timeout_seconds=30)

    if response_payload.get("status") != "done":
        raise RuntimeError(f"Unexpected report response status: {response_payload}")

    return response_payload


def fetch_report_bytes_from_minio(report_file_info: dict) -> bytes:
    bucket = report_file_info.get("bucket")
    object_name = report_file_info.get("object_name")

    if not bucket or not object_name:
        raise HTTPException(status_code=500, detail="Invalid report file metadata")

    try:
        return download_object_bytes(bucket_name=bucket, object_name=object_name)
    except RuntimeError as e:
        raise HTTPException(status_code=502, detail=str(e))


@app.get("/health")
def healthcheck():
    return {"status": "ok"}


@app.get("/ui", response_class=HTMLResponse)
def ui(request: Request, db: Session = Depends(get_db)):
    students = (
        db.query(models.Student)
        .options(joinedload(models.Student.specialty))
        .order_by(models.Student.name)
        .all()
    )
    specialties = db.query(models.Specialty).order_by(models.Specialty.id).all()

    students_data = [
        {
            "id": str(student.id),
            "name": student.name,
            "status": student.status,
            "specialty_id": student.specialty_id,
            "specialty_name": student.specialty.name if student.specialty else "Не указана"
        }
        for student in students
    ]

    specialties_data = [
        {"id": specialty.id, "name": specialty.name}
        for specialty in specialties
    ]

    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "request": request,
            "students": students_data,
            "specialties": specialties_data,
            "status_options": STATUS_OPTIONS
        }
    )


@app.post("/ui/create")
def create_student_ui(
    name: str = Form(...),
    status: str = Form(...),
    specialty_id: int = Form(...),
    db: Session = Depends(get_db)
):
    get_specialty_or_404(db, specialty_id)

    student = models.Student(
        name=name.strip(),
        status=status.strip(),
        specialty_id=specialty_id
    )

    db.add(student)
    db.commit()

    return RedirectResponse(url="/ui", status_code=303)


@app.post("/ui/update/{student_id}")
def update_student_ui(
    student_id: UUID,
    name: str = Form(...),
    status: str = Form(...),
    specialty_id: int = Form(...),
    db: Session = Depends(get_db)
):
    get_specialty_or_404(db, specialty_id)
    student = get_student_or_404(db, student_id)

    student.name = name.strip()
    student.status = status.strip()
    student.specialty_id = specialty_id

    db.commit()

    return RedirectResponse(url="/ui", status_code=303)


@app.post("/ui/delete/{student_id}")
def delete_student_ui(student_id: UUID, db: Session = Depends(get_db)):
    student = get_student_or_404(db, student_id)

    db.delete(student)
    db.commit()

    return RedirectResponse(url="/ui", status_code=303)


@app.get("/report/json")
async def get_report_json():
    try:
        report_file_info = await asyncio.to_thread(request_report_via_rabbitmq)
        file_bytes = await asyncio.to_thread(fetch_report_bytes_from_minio, report_file_info)

        payload = json.loads(file_bytes.decode("utf-8"))
        return JSONResponse(content=payload)
    except TimeoutError as e:
        raise HTTPException(status_code=504, detail=str(e))
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail="Downloaded file is not valid JSON")
    except RuntimeError as e:
        raise HTTPException(status_code=502, detail=str(e))


@app.get("/report/file")
async def download_report_file():
    try:
        report_file_info = await asyncio.to_thread(request_report_via_rabbitmq)
        file_bytes = await asyncio.to_thread(fetch_report_bytes_from_minio, report_file_info)

        object_name = report_file_info.get("object_name", "students-report.json")
        content_type = report_file_info.get("content_type", "application/json")

        return Response(
            content=file_bytes,
            media_type=content_type,
            headers={
                "Content-Disposition": f'attachment; filename="{object_name}"'
            }
        )
    except TimeoutError as e:
        raise HTTPException(status_code=504, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=502, detail=str(e))


@app.get("/", response_model=list[schemas.StudentResponse])
def get_students(db: Session = Depends(get_db)):
    return db.query(models.Student).all()


@app.post("/", response_model=schemas.StudentResponse, status_code=status.HTTP_201_CREATED)
def create_student(student: schemas.StudentCreate, db: Session = Depends(get_db)):
    get_specialty_or_404(db, student.specialty_id)

    new_student = models.Student(
        name=student.name.strip(),
        status=student.status.strip(),
        specialty_id=student.specialty_id
    )

    db.add(new_student)
    db.commit()
    db.refresh(new_student)

    return new_student


@app.get("/{student_id}", response_model=schemas.StudentResponse)
def get_student(student_id: UUID, db: Session = Depends(get_db)):
    student = get_student_or_404(db, student_id)
    return student


@app.put("/{student_id}", response_model=schemas.StudentResponse)
def update_student(student_id: UUID, updated: schemas.StudentUpdate, db: Session = Depends(get_db)):
    get_specialty_or_404(db, updated.specialty_id)
    student = get_student_or_404(db, student_id)

    student.name = updated.name.strip()
    student.status = updated.status.strip()
    student.specialty_id = updated.specialty_id

    db.commit()
    db.refresh(student)

    return student


@app.delete("/{student_id}")
def delete_student(student_id: UUID, db: Session = Depends(get_db)):
    student = get_student_or_404(db, student_id)

    db.delete(student)
    db.commit()

    return {"message": "Student deleted successfully"}