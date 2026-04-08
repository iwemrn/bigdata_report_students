import os
from io import BytesIO

from minio import Minio
from minio.error import S3Error


MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "reports")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"


def get_minio_client() -> Minio:
    return Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE
    )


def ensure_bucket_exists() -> None:
    client = get_minio_client()

    try:
        if not client.bucket_exists(MINIO_BUCKET):
            client.make_bucket(MINIO_BUCKET)
            print(f"Bucket '{MINIO_BUCKET}' created.")
        else:
            print(f"Bucket '{MINIO_BUCKET}' already exists.")
    except S3Error as e:
        raise RuntimeError(f"MinIO bucket error: {e}")


def upload_bytes_object(object_name: str, content: bytes, content_type: str) -> None:
    client = get_minio_client()

    try:
        client.put_object(
            bucket_name=MINIO_BUCKET,
            object_name=object_name,
            data=BytesIO(content),
            length=len(content),
            content_type=content_type
        )
        print(f"Object '{object_name}' uploaded to bucket '{MINIO_BUCKET}'.")
    except S3Error as e:
        raise RuntimeError(f"MinIO upload error: {e}")