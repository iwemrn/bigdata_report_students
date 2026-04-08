import os

from minio import Minio
from minio.error import S3Error


MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"


def get_minio_client() -> Minio:
    return Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE
    )


def download_object_bytes(bucket_name: str, object_name: str) -> bytes:
    client = get_minio_client()
    response = None

    try:
        response = client.get_object(bucket_name=bucket_name, object_name=object_name)
        data = response.read()
        return data
    except S3Error as e:
        raise RuntimeError(f"MinIO download error: {e}")
    finally:
        if response is not None:
            response.close()
            response.release_conn()