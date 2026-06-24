import functions_framework
from flask import Request
from google.cloud import storage
import requests


@functions_framework.http
def extract_load(request: Request):
    request_json = request.get_json(silent=True) or {}

    url = request_json.get("url")
    bucket_name = request_json.get("bucket_name")
    object_name = request_json.get("object_name")

    if not all([url, bucket_name, object_name]):
        return "Missing url, bucket_name, or object_name", 400

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(object_name)

    with requests.get(f"{url}", stream=True) as response:
        response.raise_for_status()
        blob.upload_from_file(response.raw, content_type="application/octet-stream")
    
    return "Success", 200