from gcloud import storage
from oauth2client.service_account import ServiceAccountCredentials
import os

credentials_dict = {
    "type": "service_account",
    "project_id": "project_id",
    "private_key_id": "private_key_id",
    "private_key": "private_key",
    "client_email": "client_email",
    "client_id": "client_id",
    "auth_uri": "auth_uri",
    "token_uri": "token_uri",
    "auth_provider_x509_cert_url": "auth_provider_x509_cert_url",
    "client_x509_cert_url": "client_x509_cert_url"
}
credentials = ServiceAccountCredentials.from_json_keyfile_dict(
    credentials_dict
)
client = storage.Client(credentials=credentials, project='My First Project')
bucket = client.get_bucket('big-data-hw2-kafka')
blob = bucket.blob('SRP.json')
blob.upload_from_filename('SRP.json')