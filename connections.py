import google.auth as gg
from googleapiclient.discovery import build
import json
import os
credentials, project = gg.default(
    scopes=[
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/bigquery",
    ]
)
def access_secret_version(project_id, secret_id, version_id):
  from google.cloud import secretmanager
  client = secretmanager.SecretManagerServiceClient()
  # Build the resource name of the secret version.
  # projects / 351263889262 / secrets / postgrest - anonimizacion
  name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
  response = client.access_secret_version(request={"name": name})
  payload = response.payload.data.decode("UTF-8")
  jsonv = json.loads(json.loads(json.dumps(payload.replace("\n", '').replace("'", '"'))))
  return jsonv

service = build('sheets', 'v4', credentials=credentials)
# Call the Sheets API
sheet = service.spreadsheets()
accesos = access_secret_version('351263889262', '99minutos-sharepoint', '2')
