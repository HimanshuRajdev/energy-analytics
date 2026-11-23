"""
Ten second test of the Google Sheet connection. Shows which account is used and what Google replies.

    python -m scripts.check_sheet
"""
import json
import os

from dotenv import load_dotenv

from src import sheets_export

load_dotenv()
key = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
print("key file:", key, "| exists:", os.path.exists(key))
if os.path.exists(key):
    info = json.load(open(key))
    print("service account:", info.get("client_email"))
    print("project in key: ", info.get("project_id"))
sid = os.environ.get("SHEET_ID", "")
print("sheet id:", sid)

session = sheets_export._service()
r = session.get(f"{sheets_export.API}/{sid}", params={"fields": "properties.title,sheets.properties.title"})
print("\nGoogle replied:", r.status_code)
print(r.text[:600])
if r.status_code == 403 and "SERVICE_DISABLED" in r.text:
    print("\n=> The Google Sheets API is off for this project. Enable it in APIs & Services > Library.")
elif r.status_code in (403, 404):
    print("\n=> The sheet is not shared with the service account email above. Share it as Editor.")
elif r.ok:
    print("\n=> Connection works.")
