import os, json, gspread
from oauth2client.service_account import ServiceAccountCredentials

def upload_to_sheets(sheet_name, tabs_dict):
    """
    Upload pandas DataFrames to Google Sheets (multiple tabs).
    Uses the GCP_CREDENTIALS GitHub Actions secret (JSON string).
    """
    creds_json = os.environ.get("GCP_CREDENTIALS")
    if not creds_json:
        raise RuntimeError("❌ Missing GCP_CREDENTIALS secret in environment.")

    creds_dict = json.loads(creds_json)

    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)

    # open the target sheet (must already exist & be shared with the service account email)
    sh = client.open(sheet_name)

    for tab_name, df in tabs_dict.items():
        try:
            ws = sh.worksheet(tab_name)
        except gspread.exceptions.WorksheetNotFound:
            ws = sh.add_worksheet(title=tab_name, rows="2000", cols="50")

        ws.clear()
        if df is None or df.empty:
            ws.update([["(no rows)"]])
        else:
            ws.update([df.columns.tolist()] + df.values.tolist())
