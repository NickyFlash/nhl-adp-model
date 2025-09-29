import os, json, gspread
from oauth2client.service_account import ServiceAccountCredentials

def upload_to_sheets(sheet_name, tabs_dict):
    """
    Upload pandas DataFrames to Google Sheets.
    Uses the GCP_CREDENTIALS secret stored in GitHub Actions.
    
    :param sheet_name: name of the Google Sheet (must exist + shared with the service account email)
    :param tabs_dict: dictionary { "TabName": dataframe, ... }
    """
    creds_json = os.environ.get("GCP_CREDENTIALS")
    if not creds_json:
        raise RuntimeError("❌ Missing GCP_CREDENTIALS secret in environment.")

    creds_dict = json.loads(creds_json)

    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]

    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)

    # Open the Google Sheet
    sh = client.open(sheet_name)

    # Write each dataframe to its tab
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

    print(f"✅ Google Sheet '{sheet_name}' updated successfully!")
