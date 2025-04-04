import pandas as pd
import pytchat
import xlsxwriter
import io
import time
import json
import os
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from datetime import datetime, timedelta

# 環境変数からサービスアカウントキーを取得
google_credentials_json = os.getenv("GOOGLE_SERVICE_ACCOUNT")
if not google_credentials_json:
    raise ValueError("GOOGLE_SERVICE_ACCOUNT が設定されていません。")
json_data = json.loads(google_credentials_json)

# Google Drive API 認証
credentials = service_account.Credentials.from_service_account_info(json_data)
drive_service = build("drive", "v3", credentials=credentials)

# Google Drive からファイル ID を取得
def get_file_id(file_name):
    query = f"name = '{file_name}' and trashed = false"
    results = drive_service.files().list(q=query, fields="files(id, name, mimeType)").execute()
    files = results.get("files", [])
    return (files[0]["id"], files[0]["mimeType"]) if files else (None, None)

# Google スプレッドシートを Excel 形式でエクスポート
def download_google_sheets_as_excel(file_id):
    request = drive_service.files().export_media(fileId=file_id, mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.seek(0)
    return pd.read_excel(fh)

# YouTube Live の動画 ID
video_id = "4FBW3mkdKOs"
chat = pytchat.create(video_id)

# 記録ファイルの取得と更新
history_file = "chat_shukei.xlsx"
history_id, history_mime = get_file_id(history_file)

if history_id:
    try:
        if history_mime == "application/vnd.google-apps.spreadsheet":
            history_df = download_google_sheets_as_excel(history_id)
        else:
            # Excel ファイルがある場合、読み込む
            history_df = pd.read_excel(f"https://drive.google.com/uc?id={history_id}")
        # Excel にカラムがない場合は空の DataFrame を作成
        if history_df.empty:
            history_df = pd.DataFrame(columns=["timestamp", "username", "message"])
    except Exception as e:
        print(f"ファイル読み込み失敗: {e}")
        history_df = pd.DataFrame(columns=["timestamp", "username", "message"])
else:
    history_df = pd.DataFrame(columns=["timestamp", "username", "message"])

# 4時間後に終了するための時間を設定
end_time = datetime.now() + timedelta(hours=4)

# チャットをリアルタイムで記録
while chat.is_alive():
    # 4時間経過したら終了
    if datetime.now() > end_time:
        break
    
    new_data = []
    for c in chat.get().items:
        new_data.append([c.datetime, c.author.name, c.message])
        print(f"[{c.datetime}] {c.author.name}: {c.message}")

    if new_data:
        new_df = pd.DataFrame(new_data, columns=["timestamp", "username", "message"])
        history_df = pd.concat([history_df, new_df], ignore_index=True)

        # Google Drive にアップロード（Excel）
        with io.BytesIO() as fh:
            with pd.ExcelWriter(fh, engine='xlsxwriter') as writer:
                history_df.to_excel(writer, index=False)
            fh.seek(0)
            media = MediaIoBaseUpload(fh, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

            if history_id:
                drive_service.files().update(fileId=history_id, media_body=media).execute()
            else:
                file_metadata = {"name": history_file, "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"}
                drive_service.files().create(body=file_metadata, media_body=media).execute()

    time.sleep(5)  # 5秒ごとに更新
