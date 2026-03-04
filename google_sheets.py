# -*- coding: utf-8 -*-
"""
Модуль авторизации Google Sheets через Service Account (OAuth2).
Читает credentials из переменной окружения GOOGLE_CREDENTIALS_JSON (Railway Secret).
Spreadsheet ID берётся из SPREADSHEET_ID.
"""

import os
import json
import time
from typing import List, Dict

try:
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
except ImportError:
    raise ImportError("Установите: pip install google-auth google-auth-httplib2 google-api-python-client")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Столбцы в порядке таблицы 2gispars (строка 1 — заголовки)
COLUMNS = ["id_2gis", "name", "address", "city", "lat", "lon", "coordinates", "phone", "instagram", "facebook", "telegram"]


def _get_service():
    """Создаёт авторизованный сервис Google Sheets API."""
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if not creds_json:
        raise EnvironmentError(
            "Переменная окружения GOOGLE_CREDENTIALS_JSON не задана. "
            "Добавьте её в Railway → Variables как содержимое service_account.json."
        )
    info = json.loads(creds_json)
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def get_spreadsheet_id() -> str:
    sid = os.environ.get("SPREADSHEET_ID", "").strip()
    if not sid:
        raise EnvironmentError(
            "Переменная окружения SPREADSHEET_ID не задана. "
            "Пример: 1sY3aWslCs5f3jxypf8CA-_qEP4YAD08vpXnjyUelLZc"
        )
    return sid


def ensure_header(service, spreadsheet_id: str, sheet_name: str = "Sheet1") -> None:
    """Если строка 1 пустая — записывает заголовки. Всегда пишет данные как текст (USER_ENTERED)."""
    range_ = f"{sheet_name}!A1:K1"
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=range_
    ).execute()
    existing = result.get("values", [])
    if not existing or not existing[0]:
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=range_,
            valueInputOption="RAW",
            body={"values": [COLUMNS]},
        ).execute()
        print("Заголовки записаны в строку 1.", flush=True)


def get_existing_ids(service, spreadsheet_id: str, sheet_name: str = "Sheet1") -> set:
    """Читает все id_2gis из столбца A (начиная со строки 2). Нормализует числовой формат."""
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"{sheet_name}!A2:A",
    ).execute()
    values = result.get("values", [])
    ids = set()
    for row in values:
        if row:
            # Убираем пробелы и запятые которые Google Sheets добавляет к числам
            raw = str(row[0]).replace("\xa0", "").replace(" ", "").replace(",", "").replace(".", "").strip()
            ids.add(raw)
            ids.add(row[0])  # оригинал тоже
    return ids


def append_rows(service, spreadsheet_id: str, rows: List[Dict], sheet_name: str = "Sheet1") -> int:
    """
    Дописывает строки в конец таблицы.
    Возвращает количество добавленных строк.
    """
    if not rows:
        return 0
    values = []
    for r in rows:
        row_vals = []
        for col in COLUMNS:
            val = str(r.get(col, "") or "")
            # Апостроф перед числовыми ID чтобы Sheets не конвертировал в число
            if col == "id_2gis" and val.isdigit():
                val = "'" + val
            row_vals.append(val)
        values.append(row_vals)

    body = {"values": values}
    for attempt in range(5):
        try:
            service.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body=body,
            ).execute()
            return len(values)
        except HttpError as e:
            if e.resp.status == 429:
                # Rate limit — ждём и повторяем
                wait = 60 * (attempt + 1)
                print(f"  Rate limit, ждём {wait}с…", flush=True)
                time.sleep(wait)
            else:
                raise
    return 0
