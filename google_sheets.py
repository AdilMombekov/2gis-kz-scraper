# -*- coding: utf-8 -*-
"""
Модуль авторизации Google Sheets через Service Account (OAuth2).
Читает credentials из переменной окружения GOOGLE_CREDENTIALS_JSON (Railway Secret).
Spreadsheet ID берётся из SPREADSHEET_ID.
"""

import os
import json
import time
import ssl
import socket
from typing import List, Dict

try:
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
except ImportError:
    raise ImportError("Установите: pip install google-auth google-auth-httplib2 google-api-python-client")

# Сетевые ошибки которые нужно повторять (SSL обрывы, таймауты, сброс соединения)
_RETRYABLE_ERRORS = (
    ssl.SSLError,
    socket.timeout,
    socket.error,
    ConnectionResetError,
    ConnectionAbortedError,
    BrokenPipeError,
    TimeoutError,
    OSError,
)
_MAX_RETRIES = 7


def _retry(fn, *args, **kwargs):
    """Выполняет fn с повторами при сетевых ошибках и rate limit."""
    for attempt in range(_MAX_RETRIES):
        try:
            return fn(*args, **kwargs)
        except HttpError as e:
            if e.resp.status == 429:
                wait = 60 * (attempt + 1)
                print(f"  Rate limit (429), ждём {wait}с…", flush=True)
                time.sleep(wait)
            elif e.resp.status in (500, 502, 503, 504):
                wait = 10 * (attempt + 1)
                print(f"  Google API {e.resp.status}, ждём {wait}с…", flush=True)
                time.sleep(wait)
            else:
                raise
        except _RETRYABLE_ERRORS as e:
            wait = 5 * (attempt + 1)
            print(f"  Сетевая ошибка ({type(e).__name__}: {e}), ждём {wait}с… (попытка {attempt+1}/{_MAX_RETRIES})", flush=True)
            time.sleep(wait)
    raise RuntimeError(f"Не удалось выполнить запрос к Google Sheets после {_MAX_RETRIES} попыток")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Столбцы в порядке таблицы 2gispars (строка 1 — заголовки)
COLUMNS = ["id_2gis", "name", "address", "city", "lat", "lon", "coordinates", "phone", "instagram", "facebook", "telegram"]


def _sheet_range(sheet_name: str, range_: str) -> str:
    """Формирует корректный range с именем листа."""
    # Google Sheets API требует одинарные кавычки вокруг имён с пробелами/спецсимволами
    # Но передаём через escaped имя — заменяем одинарные кавычки на двойные внутри
    safe = sheet_name.replace("'", "''")
    return f"'{safe}'!{range_}"


def get_or_create_safe_sheet(service, spreadsheet_id: str, preferred_name: str = "Data") -> str:
    """
    Возвращает имя листа без пробелов и кириллицы.
    Если лист с таким именем не существует — переименовывает первый лист.
    """
    meta = _retry(service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute)
    sheets = meta.get("sheets", [])

    for s in sheets:
        if s["properties"]["title"] == preferred_name:
            return preferred_name

    first_sheet_id = sheets[0]["properties"]["sheetId"]
    _retry(service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": [{"updateSheetProperties": {
            "properties": {"sheetId": first_sheet_id, "title": preferred_name},
            "fields": "title"
        }}]}
    ).execute)
    print(f"Лист переименован в '{preferred_name}'", flush=True)
    return preferred_name


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
    """Если строка 1 пустая — записывает заголовки."""
    range_ = _sheet_range(sheet_name, "A1:K1")
    result = _retry(service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=range_
    ).execute)
    existing = result.get("values", [])
    if not existing or not existing[0]:
        _retry(service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=range_,
            valueInputOption="RAW",
            body={"values": [COLUMNS]},
        ).execute)
        print("Заголовки записаны в строку 1.", flush=True)


def _normalize_id(raw_id: str) -> str:
    """Нормализует ID — убирает форматирование Google Sheets (пробелы, запятые, апостроф)."""
    return str(raw_id).lstrip("'").replace("\xa0", "").replace(" ", "").replace(",", "").replace(".", "").strip()


def get_existing_rows(service, spreadsheet_id: str, sheet_name: str = "Sheet1") -> list:
    """
    Читает все строки из таблицы (начиная со строки 2).
    Возвращает список dict с ключами по COLUMNS + '_row_index' (1-based номер строки).
    """
    result = _retry(service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=_sheet_range(sheet_name, "A2:K"),
    ).execute)
    values = result.get("values", [])
    rows = []
    for i, row in enumerate(values):
        # Дополняем до длины COLUMNS
        padded = row + [""] * (len(COLUMNS) - len(row))
        d = dict(zip(COLUMNS, padded))
        d["id_2gis"] = _normalize_id(d["id_2gis"])
        d["_row_index"] = i + 2  # строка в таблице (1-based, +1 за заголовок)
        rows.append(d)
    return rows


def get_existing_ids(service, spreadsheet_id: str, sheet_name: str = "Sheet1") -> set:
    """Читает все id_2gis из столбца A (начиная со строки 2). Нормализует числовой формат."""
    result = _retry(service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=_sheet_range(sheet_name, "A2:A"),
    ).execute)
    values = result.get("values", [])
    ids = set()
    for row in values:
        if row:
            nid = _normalize_id(row[0])
            if nid:
                ids.add(nid)
    return ids


def update_row(service, spreadsheet_id: str, row_index: int, data: dict, sheet_name: str = "Sheet1") -> None:
    """Обновляет одну строку в таблице по номеру строки (1-based)."""
    row_vals = []
    for col in COLUMNS:
        val = str(data.get(col, "") or "")
        if col == "id_2gis" and val.isdigit():
            val = "'" + val
        row_vals.append(val)
    range_ = _sheet_range(sheet_name, f"A{row_index}:K{row_index}")
    _retry(service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=range_,
        valueInputOption="RAW",
        body={"values": [row_vals]},
    ).execute)


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
            if col == "id_2gis" and val.isdigit():
                val = "'" + val
            row_vals.append(val)
        values.append(row_vals)

    _retry(service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=_sheet_range(sheet_name, "A1"),
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": values},
    ).execute)
    return len(values)
