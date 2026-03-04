# -*- coding: utf-8 -*-
"""
Railway-скрипт: парсит все цветочные 2GIS Казахстан и пишет результаты
прямо в Google Sheets (таблица 2gispars).

Переменные окружения Railway:
  GOOGLE_CREDENTIALS_JSON  -- содержимое service_account.json (всё одной строкой)
  SPREADSHEET_ID           -- ID таблицы Google Sheets
  SHEET_NAME               -- имя листа (по умолчанию Лист1)
  MAX_WORKERS              -- потоков (по умолчанию 4)
  DELAY                    -- задержка между запросами в сек (по умолчанию 0.8)
  MAX_PAGES                -- страниц на запрос/город (по умолчанию 50)
  BATCH_SIZE               -- сколько строк писать в Sheets за раз (по умолчанию 50)
"""

import os
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from parser_2gis import (
    BASE_2GIS_KZ,
    CITIES_KZ,
    FLOWER_QUERIES_KZ,
    get_search_page,
    parse_search_results,
    _fetch_firm_data,
)
from google_sheets import (
    _get_service,
    get_spreadsheet_id,
    ensure_header,
    get_existing_ids,
    append_rows,
)

MAX_WORKERS = int(os.environ.get("MAX_WORKERS", 4))
DELAY       = float(os.environ.get("DELAY", 0.8))
MAX_PAGES   = int(os.environ.get("MAX_PAGES", 50))
BATCH_SIZE  = int(os.environ.get("BATCH_SIZE", 50))
SHEET_NAME  = os.environ.get("SHEET_NAME", "Лист1")


def log(msg: str) -> None:
    print(msg, flush=True)


def scrape_one_city(city_name: str, city_slug: str) -> list:
    """Собирает все карточки по одному городу."""
    results = []
    for query in FLOWER_QUERIES_KZ:
        for page in range(1, MAX_PAGES + 1):
            html = get_search_page(city_slug, query, page, base_url=BASE_2GIS_KZ)
            time.sleep(DELAY)
            if not html:
                continue
            chunk = parse_search_results(html, city_name, city_slug)
            for item in chunk:
                item.setdefault("phone", "")
                results.append(item)
            if not chunk:
                break
    return results


def enrich_batch(items: list, city_slug_by_city: dict, city_list: list) -> None:
    """Обогащает батч карточек координатами, телефоном и соцсетями."""
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = []
        for item in items:
            slug = city_slug_by_city.get(item["city"], city_list[0][1])
            futs.append(ex.submit(_fetch_firm_data, item, slug, BASE_2GIS_KZ, DELAY))
        for i, f in enumerate(futs):
            try:
                f.result()
            except Exception as e:
                log(f"    Ошибка обогащения #{i}: {e}")
    for r in items:
        r["coordinates"] = f"{r.get('lat') or ''},{r.get('lon') or ''}".strip(",")


def main():
    log("=" * 60)
    log("2GIS KZ Flower Scraper -> Google Sheets")
    log(f"Городов: {len(CITIES_KZ)}, Запросов: {len(FLOWER_QUERIES_KZ)}")
    log(f"Потоков: {MAX_WORKERS}, Задержка: {DELAY}s, Страниц/запрос: {MAX_PAGES}")
    log("=" * 60)

    service = _get_service()
    spreadsheet_id = get_spreadsheet_id()
    ensure_header(service, spreadsheet_id, SHEET_NAME)

    log("Загружаем существующие ID из таблицы...")
    seen_ids = get_existing_ids(service, spreadsheet_id, SHEET_NAME)
    log(f"Уже в таблице: {len(seen_ids)} записей")

    city_list = list(CITIES_KZ.items())
    city_slug_by_city = dict(city_list)
    lock = threading.Lock()
    new_buffer: list = []   # новые записи ещё без координат
    total_written = 0

    def write_buffer_to_sheets():
        """Обогащает буфер и пишет в Sheets. Вызывать без lock."""
        nonlocal total_written
        if not new_buffer:
            return
        batch = list(new_buffer)
        new_buffer.clear()
        log(f"  Обогащение {len(batch)} карточек (координаты, телефон, соцсети)...")
        enrich_batch(batch, city_slug_by_city, city_list)
        written = append_rows(service, spreadsheet_id, batch, SHEET_NAME)
        total_written += written
        log(f"  >> Записано в Sheets: {written}, всего в таблице: {total_written + len(seen_ids)}")

    # Фаза 1: сбор по городам параллельно
    log(f"Фаза 1: сбор по {len(city_list)} городам...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {
            ex.submit(scrape_one_city, name, slug): (name, slug)
            for name, slug in city_list
        }
        for fut in as_completed(futures):
            city_name, _ = futures[fut]
            try:
                items = fut.result()
            except Exception as e:
                log(f"  {city_name}: ошибка -- {e}")
                continue

            new_items = []
            with lock:
                for item in items:
                    if item["id_2gis"] not in seen_ids:
                        seen_ids.add(item["id_2gis"])
                        new_items.append(item)
                        new_buffer.append(item)

            log(f"  {city_name}: +{len(new_items)} новых (буфер: {len(new_buffer)})")

            # Когда буфер достиг BATCH_SIZE — обогащаем и пишем
            if len(new_buffer) >= BATCH_SIZE:
                write_buffer_to_sheets()

    # Финальный сброс остатка
    if new_buffer:
        log(f"Финальный сброс: {len(new_buffer)} записей...")
        write_buffer_to_sheets()

    log("=" * 60)
    log(f"Готово! Всего записано новых: {total_written}")
    log(f"Таблица: https://docs.google.com/spreadsheets/d/{spreadsheet_id}")


if __name__ == "__main__":
    import traceback
    try:
        main()
    except Exception:
        traceback.print_exc()
        sys.exit(1)
