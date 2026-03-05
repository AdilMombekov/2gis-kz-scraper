# -*- coding: utf-8 -*-
"""
Telegram-бот для управления парсером 2GIS KZ -> Google Sheets.

Команды:
  /start   — приветствие
  /run     — запустить полный парсинг (новые записи добавляются, существующие пропускаются)
  /fix     — дозаполнить координаты/телефон для строк у которых они пустые
  /stop    — остановить парсинг
  /status  — текущий статус (работает / не работает, сколько собрано)
  /count   — сколько записей сейчас в таблице
  /sheet   — ссылка на таблицу
  /clear   — очистить таблицу (оставить заголовок)
  /help    — список команд

Переменные окружения Railway:
  TELEGRAM_TOKEN           — токен бота
  GOOGLE_CREDENTIALS_JSON  — содержимое service_account.json
  SPREADSHEET_ID           — ID таблицы
  SHEET_NAME               — имя листа (по умолчанию Лист1)
  ALLOWED_USER_ID          — Telegram user_id которому разрешено управление
  MAX_WORKERS              — потоков (по умолчанию 4)
  DELAY                    — задержка (по умолчанию 0.8)
  MAX_PAGES                — страниц на запрос (по умолчанию 50)
  BATCH_SIZE               — батч записи в Sheets (по умолчанию 40)
"""

import os
import sys
import time
import threading
import traceback

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    import telebot
except ImportError:
    raise ImportError("Установите: pip install pyTelegramBotAPI")

from parser_2gis import (
    BASE_2GIS_KZ, CITIES_KZ, FLOWER_QUERIES_KZ,
    get_search_page, parse_search_results,
    search_via_api, parse_api_results,
    _fetch_firm_data, read_log_since,
    is_flower_shop,
)
from google_sheets import (
    _get_service, get_spreadsheet_id,
    ensure_header, get_existing_ids, get_existing_rows,
    append_rows, update_row, get_or_create_safe_sheet,
)
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Настройки ──────────────────────────────────────────────────────────────
TOKEN        = os.environ.get("TELEGRAM_TOKEN", "").strip()
ALLOWED_ID   = int(os.environ.get("ALLOWED_USER_ID", "0"))
MAX_WORKERS  = int(os.environ.get("MAX_WORKERS", 4))
DELAY        = float(os.environ.get("DELAY", 0.8))
MAX_PAGES    = int(os.environ.get("MAX_PAGES", 50))
BATCH_SIZE   = int(os.environ.get("BATCH_SIZE", 40))
SHEET_NAME   = os.environ.get("SHEET_NAME", "Лист1")

if not TOKEN:
    raise EnvironmentError("TELEGRAM_TOKEN не задан")

bot = telebot.TeleBot(TOKEN, parse_mode=None)

# ── Состояние парсера ──────────────────────────────────────────────────────
_state = {
    "running": False,
    "stop_flag": False,
    "total_new": 0,
    "total_in_sheet": 0,
    "current_city": "",
    "cities_done": 0,
    "cities_total": len(CITIES_KZ),
    "started_at": None,
    "thread": None,
}
_state_lock = threading.Lock()


def _allowed(message) -> bool:
    if ALLOWED_ID and message.from_user.id != ALLOWED_ID:
        bot.reply_to(message, "Нет доступа.")
        return False
    return True


def _elapsed() -> str:
    if not _state["started_at"]:
        return ""
    secs = int(time.time() - _state["started_at"])
    m, s = divmod(secs, 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}ч {m}м {s}с"
    if m:
        return f"{m}м {s}с"
    return f"{s}с"


# ── Парсер в отдельном потоке ──────────────────────────────────────────────
def _run_scraper(chat_id: int):
    def log(msg: str):
        try:
            bot.send_message(chat_id, msg)
        except Exception:
            pass

    try:
        with _state_lock:
            _state["running"] = True
            _state["stop_flag"] = False
            _state["total_new"] = 0
            _state["cities_done"] = 0
            _state["started_at"] = time.time()

        service = _get_service()
        spreadsheet_id = get_spreadsheet_id()
        sheet = get_or_create_safe_sheet(service, spreadsheet_id, "Data")
        ensure_header(service, spreadsheet_id, sheet)

        log("Загружаю существующие ID из таблицы...")
        seen_ids = get_existing_ids(service, spreadsheet_id, sheet)
        with _state_lock:
            _state["total_in_sheet"] = len(seen_ids)
        log(f"Уже в таблице: {len(seen_ids)} записей\nНачинаю парсинг {len(CITIES_KZ)} городов...")

        city_list = list(CITIES_KZ.items())
        city_slug_by_city = dict(city_list)
        lock = threading.Lock()
        new_buffer = []
        total_written = 0

        def stopped():
            return _state["stop_flag"]

        def enrich_and_write(batch):
            nonlocal total_written
            if not batch:
                return
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
                futs = []
                for item in batch:
                    slug = city_slug_by_city.get(item["city"], city_list[0][1])
                    futs.append(ex.submit(_fetch_firm_data, item, slug, BASE_2GIS_KZ, DELAY))
                for f in futs:
                    try:
                        f.result()
                    except Exception:
                        pass
            for r in batch:
                r["coordinates"] = f"{r.get('lat') or ''},{r.get('lon') or ''}".strip(",")
            written = append_rows(service, spreadsheet_id, batch, sheet)
            total_written += written
            with _state_lock:
                _state["total_new"] = total_written
                _state["total_in_sheet"] = len(seen_ids)

        def scrape_city(city_name, city_slug):
            results = []
            seen_local = set()
            for query in FLOWER_QUERIES_KZ:
                if stopped():
                    break
                for page in range(1, MAX_PAGES + 1):
                    if stopped():
                        break

                    # Пробуем API
                    api_data = search_via_api(city_slug, query, page)
                    time.sleep(DELAY)

                    if api_data is not None:
                        chunk = parse_api_results(api_data, city_name)
                        if not chunk:
                            break
                        total = api_data.get("result", {}).get("total", 0)
                        for item in chunk:
                            if item["id_2gis"] not in seen_local and is_flower_shop(item["name"]):
                                seen_local.add(item["id_2gis"])
                                item.setdefault("phone", item.get("phone", ""))
                                results.append(item)
                        if total and page * 20 >= total:
                            break
                    else:
                        # Fallback: HTML
                        html = get_search_page(city_slug, query, page, base_url=BASE_2GIS_KZ)
                        time.sleep(DELAY)
                        if not html:
                            break
                        chunk = parse_search_results(html, city_name, city_slug)
                        if not chunk:
                            break
                        for item in chunk:
                            if item["id_2gis"] not in seen_local and is_flower_shop(item["name"]):
                                seen_local.add(item["id_2gis"])
                                item.setdefault("phone", "")
                                results.append(item)
            return results

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {
                ex.submit(scrape_city, name, slug): (name, slug)
                for name, slug in city_list
            }
            for fut in as_completed(futures):
                if stopped():
                    break
                city_name, _ = futures[fut]
                try:
                    items = fut.result()
                except Exception as e:
                    log(f"⚠️ {city_name}: ошибка — {e}")
                    continue

                new_items = []
                with lock:
                    for item in items:
                        if item["id_2gis"] not in seen_ids:
                            seen_ids.add(item["id_2gis"])
                            new_items.append(item)
                            new_buffer.append(item)

                with _state_lock:
                    _state["cities_done"] += 1
                    _state["current_city"] = city_name

                log(f"✅ {city_name}: +{len(new_items)} новых | буфер: {len(new_buffer)} | время: {_elapsed()}")

                if len(new_buffer) >= BATCH_SIZE:
                    batch = list(new_buffer)
                    new_buffer.clear()
                    log(f"💾 Записываю {len(batch)} записей в таблицу...")
                    enrich_and_write(batch)
                    log(f"✔️ Записано. Всего новых: {total_written}")

        # Финальный сброс
        if new_buffer and not stopped():
            log(f"💾 Финальная запись {len(new_buffer)} записей...")
            enrich_and_write(list(new_buffer))
            new_buffer.clear()

        sid = get_spreadsheet_id()
        if stopped():
            log(f"🛑 Остановлено вручную.\nЗаписано новых: {total_written}\nhttps://docs.google.com/spreadsheets/d/{sid}")
        else:
            log(f"🎉 Готово! Записано новых: {total_written}\nВремя: {_elapsed()}\nhttps://docs.google.com/spreadsheets/d/{sid}")

    except Exception as e:
        try:
            bot.send_message(chat_id, f"❌ Критическая ошибка:\n{e}\n\n{traceback.format_exc()[:1000]}")
        except Exception:
            pass
    finally:
        with _state_lock:
            _state["running"] = False
            _state["stop_flag"] = False


# ── Команды бота ───────────────────────────────────────────────────────────
@bot.message_handler(commands=["start", "help"])
def cmd_start(message):
    if not _allowed(message):
        return
    bot.reply_to(message, (
        "🌸 2GIS KZ Flower Scraper\n\n"
        "Команды:\n"
        "/run — запустить полный парсинг\n"
        "       (существующие записи пропускаются)\n"
        "/fix — дозаполнить пустые координаты/телефон\n"
        "/stop — остановить парсинг\n"
        "/status — текущий статус\n"
        "/count — кол-во записей в таблице\n"
        "/sheet — ссылка на таблицу\n"
        "/clear — очистить таблицу\n"
        "/log2h — что добавлено за последние 2 часа\n"
        "/log5h — что добавлено за последние 5 часов\n"
        "/log10h — что добавлено за последние 10 часов\n"
        "/help — эта справка"
    ))


@bot.message_handler(commands=["run"])
def cmd_run(message):
    if not _allowed(message):
        return
    with _state_lock:
        if _state["running"]:
            bot.reply_to(message, "⚙️ Парсинг уже запущен. /status — посмотреть прогресс.")
            return

    bot.reply_to(message, (
        f"🚀 Запускаю парсинг...\n"
        f"Городов: {len(CITIES_KZ)}, Запросов: {len(FLOWER_QUERIES_KZ)}\n"
        f"Страниц/запрос: {MAX_PAGES}, Потоков: {MAX_WORKERS}\n"
        f"Буду писать в таблицу батчами по {BATCH_SIZE} записей."
    ))

    t = threading.Thread(target=_run_scraper, args=(message.chat.id,), daemon=True)
    with _state_lock:
        _state["thread"] = t
    t.start()


@bot.message_handler(commands=["stop"])
def cmd_stop(message):
    if not _allowed(message):
        return
    with _state_lock:
        if not _state["running"]:
            bot.reply_to(message, "Парсинг не запущен.")
            return
        _state["stop_flag"] = True
    bot.reply_to(message, "🛑 Отправлен сигнал остановки. Подожди завершения текущего города...")


@bot.message_handler(commands=["status"])
def cmd_status(message):
    if not _allowed(message):
        return
    with _state_lock:
        running = _state["running"]
        new = _state["total_new"]
        done = _state["cities_done"]
        total = _state["cities_total"]
        city = _state["current_city"]
        elapsed = _elapsed()

    if running:
        bot.reply_to(message, (
            f"⚙️ Парсинг идёт\n"
            f"Городов: {done}/{total}\n"
            f"Последний: {city}\n"
            f"Записано новых: {new}\n"
            f"Время: {elapsed}"
        ))
    else:
        bot.reply_to(message, f"💤 Парсинг не запущен.\nПоследний раз записано: {new} новых за {elapsed}")


@bot.message_handler(commands=["count"])
def cmd_count(message):
    if not _allowed(message):
        return
    bot.reply_to(message, "⏳ Считаю...")
    try:
        service = _get_service()
        sid = get_spreadsheet_id()
        sheet = get_or_create_safe_sheet(service, sid, "Data")
        result = service.spreadsheets().values().get(
            spreadsheetId=sid, range=f"'{sheet}'!A:A"
        ).execute()
        rows = result.get("values", [])
        bot.reply_to(message, f"📊 Записей в таблице: {len(rows) - 1}")
    except Exception as e:
        bot.reply_to(message, f"❌ Ошибка: {e}")


@bot.message_handler(commands=["sheet"])
def cmd_sheet(message):
    if not _allowed(message):
        return
    try:
        sid = get_spreadsheet_id()
        bot.reply_to(message, f"📋 Таблица:\nhttps://docs.google.com/spreadsheets/d/{sid}")
    except Exception as e:
        bot.reply_to(message, f"❌ Ошибка: {e}")


@bot.message_handler(commands=["clear"])
def cmd_clear(message):
    if not _allowed(message):
        return
    with _state_lock:
        if _state["running"]:
            bot.reply_to(message, "⚠️ Нельзя очистить во время парсинга. Сначала /stop")
            return
    bot.reply_to(message, "⏳ Очищаю таблицу...")
    try:
        service = _get_service()
        sid = get_spreadsheet_id()
        sheet = get_or_create_safe_sheet(service, sid, "Data")
        service.spreadsheets().values().clear(
            spreadsheetId=sid, range=f"'{sheet}'!A2:Z"
        ).execute()
        bot.reply_to(message, "✅ Таблица очищена. Заголовок сохранён.")
    except Exception as e:
        bot.reply_to(message, f"❌ Ошибка: {e}")


def _run_fix(chat_id: int):
    """Дозаполняет координаты/телефон/соцсети для строк у которых они пустые."""
    def log(msg):
        try:
            bot.send_message(chat_id, msg)
        except Exception:
            pass

    try:
        with _state_lock:
            _state["running"] = True
            _state["stop_flag"] = False
            _state["started_at"] = time.time()

        service = _get_service()
        spreadsheet_id = get_spreadsheet_id()
        sheet = get_or_create_safe_sheet(service, spreadsheet_id, "Data")
        city_list = list(CITIES_KZ.items())
        city_slug_by_city = dict(city_list)

        log("Читаю таблицу...")
        all_rows = get_existing_rows(service, spreadsheet_id, sheet)
        # Строки у которых нет координат
        need_fix = [r for r in all_rows if not r.get("lat") or not r.get("lon")]
        log(f"Всего строк: {len(all_rows)}\nНужно дозаполнить: {len(need_fix)}")

        if not need_fix:
            log("✅ Все строки уже заполнены!")
            return

        fixed = 0
        for i, row in enumerate(need_fix):
            if _state["stop_flag"]:
                break
            slug = city_slug_by_city.get(row.get("city", ""), city_list[0][1])
            try:
                _fetch_firm_data(row, slug, BASE_2GIS_KZ, DELAY)
                row["coordinates"] = f"{row.get('lat') or ''},{row.get('lon') or ''}".strip(",")
                update_row(service, spreadsheet_id, row["_row_index"], row, sheet)
                fixed += 1
            except Exception as e:
                log(f"⚠️ Ошибка строки {row.get('id_2gis')}: {e}")
            if (i + 1) % 50 == 0:
                log(f"⚙️ Обработано {i+1}/{len(need_fix)}, исправлено: {fixed}")
            time.sleep(DELAY)

        log(f"✅ Готово! Дозаполнено: {fixed}/{len(need_fix)} строк\nВремя: {_elapsed()}")

    except Exception as e:
        try:
            bot.send_message(chat_id, f"❌ Ошибка /fix:\n{e}\n{traceback.format_exc()[:800]}")
        except Exception:
            pass
    finally:
        with _state_lock:
            _state["running"] = False
            _state["stop_flag"] = False


@bot.message_handler(commands=["fix"])
def cmd_fix(message):
    if not _allowed(message):
        return
    with _state_lock:
        if _state["running"]:
            bot.reply_to(message, "⚙️ Уже что-то работает. /status — посмотреть.")
            return
    bot.reply_to(message, "🔧 Запускаю дозаполнение пустых координат/телефонов...")
    t = threading.Thread(target=_run_fix, args=(message.chat.id,), daemon=True)
    with _state_lock:
        _state["thread"] = t
    t.start()


def _format_log_report(hours: float) -> str:
    """Формирует текстовый отчёт по логу за последние N часов."""
    entries = read_log_since(hours)
    if not entries:
        return f"📭 За последние {int(hours)}ч лог пуст (или парсинг не запускался)."

    city_entries = [e for e in entries if e.get("event") == "city_done"]
    start_entries = [e for e in entries if e.get("event") == "start"]
    finish_entries = [e for e in entries if e.get("event") == "finish"]

    total_added = sum(e.get("added", 0) for e in city_entries)
    cities_processed = len(city_entries)

    lines = [f"📊 Лог за последние {int(hours)}ч:"]
    lines.append(f"Запусков парсинга: {len(start_entries)}")
    lines.append(f"Городов обработано: {cities_processed}")
    lines.append(f"Новых записей добавлено: {total_added}")

    if finish_entries:
        last_finish = finish_entries[-1]
        lines.append(f"Последний финиш: {last_finish.get('ts', '?')} | итого {last_finish.get('total', '?')} записей")

    if city_entries:
        lines.append("\nПоследние города:")
        for e in city_entries[-10:]:
            lines.append(f"  {e.get('ts','?')} {e.get('city','?')}: +{e.get('added',0)} (найдено {e.get('found',0)})")

    return "\n".join(lines)


@bot.message_handler(commands=["log2h"])
def cmd_log2h(message):
    if not _allowed(message):
        return
    bot.reply_to(message, _format_log_report(2))


@bot.message_handler(commands=["log5h"])
def cmd_log5h(message):
    if not _allowed(message):
        return
    bot.reply_to(message, _format_log_report(5))


@bot.message_handler(commands=["log10h"])
def cmd_log10h(message):
    if not _allowed(message):
        return
    bot.reply_to(message, _format_log_report(10))


@bot.message_handler(commands=["test"])
def cmd_test(message):
    if not _allowed(message):
        return
    bot.reply_to(message, "🔍 Тестирую API и HTML парсинг для Алматы / цветы...")

    import requests as _req
    results = []

    # Тест 1: Catalog API
    try:
        r = _req.get(
            "https://catalog.api.2gis.com/3.0/items",
            params={"key": "demos", "q": "цветы", "type": "branch",
                    "locale": "ru_KZ", "page": 1, "page_size": 5},
            timeout=15,
        )
        data = r.json()
        total = data.get("result", {}).get("total", "?")
        items = data.get("result", {}).get("items", [])
        results.append(f"✅ Catalog API (без region_id):\nstatus={r.status_code}, total={total}, items={len(items)}")
        if items:
            results.append(f"  Первый: {items[0].get('name','?')} | id={items[0].get('id','?')}")
        else:
            results.append(f"  Ответ: {str(data)[:300]}")
    except Exception as e:
        results.append(f"❌ Catalog API ошибка: {e}")

    # Тест 2: API с region_id Алматы
    try:
        r2 = _req.get(
            "https://catalog.api.2gis.com/2.0/region/list",
            params={"key": "demos", "q": "almaty", "locale": "ru_KZ"},
            timeout=15,
        )
        rdata = r2.json()
        reg_items = rdata.get("result", {}).get("items", [])
        if reg_items:
            rid = reg_items[0].get("id")
            results.append(f"✅ Region API: almaty region_id={rid}")
            r3 = _req.get(
                "https://catalog.api.2gis.com/3.0/items",
                params={"key": "demos", "q": "цветы", "type": "branch",
                        "locale": "ru_KZ", "region_id": rid, "page": 1, "page_size": 5},
                timeout=15,
            )
            d3 = r3.json()
            total3 = d3.get("result", {}).get("total", "?")
            items3 = d3.get("result", {}).get("items", [])
            results.append(f"  С region_id: total={total3}, items={len(items3)}")
            if items3:
                results.append(f"  Первый: {items3[0].get('name','?')}")
            else:
                results.append(f"  Ответ: {str(d3)[:300]}")
        else:
            results.append(f"❌ Region API пустой: {str(rdata)[:200]}")
    except Exception as e:
        results.append(f"❌ Region API ошибка: {e}")

    # Тест 3: HTML страница
    try:
        from parser_2gis import HEADERS
        r4 = _req.get("https://2gis.kz/almaty/search/%D1%86%D0%B2%D0%B5%D1%82%D1%8B",
                      headers=HEADERS, timeout=15)
        html = r4.text
        firm_count = html.count("/firm/")
        script_count = html.count("<script")
        results.append(f"✅ HTML 2gis.kz: status={r4.status_code}, /firm/ упоминаний={firm_count}, <script>={script_count}, размер={len(html)}")
    except Exception as e:
        results.append(f"❌ HTML ошибка: {e}")

    bot.send_message(message.chat.id, "\n".join(results))


if __name__ == "__main__":
    print("Bot started...", flush=True)
    bot.infinity_polling(timeout=30, long_polling_timeout=30)
