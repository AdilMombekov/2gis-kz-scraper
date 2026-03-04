# -*- coding: utf-8 -*-
"""
Telegram-бот для управления парсером 2GIS KZ -> Google Sheets.

Команды:
  /start   — приветствие
  /run     — запустить полный парсинг
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
    get_search_page, parse_search_results, _fetch_firm_data,
)
from google_sheets import (
    _get_service, get_spreadsheet_id,
    ensure_header, get_existing_ids, append_rows,
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
        ensure_header(service, spreadsheet_id, SHEET_NAME)

        log("Загружаю существующие ID из таблицы...")
        seen_ids = get_existing_ids(service, spreadsheet_id, SHEET_NAME)
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
            written = append_rows(service, spreadsheet_id, batch, SHEET_NAME)
            total_written += written
            with _state_lock:
                _state["total_new"] = total_written
                _state["total_in_sheet"] = len(seen_ids)

        def scrape_city(city_name, city_slug):
            results = []
            for query in FLOWER_QUERIES_KZ:
                if stopped():
                    break
                for page in range(1, MAX_PAGES + 1):
                    if stopped():
                        break
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
        "/stop — остановить парсинг\n"
        "/status — текущий статус\n"
        "/count — кол-во записей в таблице\n"
        "/sheet — ссылка на таблицу\n"
        "/clear — очистить таблицу\n"
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
        result = service.spreadsheets().values().get(
            spreadsheetId=sid, range=f"{SHEET_NAME}!A:A"
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
        service.spreadsheets().values().clear(
            spreadsheetId=sid, range=f"{SHEET_NAME}!A2:Z"
        ).execute()
        bot.reply_to(message, "✅ Таблица очищена. Заголовок сохранён.")
    except Exception as e:
        bot.reply_to(message, f"❌ Ошибка: {e}")


if __name__ == "__main__":
    print("Bot started...", flush=True)
    bot.infinity_polling(timeout=30, long_polling_timeout=30)
