# -*- coding: utf-8 -*-
"""
Модуль парсинга 2GIS и др. Платформа, запрос по названию, мультигород, многопоточность.
Регион по умолчанию: Казахстан (2gis.kz).
"""

import re
import time
import csv
import json
import datetime
import urllib.parse
from pathlib import Path
from typing import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

import requests
from bs4 import BeautifulSoup

# ── Файл лога с временными метками ────────────────────────────────────────
_LOG_FILE = Path(__file__).resolve().parent / "parse_log.jsonl"
_log_lock = threading.Lock()


def _write_log_entry(entry: dict) -> None:
    entry["ts"] = datetime.datetime.now().isoformat(timespec="seconds")
    with _log_lock:
        with open(_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def read_log_since(hours: float) -> list[dict]:
    """Возвращает все записи лога за последние N часов."""
    if not _LOG_FILE.exists():
        return []
    cutoff = datetime.datetime.now() - datetime.timedelta(hours=hours)
    result = []
    with open(_LOG_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
                ts = datetime.datetime.fromisoformat(entry.get("ts", ""))
                if ts >= cutoff:
                    result.append(entry)
            except Exception:
                pass
    return result

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
}

# Казахстан: 2gis.kz — город -> slug в URL (все доступные на 2gis.kz)
# Порядок: крупные города первыми (Астана, Алматы, Шымкент), затем остальные
BASE_2GIS_KZ = "https://2gis.kz"
CITIES_KZ = {
    "Астана": "astana",
    "Алматы": "almaty",
    "Шымкент": "shymkent",
    "Караганда": "karaganda",
    "Актобе": "aktobe",
    "Тараз": "taraz",
    "Павлодар": "pavlodar",
    "Усть-Каменогорск": "oskemen",
    "Семей": "semey",
    "Атырау": "atyrau",
    "Костанай": "kostanay",
    "Петропавловск": "petropavl",
    "Уральск": "oral",
    "Туркестан": "turkistan",
    "Кызылорда": "kyzylorda",
    "Актау": "aktau",
    "Темиртау": "temirtau",
    "Кокшетау": "kokchetav",
    "Талдыкорган": "taldikorgan",
    "Экибастуз": "ekibastuz",
    "Рудный": "rudny",
    "Жанаозен": "zhanaozen",
    "Балхаш": "balkhash",
    "Сарань": "saran",
    "Капшагай": "kapshagay",
    "Сатпаев": "satpaev",
    "Кандыагаш": "kandyagash",
    "Жанаарка": "zhanaarka",
    "Аркалык": "arkalyk",
    "Степногорск": "stepnogorsk",
    "Щучинск": "shchuchinsk",
    "Риддер": "ridder",
    "Зыряновск": "zyryanovsk",
    "Аягоз": "ayagoz",
    "Курчатов": "kurchatov",
    "Серебрянск": "serebryansk",
    "Шахтинск": "shakhtinsk",
    "Приозёрск": "priozersk",
    "Каратау": "karatau",
    "Кульсары": "kulsary",
    "Аксай": "aksay",
    "Форт-Шевченко": "fort_shevchenko",
}

# Стоп-слова: если название содержит одно из них — запись отфильтровывается
FLOWER_STOPWORDS = [
    "комнатные растения", "горшечные", "горшок", "питомник", "семена", "рассада",
    "огород", "дача", "садовый центр", "садовые растения", "ландшафт", "газон",
    "удобрения", "агро", "зоомагазин", "зоотовары", "ветеринар", "аптека",
    "продукты", "супермаркет", "гипермаркет", "магазин продуктов", "хозяйственный",
    "строительный", "мебель", "текстиль", "одежда", "обувь", "ювелир",
    "автозапчасти", "шиномонтаж", "автосервис", "кафе", "ресторан", "столовая",
    "пиццерия", "суши", "банк", "страхование", "нотариус", "юридические",
    "бухгалтер", "стоматолог", "клиника", "больница", "медицин", "школа",
    "детский сад", "университет", "колледж", "гостиница", "отель", "хостел",
    "парикмахер", "салон красоты", "маникюр", "спортзал", "фитнес",
    "химчистка", "прачечная", "ремонт телефон", "ремонт обуви", "ателье",
    "похоронное", "ритуальные услуги", "памятники",
]


def is_flower_shop(name: str) -> bool:
    name_lower = name.lower()
    for stop in FLOWER_STOPWORDS:
        if stop in name_lower:
            return False
    return True


# Все поисковые запросы для цветочных — максимальный охват 10000+ по КЗ
FLOWER_QUERIES_KZ = [
    "цветы",
    "цветочный магазин",
    "салон цветов",
    "доставка цветов",
    "букеты",
    "флорист",
    "цветочный салон",
    "купить цветы",
    "заказать цветы",
    "розы",
    "букет",
    "цветы с доставкой",
    "свадебная флористика",
    "траурные букеты",
    "комнатные растения",
    "горшечные цветы",
    "магазин цветов",
    "доставка букетов",
    "флористика",
    "цветочная лавка",
    "букеты алматы",
    "букеты астана",
    "розы с доставкой",
    "искусственные цветы",
    "сухоцветы",
    "подарочные букеты",
    "гүлдер",
    "гүл дүкені",
    "букет цветов",
]

# Россия (для совместимости): 2gis.ru
BASE_2GIS_RU = "https://www.2gis.ru"
CITIES = {
    "Москва": "moscow",
    "Санкт-Петербург": "spb",
    "Новосибирск": "novosibirsk",
    "Екатеринбург": "ekaterinburg",
    "Казань": "kazan",
    "Нижний Новгород": "nizhny_novgorod",
    "Самара": "samara",
    "Ростов-на-Дону": "rostov_on_don",
    "Краснодар": "krasnodar",
    "Воронеж": "voronezh",
    "Уфа": "ufa",
    "Красноярск": "krasnoyarsk",
    "Пермь": "perm",
    "Волгоград": "volgograd",
    "Саратов": "saratov",
    "Тюмень": "tyumen",
    "Ижевск": "izhevsk",
    "Барнаул": "barnaul",
    "Иркутск": "irkutsk",
    "Хабаровск": "khabarovsk",
    "Ярославль": "yaroslavl",
    "Владивосток": "vladivostok",
    "Махачкала": "makhachkala",
    "Томск": "tomsk",
    "Оренбург": "orenburg",
    "Кемерово": "kemerovo",
    "Новокузнецк": "novokuznetsk",
    "Рязань": "ryazan",
    "Астрахань": "astrahan",
    "Набережные Челны": "naberezhnye_chelny",
    "Пенза": "penza",
    "Киров": "kirov",
    "Липецк": "lipetsk",
    "Тула": "tula",
    "Чебоксары": "cheboksary",
    "Калининград": "kaliningrad",
    "Курск": "kursk",
    "Ульяновск": "ulyanovsk",
    "Магнитогорск": "magnitogorsk",
    "Тверь": "tver",
    "Ставрополь": "stavropol",
    "Сочи": "sochi",
    "Белгород": "belgorod",
    "Владикавказ": "vladikavkaz",
}


# ── 2GIS Catalog API (внутренний, используется самим сайтом) ──────────────
_API_BASE = "https://catalog.api.2gis.com/3.0/items"
_API_KEY = "demos"  # публичный демо-ключ 2GIS

# Словарь: slug города -> region_id для API (получается из первого запроса)
_REGION_ID_CACHE: dict[str, str] = {}


def _get_region_id(city_slug: str, timeout: int = 15) -> str | None:
    """Получает region_id города по slug через API регионов 2GIS."""
    if city_slug in _REGION_ID_CACHE:
        return _REGION_ID_CACHE[city_slug]
    try:
        r = requests.get(
            "https://catalog.api.2gis.com/2.0/region/list",
            params={"key": _API_KEY, "q": city_slug, "locale": "ru_KZ"},
            headers=HEADERS, timeout=timeout,
        )
        data = r.json()
        items = data.get("result", {}).get("items", [])
        if items:
            rid = str(items[0].get("id", ""))
            _REGION_ID_CACHE[city_slug] = rid
            return rid
    except Exception:
        pass
    return None


def search_via_api(city_slug: str, query: str, page: int, timeout: int = 20, max_retries: int = 3) -> list[dict] | None:
    """
    Поиск через 2GIS Catalog API.
    Возвращает список организаций или None при ошибке.
    """
    region_id = _get_region_id(city_slug, timeout)
    params = {
        "key": _API_KEY,
        "q": query,
        "type": "branch",
        "locale": "ru_KZ",
        "fields": "items.point,items.address,items.contact_groups,items.rubrics",
        "page": page,
        "page_size": 20,
    }
    if region_id:
        params["region_id"] = region_id

    for attempt in range(max_retries):
        try:
            r = requests.get(_API_BASE, params=params, headers=HEADERS, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception:
            if attempt < max_retries - 1:
                time.sleep(2 + attempt * 2)
    return None


def parse_api_results(data: dict, city_name: str) -> list[dict]:
    """Разбирает ответ 2GIS Catalog API в список словарей."""
    items = []
    result = data.get("result", {})
    if not result:
        return items
    for obj in result.get("items", []):
        firm_id = str(obj.get("id", "")).split("_")[0]
        if not firm_id:
            continue
        name = (obj.get("name") or obj.get("full_name") or "").strip()
        if not name:
            continue

        address = ""
        addr_obj = obj.get("address_name") or obj.get("address") or ""
        if isinstance(addr_obj, str):
            address = addr_obj
        elif isinstance(addr_obj, dict):
            address = addr_obj.get("name", "") or addr_obj.get("full_name", "")

        lat, lon = "", ""
        point = obj.get("point") or {}
        if point:
            lat = str(point.get("lat", ""))
            lon = str(point.get("lon", ""))

        phone = ""
        for cg in obj.get("contact_groups") or []:
            for c in cg.get("contacts") or []:
                if c.get("type") == "phone":
                    phone = c.get("value", "").replace(" ", "").replace("-", "")
                    break
            if phone:
                break

        items.append({
            "id_2gis": firm_id,
            "name": name[:200],
            "address": address[:300],
            "city": city_name,
            "lat": lat,
            "lon": lon,
            "phone": phone,
            "instagram": "",
            "facebook": "",
            "telegram": "",
        })

    seen = set()
    return [x for x in items if x["id_2gis"] not in seen and not seen.add(x["id_2gis"])]


def get_search_page(city_slug: str, query: str, page: int, base_url: str = BASE_2GIS_KZ, timeout: int = 20, max_retries: int = 3) -> str | None:
    """Оставлен для совместимости. Возвращает HTML страницы поиска."""
    base = base_url.rstrip("/")
    url = f"{base}/{city_slug}/search/{urllib.parse.quote(query)}"
    if page > 1:
        url += f"/page/{page}"
    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            r.raise_for_status()
            r.encoding = "utf-8"
            return r.text
        except Exception:
            if attempt < max_retries - 1:
                time.sleep(2 + attempt * 2)
    return None


def get_firm_page(city_slug: str, firm_id: str, base_url: str = BASE_2GIS_KZ, timeout: int = 20, max_retries: int = 3) -> str | None:
    base = base_url.rstrip("/")
    url = f"{base}/{city_slug}/firm/{firm_id}"
    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            r.raise_for_status()
            r.encoding = "utf-8"
            return r.text
        except Exception:
            if attempt < max_retries - 1:
                time.sleep(2 + attempt * 2)
    return None


def parse_search_results(html: str, city_name: str, city_slug: str) -> list[dict]:
    """HTML парсинг — fallback если API недоступен."""
    soup = BeautifulSoup(html, "html.parser")
    items = []

    # Пробуем вытащить данные из JSON в <script> тегах (Next.js / SSR)
    for script in soup.find_all("script"):
        text = script.string or ""
        if "firm" not in text and "branch" not in text:
            continue
        for m in re.finditer(r'"id"\s*:\s*"(\d{15,})"[^}]*?"name"\s*:\s*"([^"]+)"', text):
            firm_id, name = m.group(1), m.group(2)
            items.append({
                "id_2gis": firm_id, "name": name[:200], "address": "",
                "city": city_name, "lat": "", "lon": "", "phone": "",
                "instagram": "", "facebook": "", "telegram": "",
            })

    # Старый метод — ссылки <a href="/city/firm/id">
    if not items:
        for a in soup.find_all("a", href=re.compile(r"/" + re.escape(city_slug) + r"/firm/(\d+)")):
            href = a.get("href", "")
            match = re.search(r"/firm/(\d+)", href)
            if not match:
                continue
            firm_id = match.group(1)
            name = (a.get_text(strip=True) or f"Организация {firm_id}")[:200]
            items.append({
                "id_2gis": firm_id, "name": name, "address": "",
                "city": city_name, "lat": "", "lon": "", "phone": "",
                "instagram": "", "facebook": "", "telegram": "",
            })

    seen = set()
    return [x for x in items if x["id_2gis"] not in seen and not seen.add(x["id_2gis"])]


def _extract_social_links(html: str) -> dict[str, str]:
    """Извлекает ссылки на Instagram, Facebook, Telegram из HTML карточки."""
    out = {"instagram": "", "facebook": "", "telegram": ""}
    # instagram.com/username или instagram.com/username/
    for m in re.finditer(r"instagram\.com[/\w\.\-]+", html):
        s = m.group(0).replace("instagram.com/", "").strip("/")
        if s and s != "accounts" and len(s) < 80:
            out["instagram"] = "https://www.instagram.com/" + s.split("/")[0]
            break
    for m in re.finditer(r"facebook\.com[/\w\.\-]+", html):
        s = m.group(0).replace("facebook.com/", "").strip("/")
        if s and s != "sharer" and len(s) < 80:
            out["facebook"] = "https://www.facebook.com/" + s.split("/")[0]
            break
    for m in re.finditer(r"t\.me/[\w\.\-]+", html):
        s = m.group(0).replace("t.me/", "").strip()
        if s and len(s) < 80:
            out["telegram"] = "https://t.me/" + s.split("?")[0]
            break
    if not out["telegram"]:
        for m in re.finditer(r"telegram\.(me|dog)/[\w\.\-]+", html):
            out["telegram"] = "https://" + m.group(0).split('"')[0].split("'")[0]
            break
    return out


def _extract_phone(html: str) -> str:
    """Извлекает номер телефона из карточки 2GIS (tel:, +7, 8...)."""
    # tel: ссылки
    for m in re.finditer(r'tel:[\s]*([+\d\s\-\(\)]{10,20})', html):
        s = re.sub(r"[\s\-\(\)]", "", m.group(1))
        if len(s) >= 10 and s.isdigit() or s.startswith("+"):
            return s[:20]
    # +7 или 8 и 10 цифр
    for m in re.finditer(r"(?:\+7|8)[\s\-]?\(?\d{3}\)?[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}", html):
        s = re.sub(r"[\s\-\(\)]", "", m.group(0))
        if len(s) >= 10:
            return s[:20]
    return ""


def parse_firm_page(html: str) -> tuple[str, str, str, str, dict[str, str]]:
    """Возвращает (address, lat, lon, phone, socials_dict)."""
    lat, lon = "", ""
    match = re.search(r"directions/points/[^\"']*?[\|%7C](\d+\.\d+)[,%2C](\d+\.\d+)", html)
    if match:
        lon, lat = match.group(1), match.group(2)
        try:
            lat_f, lon_f = float(lat), float(lon)
            if lat_f > 90 or lon_f > 180 or lat_f < 1 or lon_f < 1:
                lat, lon = "", ""
        except ValueError:
            lat, lon = "", ""
    if not lat or not lon:
        for m in re.finditer(r'"lat":\s*([\d.]+).*?"lon":\s*([\d.]+)', html, re.DOTALL):
            lat, lon = m.group(1), m.group(2)
            break
        if not lat and re.search(r'"lon":\s*([\d.]+).*?"lat":\s*([\d.]+)', html, re.DOTALL):
            m = re.search(r'"lon":\s*([\d.]+).*?"lat":\s*([\d.]+)', html, re.DOTALL)
            if m:
                lon, lat = m.group(1), m.group(2)

    address = ""
    m = re.search(r'[А-Яа-яёЁ0-9\s\-\.]+(?:проспект|ул\.|улица|набережная|шоссе|переулок|бульвар)[^<]+', html)
    if m:
        address = re.sub(r"\s+", " ", m.group(0)).strip()
    if not address:
        m = re.search(r'(\d+)\s*этаж', html)
        if m:
            pos = html.find(m.group(0))
            chunk = html[max(0, pos - 200):pos]
            for part in re.findall(r"[А-Яа-яёЁ0-9\s\-\.]+", chunk):
                if re.search(r"(улица|проспект|набережная|шоссе)", part) and len(part) > 10:
                    address = part.strip()
                    break
    socials = _extract_social_links(html)
    phone = _extract_phone(html)
    return (address, lat, lon, phone, socials)


def normalize_cities(user_cities: list[str], cities_dict: dict | None = None) -> list[tuple[str, str]]:
    """Превращает список названий городов в список (название, slug). cities_dict по умолчанию CITIES_KZ (Казахстан)."""
    ref = cities_dict if cities_dict is not None else CITIES_KZ
    result = []
    for name in user_cities:
        n = (name or "").strip()
        if not n:
            continue
        for city_name, slug in ref.items():
            if city_name.lower() == n.lower():
                result.append((city_name, slug))
                break
        else:
            if n in ref:
                result.append((n, ref[n]))
            else:
                result.append((n, n.lower().replace(" ", "_").replace("-", "_")))
    return result


def _scrape_one_city(
    city_name: str,
    city_slug: str,
    search_queries: list[str],
    max_pages_per_city: int,
    delay: float,
    base_url: str,
    stop_event: object | None,
) -> list[dict]:
    """Собирает результаты по одному городу через API (с fallback на HTML)."""
    def stopped() -> bool:
        return stop_event is not None and getattr(stop_event, "is_set", lambda: False)()

    results = []
    seen_local: set[str] = set()

    for query in search_queries:
        if stopped():
            break
        for page in range(1, max_pages_per_city + 1):
            if stopped():
                break

            # Сначала пробуем API
            api_data = search_via_api(city_slug, query, page)
            time.sleep(delay)

            if api_data is not None:
                chunk = parse_api_results(api_data, city_name)
                # Если API вернул пустой результат — страниц больше нет
                if not chunk:
                    break
                # Проверяем total_count чтобы не листать лишние страницы
                total = api_data.get("result", {}).get("total", 0)
                for item in chunk:
                    if item["id_2gis"] not in seen_local:
                        seen_local.add(item["id_2gis"])
                        results.append(item)
                if total and page * 20 >= total:
                    break
            else:
                # Fallback: HTML парсинг
                html = get_search_page(city_slug, query, page, base_url=base_url)
                time.sleep(delay)
                if not html:
                    break
                chunk = parse_search_results(html, city_name, city_slug)
                if not chunk:
                    break
                for item in chunk:
                    if item["id_2gis"] not in seen_local:
                        seen_local.add(item["id_2gis"])
                        results.append(item)

    return results


def _fetch_firm_data(item: dict, city_slug: str, base_url: str, delay: float) -> None:
    """Заполняет координаты, телефон и соц. ссылки для одной карточки (для пула потоков)."""
    html = get_firm_page(city_slug, item["id_2gis"], base_url=base_url)
    time.sleep(delay)
    if not html:
        return
    address, lat, lon, phone, socials = parse_firm_page(html)
    item["lat"] = lat
    item["lon"] = lon
    item["phone"] = phone
    item["instagram"] = socials.get("instagram", "")
    item["facebook"] = socials.get("facebook", "")
    item["telegram"] = socials.get("telegram", "")
    if not item.get("address") and address:
        item["address"] = address
    if item.get("address"):
        item["address"] = item["address"].replace(" — 2ГИС", "").strip()


def run_parsing(
    search_queries: list[str],
    cities: list[tuple[str, str]],
    max_pages_per_city: int = 10,
    delay: float = 1.0,
    fetch_coordinates: bool = True,
    progress_callback: Callable[[str], None] | None = None,
    stop_event: object | None = None,
    base_url: str = BASE_2GIS_KZ,
    max_workers: int = 4,
    filter_noise: bool = True,
) -> list[dict]:
    """
    Запускает парсинг — последовательный обход городов по порядку.
    Города обходятся один за другим (Астана → Алматы → Шымкент → ...).
    Каждый город логируется в parse_log.jsonl с временной меткой.
    filter_noise: фильтровать нецелевые организации по стоп-словам.
    """
    def log(msg: str) -> None:
        if progress_callback:
            progress_callback(msg)

    def stopped() -> bool:
        return stop_event is not None and getattr(stop_event, "is_set", lambda: False)()

    workers = max(1, min(16, max_workers))
    seen_ids: set[str] = set()
    all_rows: list[dict] = []

    _write_log_entry({"event": "start", "cities": len(cities), "queries": len(search_queries)})
    log(f"Сбор по {len(cities)} городам (последовательно, по порядку)…")

    for city_idx, (city_name, city_slug) in enumerate(cities, 1):
        if stopped():
            break
        log(f"[{city_idx}/{len(cities)}] {city_name}…")
        items = _scrape_one_city(city_name, city_slug, search_queries, max_pages_per_city, delay, base_url, stop_event)

        new_items = []
        for item in items:
            if item["id_2gis"] in seen_ids:
                continue
            if filter_noise and not is_flower_shop(item["name"]):
                continue
            seen_ids.add(item["id_2gis"])
            all_rows.append(item)
            new_items.append(item)

        log(f"  {city_name}: найдено {len(items)}, добавлено {len(new_items)}, всего {len(all_rows)}")
        _write_log_entry({
            "event": "city_done",
            "city": city_name,
            "found": len(items),
            "added": len(new_items),
            "total": len(all_rows),
        })

    if stopped():
        log("Остановлено пользователем.")
    if not all_rows:
        _write_log_entry({"event": "finish", "total": 0})
        return all_rows

    if fetch_coordinates:
        log(f"Загрузка карточек ({len(all_rows)} шт.) в {workers} потоков…")
        city_slug_by_city = {c[0]: c[1] for c in cities}
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures_map = {}
            for item in all_rows:
                if stopped():
                    break
                slug = city_slug_by_city.get(item["city"], cities[0][1] if cities else "")
                futures_map[ex.submit(_fetch_firm_data, item, slug, base_url, delay)] = item
            for fut in as_completed(futures_map):
                pass
        log("    Карточки загружены.")

    for r in all_rows:
        r["coordinates"] = f"{r['lat']},{r['lon']}" if r.get("lat") and r.get("lon") else ""

    _write_log_entry({"event": "finish", "total": len(all_rows)})
    return all_rows


def save_csv(rows: list[dict], path: str | Path) -> None:
    """Норматив: id_2gis, name, address, city, lat, lon, coordinates, phone, instagram, facebook, telegram."""
    fieldnames = ["id_2gis", "name", "address", "city", "lat", "lon", "coordinates", "phone", "instagram", "facebook", "telegram"]
    for r in rows:
        r["coordinates"] = f"{r.get('lat') or ''},{r.get('lon') or ''}".strip(",")
        r.setdefault("phone", "")
        r.setdefault("instagram", "")
        r.setdefault("facebook", "")
        r.setdefault("telegram", "")
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";", extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
