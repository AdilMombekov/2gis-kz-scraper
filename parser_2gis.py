# -*- coding: utf-8 -*-
"""
Модуль парсинга 2GIS и др. Платформа, запрос по названию, мультигород, многопоточность.
Регион по умолчанию: Казахстан (2gis.kz).
"""

import re
import time
import csv
import urllib.parse
from pathlib import Path
from typing import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
}

# Казахстан: 2gis.kz — город -> slug в URL (все доступные на 2gis.kz)
BASE_2GIS_KZ = "https://2gis.kz"
CITIES_KZ = {
    "Алматы": "almaty",
    "Астана": "astana",
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


def get_search_page(city_slug: str, query: str, page: int, base_url: str = BASE_2GIS_KZ, timeout: int = 20, max_retries: int = 3) -> str | None:
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
    soup = BeautifulSoup(html, "html.parser")
    items = []
    for a in soup.find_all("a", href=re.compile(r"/" + re.escape(city_slug) + r"/firm/(\d+)")):
        href = a.get("href", "")
        match = re.search(r"/firm/(\d+)", href)
        if not match:
            continue
        firm_id = match.group(1)
        name = (a.get_text(strip=True) or "").strip()
        if len(name) > 200:
            name = name[:200]
        if not name:
            name = f"Организация {firm_id}"

        address = ""
        parent = a.find_parent(["div", "article", "li", "section"])
        if parent:
            for elem in parent.find_all(string=True):
                t = (elem.strip() if isinstance(elem, str) else "")
                if t and re.search(r"(проспект|улица|набережная|шоссе|переулок|бульвар|пр\.|ул\.|,\s*[А-Яа-яёЁ\s\-]+)$", t):
                    if "филиал" not in t.lower() and "оценок" not in t and "рейтинг" not in t.lower():
                        address = t.split("​")[-1].strip() if "​" in t else t
                        break
            if not address:
                for p in parent.find_all("p") or parent.find_all("span"):
                    t = p.get_text(strip=True)
                    if city_name in t and "," in t and 10 < len(t) < 200:
                        address = t
                        break

        address_clean = (address or "").replace(" — 2ГИС", "").strip()
        items.append({
            "id_2gis": firm_id,
            "name": name,
            "address": address_clean,
            "city": city_name,
            "lat": "",
            "lon": "",
            "phone": "",
            "instagram": "",
            "facebook": "",
            "telegram": "",
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
    """Собирает результаты по одному городу (для многопоточного запуска)."""
    def stopped() -> bool:
        return stop_event is not None and getattr(stop_event, "is_set", lambda: False)()

    results = []
    for query in search_queries:
        if stopped():
            break
        for page in range(1, max_pages_per_city + 1):
            if stopped():
                break
            html = get_search_page(city_slug, query, page, base_url=base_url)
            time.sleep(delay)
            if not html:
                continue
            chunk = parse_search_results(html, city_name, city_slug)
            for item in chunk:
                results.append(item)
            if not chunk:
                break
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
) -> list[dict]:
    """
    Запускает парсинг (мультигород, многопоточный).
    base_url: для Казахстана BASE_2GIS_KZ.
    max_workers: число потоков для городов и для загрузки карточек.
    """
    def log(msg: str) -> None:
        if progress_callback:
            progress_callback(msg)

    def stopped() -> bool:
        return stop_event is not None and getattr(stop_event, "is_set", lambda: False)()

    workers = max(1, min(16, max_workers))
    seen_ids: set[str] = set()
    lock = threading.Lock()
    all_rows: list[dict] = []

    # Фаза 1: сбор с поиска по городам (параллельно)
    log(f"Сбор по {len(cities)} городам в {workers} потоков…")
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                _scrape_one_city,
                city_name,
                city_slug,
                search_queries,
                max_pages_per_city,
                delay,
                base_url,
                stop_event,
            ): (city_name, city_slug)
            for city_name, city_slug in cities
        }
        for fut in as_completed(futures):
            if stopped():
                break
            city_name, city_slug = futures[fut]
            try:
                items = fut.result()
                with lock:
                    for item in items:
                        if item["id_2gis"] not in seen_ids:
                            seen_ids.add(item["id_2gis"])
                            all_rows.append(item)
                log(f"  {city_name}: +{len(items)}, всего {len(all_rows)}")
            except Exception as e:
                log(f"  {city_name}: ошибка {e}")

    if stopped():
        log("Остановлено пользователем.")
    if not all_rows:
        return all_rows

    # Фаза 2: загрузка карточек (координаты + соцсети) — пул потоков
    if fetch_coordinates:
        log(f"Загрузка карточек ({len(all_rows)} шт.) в {workers} потоков…")
        city_slug_by_city = {c[0]: c[1] for c in cities}
        with ThreadPoolExecutor(max_workers=workers) as ex:
            for item in all_rows:
                if stopped():
                    break
                slug = city_slug_by_city.get(item["city"], cities[0][1] if cities else "")
                ex.submit(_fetch_firm_data, item, slug, base_url, delay)
        log("    Карточки загружены.")

    for r in all_rows:
        r["coordinates"] = f"{r['lat']},{r['lon']}" if r.get("lat") and r.get("lon") else ""
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
