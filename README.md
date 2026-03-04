# 2GIS KZ Flower Scraper → Google Sheets (Railway)

Парсит все цветочные магазины Казахстана с 2GIS и пишет данные прямо в Google Sheets.

## Структура файлов

```
railway_data/
├── scraper.py          # Главный скрипт (запускается на Railway)
├── parser_2gis.py      # Модуль парсинга 2GIS
├── google_sheets.py    # Модуль записи в Google Sheets
├── requirements.txt    # Зависимости
├── Procfile            # Команда запуска для Railway
├── railway.toml        # Конфигурация Railway
└── README.md           # Эта инструкция
```

---

## Шаг 1 — Подготовить Google Service Account

1. Открой [Google Cloud Console](https://console.cloud.google.com/)
2. Создай проект (или выбери существующий)
3. Включи **Google Sheets API**: APIs & Services → Enable APIs → Google Sheets API
4. Создай Service Account: APIs & Services → Credentials → Create Credentials → Service Account
5. Скачай JSON-ключ: открой созданный аккаунт → Keys → Add Key → JSON
6. Скопируй email сервисного аккаунта (вида `xxx@yyy.iam.gserviceaccount.com`)

---

## Шаг 2 — Дать доступ к таблице

1. Открой таблицу **2gispars**: https://docs.google.com/spreadsheets/d/1sY3aWslCs5f3jxypf8CA-_qEP4YAD08vpXnjyUelLZc
2. Нажми **Поделиться** → вставь email сервисного аккаунта → роль **Редактор** → Готово

---

## Шаг 3 — Загрузить на GitHub

```bash
cd railway_data
git init
git add .
git commit -m "2gis kz scraper for railway"
# Создай репозиторий на github.com и:
git remote add origin https://github.com/ВАШ_ЛОГИН/2gis-kz-scraper.git
git push -u origin main
```

---

## Шаг 4 — Задеплоить на Railway

1. Зайди на [railway.app](https://railway.app) → New Project → Deploy from GitHub repo
2. Выбери репозиторий `2gis-kz-scraper`
3. Railway автоматически определит `Procfile` и `requirements.txt`

---

## Шаг 5 — Добавить переменные окружения в Railway

В Railway → твой проект → Variables добавь:

| Переменная | Значение |
|---|---|
| `GOOGLE_CREDENTIALS_JSON` | Содержимое скачанного JSON-файла (всё одной строкой) |
| `SPREADSHEET_ID` | `1sY3aWslCs5f3jxypf8CA-_qEP4YAD08vpXnjyUelLZc` |
| `SHEET_NAME` | `Sheet1` |
| `MAX_WORKERS` | `4` |
| `DELAY` | `0.8` |
| `MAX_PAGES` | `50` |
| `BATCH_SIZE` | `50` |

> **GOOGLE_CREDENTIALS_JSON**: открой JSON-файл, выдели всё содержимое (Ctrl+A), скопируй и вставь как одну строку в поле значения.

---

## Шаг 6 — Запустить

- Railway запустит `python scraper.py` автоматически после деплоя
- Логи смотри в Railway → Deployments → View Logs
- Данные появятся в таблице по мере парсинга (батчами по 50 строк)

---

## Столбцы в таблице

| Столбец | Описание |
|---|---|
| id_2gis | ID организации на 2GIS |
| name | Название |
| address | Адрес |
| city | Город |
| lat | Широта |
| lon | Долгота |
| coordinates | lat,lon |
| phone | Телефон |
| instagram | Ссылка Instagram |
| facebook | Ссылка Facebook |
| telegram | Ссылка Telegram |

---

## Повторный запуск

Скрипт автоматически читает уже существующие `id_2gis` из таблицы и **не дублирует** записи.
Просто запусти снова — продолжит с новыми данными.
