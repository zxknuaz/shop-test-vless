# Тесты для shop_bot

## Описание

Тесты проверяют функции, в которых была отключена проверка SSL сертификатов. Тесты покрывают следующие функции:

1. **speedtest_runner.py**:
   - `net_probe_for_host()` - проверка что SSL отключен при HTTP запросах

2. **handlers.py**:
   - `get_usdt_rub_rate()` - получение курса USDT/RUB
   - `get_ton_usdt_rate()` - получение курса TON/USD
   - `_create_heleket_payment_request()` - создание платежа через Heleket
   - `_yoomoney_find_payment()` - поиск платежа в YooMoney

## Установка зависимостей

```bash
# Установка всех зависимостей включая dev
pip install -e ".[dev]"

# Или установка только тестовых зависимостей
pip install pytest pytest-asyncio pytest-mock
```

## Запуск тестов

```bash
# Запуск всех тестов
pytest

# Запуск тестов с подробным выводом
pytest -v

# Запуск конкретного файла тестов
pytest tests/test_speedtest_runner.py
pytest tests/test_handlers_ssl.py

# Запуск конкретного теста
pytest tests/test_handlers_ssl.py::test_get_usdt_rub_rate_ssl_disabled

# Запуск с покрытием кода (требует pytest-cov)
pytest --cov=shop_bot --cov-report=html
```

## Что проверяют тесты

Все тесты проверяют два основных аспекта:

1. **Отключение SSL**: Проверяется, что `TCPConnector` создается с параметром `ssl=False`
2. **Корректность работы функций**: Проверяется, что функции возвращают ожидаемые результаты при успешных и ошибочных сценариях

## Структура тестов

- `test_speedtest_runner.py` - тесты для функций из `speedtest_runner.py`
- `test_handlers_ssl.py` - тесты для функций из `handlers.py`

Все тесты используют моки для:
- HTTP запросов через `aiohttp.ClientSession`
- TCP соединений
- Функций получения настроек из базы данных
- Других внешних зависимостей
