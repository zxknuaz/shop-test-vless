# Как запустить тесты

## Проблема с путями в Windows PowerShell

Если путь к проекту содержит кириллические символы, PowerShell может иметь проблемы с кодировкой. 

## Решение

### Вариант 1: Использовать командную строку (cmd)

```cmd
cd "путь_к_проекту\3xui-shopbot-main"
python -m pytest tests\ -v
```

### Вариант 2: Установить зависимости и запустить тесты через Python

```powershell
# Перейти в директорию проекта (если возможно)
# Затем:

# Установить зависимости
python -m pip install -r requirements.txt
# или
python -m pip install aiohttp paramiko qrcode pytest pytest-asyncio pytest-mock

# Запустить тесты
python -m pytest tests\ -v
```

### Вариант 3: Использовать полный путь к pytest.ini

```powershell
$dir = (Get-ChildItem -Path "C:\Users\$env:USERNAME\Downloads" -Filter "pytest.ini" -Recurse -ErrorAction SilentlyContinue | Select-Object -First 1).Directory.FullName
Set-Location -LiteralPath $dir
python -m pytest tests\ -v
```

## Установка зависимостей проекта

Перед запуском тестов нужно установить зависимости:

```bash
# Из корня проекта
pip install -e ".[dev]"

# Или вручную установить основные зависимости для тестов:
pip install aiohttp paramiko qrcode pytest pytest-asyncio pytest-mock
```

## Что тестируется

1. **test_speedtest_runner.py** - тесты функции `net_probe_for_host()`:
   - Проверка отключения SSL при HTTP HEAD запросах
   - Проверка отключения SSL при fallback на GET запросы
   - Обработка невалидных URL
   - Обработка ошибок TCP соединения

2. **test_handlers_ssl.py** - тесты функций из `handlers.py`:
   - `get_usdt_rub_rate()` - проверка SSL и обработка ошибок
   - `get_ton_usdt_rate()` - проверка SSL
   - `_create_heleket_payment_request()` - проверка SSL
   - `_yoomoney_find_payment()` - проверка SSL и обработка ошибок

## Ожидаемый результат

После успешной установки зависимостей тесты должны выполниться и показать:
- Все тесты пройдены (passed)
- Проверка что `TCPConnector` создаётся с `ssl=False` во всех HTTP запросах
