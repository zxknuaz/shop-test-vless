"""
Конфигурация для pytest тестов.
Обеспечивает правильные пути импорта и общие фикстуры.
"""
import sys
from pathlib import Path

# Добавляем src в путь для импорта модулей
project_root = Path(__file__).parent.parent
src_path = project_root / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))
