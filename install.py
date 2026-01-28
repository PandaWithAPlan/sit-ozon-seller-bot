#!/usr/bin/env python3
"""Скрипт установки Ozon Seller Bot."""

import os
import sys
import subprocess
from pathlib import Path


def run_command(cmd: str, description: str) -> bool:
    """Выполняет команду и возвращает True, если успешно."""
    print(f"\n{description}...")
    try:
        result = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
        print(f"✅ {description} выполнено успешно")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Ошибка при выполнении: {e}")
        if e.stdout:
            print(f"STDOUT:\n{e.stdout}")
        if e.stderr:
            print(f"STDERR:\n{e.stderr}")
        return False


def check_python_version():
    """Проверяет версию Python."""
    print("\n📋 Проверка версии Python...")
    version = sys.version_info
    if version.major == 3 and version.minor >= 11:
        print(f"✅ Python {version.major}.{version.minor}.{version.micro} - OK")
        return True
    else:
        print(f"❌ Требуется Python 3.11+, установлен: {version.major}.{version.minor}.{version.micro}")
        return False


def install_dependencies():
    """Устанавливает зависимости."""
    return run_command(
        "pip install -r requirements.txt",
        "Установка зависимостей"
    )


def create_directories():
    """Создаёт необходимые директории."""
    print("\n📁 Создание директорий...")
    dirs = ["data", "cache", "reports"]
    for dir_name in dirs:
        Path(dir_name).mkdir(exist_ok=True)
        print(f"✅ Создана директория: {dir_name}")
    return True


def setup_env():
    """Настраивает переменные окружения."""
    print("\n⚙️ Настройка переменных окружения...")
    
    if not Path(".env").exists():
        if Path(".env.example").exists():
            run_command(
                "copy .env.example .env",
                "Создание .env из .env.example"
            )
            print("⚠️ Пожалуйста, отредактируйте .env и добавьте свои API ключи")
        else:
            print("❌ .env.example не найден!")
            return False
    else:
        print("✅ .env уже существует")
    
    return True


def run_tests():
    """Запускает тесты."""
    return run_command(
        "pytest tests/ -v",
        "Запуск тестов"
    )


def main():
    """Главная функция."""
    print("=" * 80)
    print("🚀 Установка Ozon Seller Bot v1.0")
    print("=" * 80)
    
    # Проверка версии Python
    if not check_python_version():
        print("\n❌ Установка прервана: неверная версия Python")
        sys.exit(1)
    
    # Установка зависимостей
    if not install_dependencies():
        print("\n❌ Установка прервана: не удалось установить зависимости")
        sys.exit(1)
    
    # Создание директорий
    create_directories()
    
    # Настройка окружения
    if not setup_env():
        print("\n❌ Установка прервана: не удалось настроить окружение")
        sys.exit(1)
    
    # Запуск тестов (опционально)
    print("\n❓ Запустить тесты? (y/n)")
    answer = input().strip().lower()
    if answer == 'y':
        run_tests()
    
    print("\n" + "=" * 80)
    print("✅ Установка завершена успешно!")
    print("=" * 80)
    print("\n📝 Следующие шаги:")
    print("1. Отредактируйте .env и добавьте свои API ключи")
    print("2. Запустите бота: python bot.py")
    print("\n📚 Документация:")
    print("- README.md - Полная документация")
    print("- RELEASE_NOTES.md - Примечания к релизу")
    print("- DEVELOPER_GUIDE.md - Руководство для разработчиков")
    print("\n")


if __name__ == "__main__":
    main()