"""Запуск SQL-скриптов слоёв хранилища: sql/<слой>/<файл>.

Скрипт читается, из него выбрасываются комментарии, остаток режется по «;»
и выполняется по частям. Все скрипты идемпотентны: повторный запуск не создаёт дублей.
"""

import os

from clickhouse import client

PROJECT_DIR = os.environ.get("PROJECT_DIR", "/opt/project")
SQL_DIR = os.path.join(PROJECT_DIR, "sql")


def statements(layer, name):
    with open(os.path.join(SQL_DIR, layer, name), encoding="utf-8") as f:
        lines = [line for line in f if not line.lstrip().startswith("--")]
    return [s.strip() for s in "".join(lines).split(";") if s.strip()]


def count(ch, table):
    return ch.query(f"select count() from {table}").result_rows[0][0]


def has_rows(table):
    """Для сенсора: таблица существует и в ней есть данные."""
    try:
        return count(client(), table) > 0
    except Exception as e:
        print(f"{table} пока недоступна: {type(e).__name__}")
        return False


def run_script(layer, name, table=None):
    """Выполняет скрипт; если указана таблица, возвращает число строк в ней после запуска."""
    ch = client()
    before = count(ch, table) if table else 0
    for statement in statements(layer, name):
        ch.command(statement)
    rows = count(ch, table) if table else 0
    print(f"{layer}/{name}: " + (f"{table} {rows} строк (было {before})" if table else "выполнен"))
    return rows
