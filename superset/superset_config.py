"""Конфигурация Superset. Образ подхватывает этот файл из /app/pythonpath сам."""

import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = "postgresql+psycopg2://superset:superset@superset-db:5432/superset"

# учебный стенд: один пользователь, кеш и очередь задач не нужны
SQLLAB_CTAS_NO_LIMIT = True
WTF_CSRF_ENABLED = False
TALISMAN_ENABLED = False
