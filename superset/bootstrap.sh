#!/bin/bash
# Первый запуск создаёт схему метаданных и администратора, последующие просто стартуют сервер.
set -e

superset db upgrade
superset fab create-admin \
    --username admin --password admin \
    --firstname Admin --lastname Superset --email admin@example.com || true
superset init

exec /usr/bin/run-server.sh
