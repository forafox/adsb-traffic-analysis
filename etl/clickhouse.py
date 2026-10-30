"""Подключение к ClickHouse: адрес и учётные данные в одном месте."""

import os

HOST = os.environ["ODS_CLICKHOUSE_HOST"]


def client():
    import clickhouse_connect

    return clickhouse_connect.get_client(host=HOST, username="ods", password="ods",
                                         database="ods")
