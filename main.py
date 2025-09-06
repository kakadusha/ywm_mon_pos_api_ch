"""
Скрипт для сбора данных из API Яндекс.Вебмастера и записи их в базу данных.
Переработан для работы с ClickHouse.
MVP
"""

# from multiprocessing import connection
import os
import requests
import time
import logging

# import pymysql
# import pandas as pd
from dotenv import load_dotenv

from clickhouse_driver import Client

CONNECTION_ID = "clickhouse01"
CH_PARAMS = {}  # global
GOAL_DATE = "2025-08-29"  # расчитать для дага
DB_SAVE_THRESHOLD = 100000


def get_clickhouse_client(connection_id):
    try:
        result = Client(
            host=CH_PARAMS["host"],
            port=CH_PARAMS["port"],
            user=CH_PARAMS["user"],
            password=CH_PARAMS["password"],
            send_receive_timeout=3600,
        )

        message = f"Connection {connection_id} established"
        logging.info(message)
        return result

    except Exception as e:
        message = "Connection not established"
        logging.error(message, str(e))
        # send_dq_result(text=f"{message} - {str(e)}")
        raise


def execute_sql(
    sqls: list[str], connection_id: str, db: str, table: str, loggin_is_on: bool = True
) -> str:
    try:
        with get_clickhouse_client(connection_id) as connection:
            result = []
            sqls = [sqls] if not isinstance(sqls, list) else sqls

            for sql in sqls:
                if loggin_is_on:
                    logging.info(sql)
                result += connection.execute(sql)
                if loggin_is_on:
                    logging.info(result)
                    logging.info("EXECUTED SUCCESSFULLY!")

            return result
    except Exception as e:
        ERROR_TEXT = f"AIRFLOW ERROR: {db}.{table} - {str(e)}"

        logging.error(f"AIRFLOW ERROR: {ERROR_TEXT}", str(e))
        # send_dq_result(text=f"AIRFLOW ERROR: {ERROR_TEXT}")
        raise


def create_raw_table_in_clickhouse_if_needed():
    """Создание таблицы в ClickHouse, если она не существует."""
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {CH_DB}.{CH_TABLE} (
        url String,
        query String,
        date Date,
        demand UInt64,
        impressions UInt64,
        clicks UInt64,
        ctr Decimal64(3),
        position Decimal64(3)
    ) ENGINE = MergeTree()
    PARTITION BY toYYYYMMDD(date)
    ORDER BY (date, query, url)
    PRIMARY KEY (date, query, url);
    """
    execute_sql([create_table_query], CONNECTION_ID, CH_DB, f"{CH_TABLE}_agg")


#     """Вставка данных в таблицу MariaDB. Данные агрегируются по URL, QUERY, DATE перед вставкой."""
#     # Agg
#     df = pd.DataFrame(data)
#     df = df.groupby(["URL", "QUERY", "DATE"], as_index=False).sum()
#     df.loc[df["IMPRESSIONS"] == 0.0, ["POSITION"]] = None
#     df = df[df["DEMAND"] != 0.0]
#     df["DATE"] = pd.to_datetime(df["DATE"]).dt.strftime("%d.%m.%Y")


def drop_partition_yyyymmdd_clickhouse(partition_date):
    """Удаление партиции из таблицы ClickHouse по дате в формате YYYYMMDD."""
    drop_query = f"ALTER TABLE {CH_DB}.{CH_TABLE} DROP PARTITION {partition_date};"
    try:
        execute_sql(drop_query, CONNECTION_ID, CH_DB, CH_TABLE)
        logging.info(f"Partition {partition_date} dropped successfully.")
    except Exception as err:
        logging.error(
            f"{time.ctime()} - Error dropping partition {partition_date}: {err}."
        )
        raise


def insert_data_to_clickhouse(data):
    """Вставка данных в таблицу ClickHouse. Данные НЕ агрегируются перед вставкой.
    Вставка происходит одним мультистрочным INSERT-запросом.
    Пример данных: {'URL': '/proizvodstvennyj_kalendar/2025/', 'QUERY': 'календарь 2025', 'DATE': '2025-08-12', 'DEMAND': 0.0,
                    'IMPRESSIONS': 0.0, 'CLICKS': 0.0, 'CTR': 0.2, 'POSITION': 0.0}
    """

    attempts = 0
    insert_query_head = f"""
    INSERT INTO {CH_DB}.{CH_TABLE} (url, query, date, demand, impressions, clicks, ctr, position)
    VALUES
    """
    insert_query = (
        insert_query_head
        + ",\n".join(
            [
                (
                    "("
                    + "'"
                    + row["URL"].replace("\\", "\\\\").replace("'", "\\'")
                    + "','"
                    + row["QUERY"].replace("\\", "\\\\").replace("'", "\\'")
                    + "','"
                    + row["DATE"]
                    + "',"
                    f"{row['DEMAND']},{row['IMPRESSIONS']},{row['CLICKS']},{row['CTR']},{row['POSITION']}"
                    + ")"
                )
                for row in data  # [1:100]
            ]
        )
        + ";"
    )

    while attempts < MAX_ATTEMPTS_DB:
        try:
            execute_sql(
                insert_query,
                CONNECTION_ID,
                CH_DB,
                CH_TABLE,
                loggin_is_on=False,
            )
            break
        except Exception as err:
            logging.error(
                f"{time.ctime()} - Ответ базы данных: {err}. Повторная попытка через {SLEEP_TIME_DB_ERR} секунд..."
            )
            time.sleep(SLEEP_TIME_DB_ERR)
            attempts += 1


load_dotenv()

API_URL = os.getenv("API_URL")
HEADERS = {"Authorization": os.getenv("API_HEADERS")}
CH_DB = os.getenv("CH_DB", "sandbox")
CH_TABLE = os.getenv("CH_TABLE", "yandex_webmaster_date")
CH_PARAMS = {
    "host": os.getenv("CH_HOST"),
    "user": os.getenv("CH_USER"),
    "port": 9005,
    "password": os.getenv("CH_PASSWORD"),
}

# placed in config
GEO = 1  # 225, 1, 10174, 11079
DEVICE = "ALL"  # ALL, DESKTOP, MOBILE_AND_TABLET, MOBILE, TABLET
SLEEP_TIME_API = 20
MAX_ATTEMPTS_DB = 15
MAX_ATTEMPTS_API = 15
SLEEP_TIME_DB_ERR = 5


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.info(f"{time.ctime()} - Старт работы скрипта.")
start_time = time.time()


# connection_maria = pymysql.connect(**DB_PARAMS)
# create_table_in_maria(connection_maria)
create_raw_table_in_clickhouse_if_needed()
drop_partition_yyyymmdd_clickhouse(GOAL_DATE.replace("-", ""))


def api_request(url, headers, body):
    with requests.Session() as session:
        attempts = 0
        while attempts < MAX_ATTEMPTS_API:
            try:
                response = session.post(url, headers=headers, json=body)
                response.raise_for_status()
                return response.json()
            except requests.HTTPError as http_err:
                logging.error(
                    f"{time.ctime()} - Ответ API: {http_err}. Повторная попытка через {SLEEP_TIME_DB_ERR} секунд..."
                )
                time.sleep(SLEEP_TIME_DB_ERR)
                attempts += 1
        logging.error(
            f"{time.ctime()} - Превышено количество попыток, завершение скрипта."
        )


urls = []
total_count = None
offset = 0
while True:
    if total_count is None or total_count > 0:
        BODY_URL = {
            "offset": offset,
            "limit": 500,
            "device_type_indicator": DEVICE,
            "text_indicator": "URL",
            "region_ids": [GEO],
            "filters": {
                "text_filters": [
                    {"text_indicator": "URL", "operation": "TEXT_CONTAINS", "value": ""}
                ]
            },
        }
        response = api_request(API_URL, HEADERS, BODY_URL)
        if total_count is None:
            total_count = response["count"]
            logging.info(f"{time.ctime()} - Найдено блоков total_count: {total_count}")

        current_batch = response["text_indicator_to_statistics"]
        for item in current_batch:
            urls.append(item["text_indicator"]["value"])

        total_count -= len(current_batch)
        logging.info(
            f"{time.ctime()} - Осталось запросить блоков total_count: {total_count}"
        )
        if total_count <= 0:
            logging.info(
                f"{time.ctime()} - Составлен список urls из {len(urls)} элементов"
            )
            break

        offset += 500
        # time.sleep(SLEEP_TIME_API)
    else:
        break

logging.info(f"{time.ctime()} - Начата сборка информации о запросах")
data_buff = []
down_count = len(urls)
for url_value in urls:
    total_count = None
    offset = 0
    down_count -= 1
    logging.info(f"{time.ctime()} - Текущая очередь: {down_count}")
    # time.sleep(SLEEP_TIME_API)

    data = []
    while True:
        if total_count is None or total_count > 0:
            body_query = {
                "offset": offset,
                "limit": 500,
                "device_type_indicator": DEVICE,
                "text_indicator": "QUERY",
                "region_ids": [GEO],
                "filters": {
                    "text_filters": [
                        {
                            "text_indicator": "URL",
                            "operation": "TEXT_MATCH",
                            "value": url_value,
                        }
                    ]
                },
            }
            response = api_request(API_URL, HEADERS, body_query)
            if total_count is None:
                total_count = response["count"]

            current_batch = response["text_indicator_to_statistics"]

            if url_value != None:
                # Collect data
                for text_indicator_to_statistics in response[
                    "text_indicator_to_statistics"
                ]:
                    query_value = text_indicator_to_statistics["text_indicator"][
                        "value"
                    ]
                    for stat in text_indicator_to_statistics["statistics"]:
                        date = stat["date"]  # здесь можно пропустить по дате
                        if date != GOAL_DATE:
                            continue
                        field = stat["field"]
                        value = stat["value"]
                        row = {
                            "URL": url_value,
                            "QUERY": query_value,
                            "DATE": date,
                            "DEMAND": 0,
                            "IMPRESSIONS": 0,
                            "CLICKS": 0,
                            "CTR": 0.0,
                            "POSITION": 0.0,
                        }
                        row[field] = value
                        data.append(row)

                # Write to DB
                # insert_data_to_maria_aggrigated(connection_maria, data)
            total_count -= len(current_batch)
            if total_count <= 0:
                break
            offset += 500
            # time.sleep(SLEEP_TIME_API)
        else:
            break

    if len(data) > DB_SAVE_THRESHOLD:
        # собрали очень много
        logging.info(f"{time.ctime()} - вставка {len(data)} строк.")
        insert_data_to_clickhouse(data)
    else:
        # подкопим в буфере
        data_buff = data_buff + data
        if len(data_buff) > DB_SAVE_THRESHOLD:
            logging.info(f"{time.ctime()} - вставка {len(data_buff)} строк.")
            insert_data_to_clickhouse(data_buff)
            data_buff = []
        else:
            logging.info(f"{time.ctime()} - в буфере только {len(data_buff)} строк.")

# записываем остатки
if len(data_buff) > 0:
    logging.info(f"{time.ctime()} - вставка {len(data_buff)} строк.")
    insert_data_to_clickhouse(data_buff)

# connection_maria.close()
end_time = time.time()
logging.info(
    f"{time.ctime()} - Завершение работы скрипта. Выполнен за {end_time - start_time} секунд."
)
