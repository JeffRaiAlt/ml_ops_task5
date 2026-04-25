import os
import json
import datetime
import pandas as pd
import psycopg
from dotenv import load_dotenv
from feast.driver_test_data import create_driver_hourly_stats_df

# Загружаем переменные из .env
load_dotenv()


def convert_numpy_types(value):
    if hasattr(value, 'item'):
        return value.item()
    return value


def get_conn_string():
    user = os.getenv("DB_USER")
    pw = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    db = os.getenv("DB_NAME")
    # Формируем строку для psycopg (v3)
    return f"postgresql://{user}:{pw}@{host}:{port}/{db}"


def insert_100_rows():
    # 1. Генерация данных
    now = datetime.datetime.now().replace(microsecond=0, second=0,
                                          minute=0)
    start_date = now - datetime.timedelta(days=1)
    end_date = now + datetime.timedelta(days=15)

    driver_entities = [1001, 1002, 1003, 1004, 1005]
    driver_df = create_driver_hourly_stats_df(driver_entities, start_date,
                                              end_date)

    print(
        f"Данные сгенерированы. Подключаемся к {os.getenv('DB_HOST')}...")

    # 2. Подключение и работа с БД
    conninfo = get_conn_string()

    try:
        with psycopg.connect(conninfo=conninfo, sslmode="disable") as conn:
            with conn.cursor() as cursor:
                # Настройка search_path через сессию
                cursor.execute("SET search_path TO public")

                print("Пересоздаем таблицу...")
                cursor.execute(
                    "DROP TABLE IF EXISTS feast_driver_hourly_stats")

                create_table_sql = """CREATE TABLE feast_driver_hourly_stats (
                    index SERIAL PRIMARY KEY,
                    event_timestamp TIMESTAMP NOT NULL,
                    driver_id INTEGER NOT NULL,
                    conv_rate DOUBLE PRECISION,
                    acc_rate DOUBLE PRECISION,
                    avg_daily_trips INTEGER,
                    created TIMESTAMP NOT NULL,
                    driver_metadata TEXT, 
                    driver_config TEXT,   
                    driver_profile TEXT   
                )"""
                cursor.execute(create_table_sql)

                # 3. Подготовка данных к вставке
                insert_sql = """INSERT INTO feast_driver_hourly_stats
                    (event_timestamp, driver_id, conv_rate, acc_rate, avg_daily_trips, created,
                     driver_metadata, driver_config, driver_profile)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""

                insert_data = []
                for i in range(min(100, len(driver_df))):
                    row = driver_df.iloc[i]

                    # Обработка JSON колонок
                    meta = json.dumps(
                        row['driver_metadata']) if isinstance(
                        row['driver_metadata'], dict) else str(
                        row['driver_metadata'])
                    conf = json.dumps(row['driver_config']) if isinstance(
                        row['driver_config'], dict) else str(
                        row['driver_config'])
                    prof = json.dumps(row['driver_profile']) if isinstance(
                        row['driver_profile'], dict) else str(
                        row['driver_profile'])

                    insert_data.append((
                        convert_numpy_types(row['event_timestamp']),
                        int(row['driver_id']),
                        float(row['conv_rate']),
                        float(row['acc_rate']),
                        int(row['avg_daily_trips']),
                        convert_numpy_types(row['created']),
                        meta, conf, prof
                    ))

                cursor.executemany(insert_sql, insert_data)
                conn.commit()
                print(f"Успешно загружено строк: {len(insert_data)}")

    except Exception as e:
        print(f"Ошибка при работе с БД: {e}")


if __name__ == "__main__":
    insert_100_rows()