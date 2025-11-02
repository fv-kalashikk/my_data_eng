import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import pandas as pd


def save_to_parquet(processed_data, filename="disease_risk_processed.parquet"):
    """
    Сохранение обработанных данных в parquet файл
    """
    try:
        os.makedirs("data/processed", exist_ok=True)
        filepath = f"data/processed/{filename}"
        processed_data.to_parquet(filepath, index=False)
        print(f"Данные сохранены в parquet: {filepath}")
        return True
    except Exception as e:
        print(f"Ошибка сохранения в parquet: {e}")
        return False


def load_data_to_db(processed_data, max_rows=100):
    """
    Загрузка данных в базу данных (максимум 100 строк)
    """
    load_dotenv()

    db_user = os.getenv("db_user")
    db_password = os.getenv("db_password")
    db_url = os.getenv("db_url")
    db_port = os.getenv("db_port")
    db_name = os.getenv("db_name")

    if not all([db_user, db_password, db_url, db_port, db_name]):
        print("!!!! Ошибка: Не все переменные окружения найдены в .env файле")
        return False

    try:
        engine = create_engine(
            f"postgresql+psycopg2://{db_user}:{db_password}@{db_url}:{db_port}/{db_name}"
        )
        print("Подключение к базе данных установлено")
    except Exception as e:
        print(f"!!!! Ошибка подключения к базе данных: {e}")
        return False

    try:
        df_for_upload = processed_data.head(max_rows)
        print(f"Подготовлено {len(df_for_upload)} строк для загрузки в БД")
    except Exception as e:
        print(f"!!!! Ошибка подготовки данных: {e}")
        return False

    try:
        df_for_upload.to_sql(
            name="kalashnikova",  # имя таблицы
            con=engine,
            schema="public",
            if_exists="replace",
            index=False,
        )
        print("Данные успешно записаны в базу")
        return True
    except Exception as e:
        print(f"!!!! Ошибка записи данных в базу: {e}")
        return False


def load_data(processed_data, max_rows=100):
    """
    Главная функция загрузки данных - сохранение в формат parquet и загрузка в базу данных
    """

    parquet_success = save_to_parquet(processed_data)
    db_success = load_data_to_db(processed_data, max_rows)
    if parquet_success and db_success:
        print("Загрузка данных успешно завершена!")
        return True
    else:
        print("!!!! Загрузка данных завершена с ошибками !!!!")
        return False
