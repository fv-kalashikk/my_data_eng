import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import Session
import pandas as pd


def write_to_homeworks():
    """
    Функция записывает данные в базу homeworks
    """
    load_dotenv()

    db_user = os.getenv("db_user")
    db_password = os.getenv("db_password")
    db_url = os.getenv("db_url")
    db_port = os.getenv("db_port")
    db_name = os.getenv("db_name")

    if not all([db_user, db_password, db_url, db_port, db_name]):
        print("!!!!!!!! Не все переменные найдены в .env")
        return

    # подключение к базе
    try:
        engine = create_engine(
            f"postgresql+psycopg2://{db_user}:{db_password}@{db_url}:{db_port}/{db_name}"
        )
    except Exception as e:
        print(f"!!!!!!!! Ошибка подключения: {e}")
        return

    # загрузка данных
    try:
        df_clean = pd.read_parquet("./data/processed/disease_risk_processed.parquet")
        df_for_upload = df_clean.head(100)
    except Exception as e:
        print(f"!!!!!!!! Ошибка загрузки данных: {e}")
        return

    # запись данных
    try:
        df_for_upload.to_sql(
            name="kalashnikova",
            con=engine,
            schema="public",
            if_exists="replace",
            index=False,
        )
    except Exception as e:
        print(f"!!!!!!!! Ошибка записи: {e}")
        return

    # проверка записи
    try:
        with engine.begin() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM kalashnikova"))
            count = result.scalar()

            if count == 100:
                print(f"Проверка пройдена: {count} строк записано")
            else:
                print(f"!!!!!!!! Ошибка: записано {count} строк вместо 100")
    except Exception as e:
        print(f"!!!!!!!! Ошибка проверки: {e}")


if __name__ == "__main__":
    write_to_homeworks()
