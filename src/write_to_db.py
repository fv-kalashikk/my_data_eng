import pandas as pd
import sqlite3
from sqlalchemy import create_engine


def write_to_homeworks():
    # 1) читаем данные из creds.db
    try:
        conn_sqlite = sqlite3.connect("creds.db")
        cursor = conn_sqlite.cursor()
        cursor.execute("SELECT url, port, user, pass FROM access;")
        url, port, username, password = cursor.fetchone()
        conn_sqlite.close()
        print("Данные из creds.db получены")
    except Exception as e:
        print(f"!!!!!!!! Ошибка чтения creds.db: {e}")
        return

    # 2) загружаем данные из disease_risk_processed.parquet
    try:
        df_clean = pd.read_parquet("./data/processed/disease_risk_processed.parquet")
        data_to_write = df_clean.head(100)
    except Exception as e:
        print(f"!!!!!!!! Ошибка загрузки данных: {e}")
        return

    # 3) подключаемся к homeworks и записываем
    table_name = "kalashnikova"
    try:
        connection_string = f"postgresql://{username}:{password}@{url}:{port}/homeworks"
        engine = create_engine(connection_string)

        data_to_write.to_sql(
            name=table_name,
            con=engine,
            schema="public",
            if_exists="replace",
            index=False,
            method="multi",
        )

        print(f" Данные записаны. Таблица: public.{table_name}")
        print(f" Записано строк: {len(data_to_write)}")

        # проверка
        verify_data(engine, table_name)

    except Exception as e:
        print(f"!!!!!!!! Ошибка записи в базу: {e}")


def verify_data(engine, table_name):
    """
    Проверяем что данные записались правильно
    """
    try:
        # считаем строки
        count_query = f"SELECT COUNT(*) as row_count FROM public.{table_name};"
        count_result = pd.read_sql(count_query, engine)
        row_count = count_result["row_count"].iloc[0]

        print(f"В таблице: {row_count} строк")

        if row_count == 100:
            print("Записано ровно 100 строк")
        else:
            print(f"!!!!!!!! Записано {row_count} строк вместо 100")

        # первые 3 строки
        sample_query = f"SELECT * FROM public.{table_name} LIMIT 3;"
        sample_data = pd.read_sql(sample_query, engine)
        print("Пример данных (первые 3 строки):")
        print(sample_data)

    except Exception as e:
        print(f"!!!!!!!! Ошибка проверки: {e}")


if __name__ == "__main__":
    write_to_homeworks()
