import pandas as pd
import os

file_id = "1fVkeUdzuBjqHsLPtL_AzUOeL4kX4Y7sV"
file_url = f"https://drive.google.com/uc?id={file_id}"


def load_data_from_drive(file_url=file_url):
    """Загрузка датасета с Google Drive"""

    raw_data = pd.read_csv(file_url)

    if raw_data.empty:
        raise ValueError("!!!! Пустой файл !!!!")

    print(f"Файл загружен. Файл содержит {len(raw_data)} строк.")

    return raw_data


def save_raw_data(raw_data, filename="disease_risk_dataset.csv"):
    """Сохранение сырых данных в папку data/raw"""

    # создаем папки если их нет
    os.makedirs("data/raw", exist_ok=True)
    os.makedirs("data/processed", exist_ok=True)

    filepath = f"data/raw/{filename}"
    raw_data.to_csv(filepath, index=False)
    print(f"Сырые данные сохранены: {filepath}")

    return filepath


def extract_data():
    """Полный процесс извлечения данных"""
    raw_data = load_data_from_drive()
    save_raw_data(raw_data)
    return raw_data
