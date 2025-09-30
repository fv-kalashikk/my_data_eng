import pandas as pd
import os


def load_data_from_drive():
    """Загрузка датасета с Google Drive"""

    # Создаем папки если их нет
    os.makedirs("data/raw", exist_ok=True)
    os.makedirs("data/processed", exist_ok=True)

    file_id = "1fVkeUdzuBjqHsLPtL_AzUOeL4kX4Y7sV"
    file_url = f"https://drive.google.com/uc?id={file_id}"

    raw_data = pd.read_csv(file_url)

    # Сохраняем в папку data/raw
    raw_data.to_csv("data/raw/disease_risk_dataset.csv", index=False)

    return raw_data


# Загружаем данные
raw_data = load_data_from_drive()
