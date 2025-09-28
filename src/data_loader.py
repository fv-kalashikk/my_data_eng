import pandas as pd

file_id = "1fVkeUdzuBjqHsLPtL_AzUOeL4kX4Y7sV"  # ID файла на Google Drive
file_url = f"https://drive.google.com/uc?id={file_id}"

raw_data = pd.read_csv(file_url)  # читаем файл

raw_data.head(10)
