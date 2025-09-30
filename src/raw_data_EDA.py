# Основная информация
print("=== ИНФОРМАЦИЯ О ДАННЫХ ===")
raw_data.info()

# Пропущенные значения
print("\n=== ПРОПУЩЕННЫЕ ЗНАЧЕНИЯ ===")
print(raw_data.isnull().sum())

# Дубликаты
print("\n=== ДУБЛИКАТЫ ===")
print(f"Количество дубликатов: {raw_data.duplicated().sum()}")

# Уникальные значения в потенциальных категориальных колонках
print("\n=== УНИКАЛЬНЫЕ ЗНАЧЕНИЯ ===")
categorical_cols = [
    "Gender",
    "Smoking_Status",
    "Alcohol_Consumption",
    "Physical_Activity_Level",
    "Family_History",
    "Previous_Diagnosis",
    "Disease_Risk",
]
for col in categorical_cols:
    print(f"{col}: {raw_data[col].unique()}")
    print(f"Пропусков: {raw_data[col].isnull().sum()}")
    print("---")

# из этого кода можно понять, что "Gender", "Smoking_Status", "Alcohol_Consumption",
# "Physical_Activity_Level" и "Previous_Diagnosis" следует преобразовать
# в категориальных тип данных (так как есть только 3 уникальных значения), а
# "Family_History" и "Disease_Risk" можно преобразовать в булевый тип данных
# (так как есть только 2 уникальных значения "YES" и "NO")
