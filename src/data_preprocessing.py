import pandas as pd
from data_loader import raw_data

# Создаем копию датафрейма
df_processed = raw_data.copy()

# 1. "Family_History" и "Disease_Risk" можно преобразовать в булевый тип данных
# (так как есть только 2 уникальных значения "YES" и "NO")
# (Yes/No -> True/False)

bool_columns = ["Family_History", "Disease_Risk"]
for col in bool_columns:
    df_processed[col] = df_processed[col].map({"Yes": True, "No": False}).astype(bool)


# 2. "Gender", "Smoking_Status", "Alcohol_Consumption", "Physical_Activity_Level" и "Previous_Diagnosis" следует преобразовать
# в категориальный тип данных (так как есть только 3 уникальных значения),

categorical_columns = [
    "Gender",  # ['Female', 'Male', 'Other']
    "Smoking_Status",  # ['Never', 'Former', 'Current']
    "Alcohol_Consumption",  # [nan, 'Moderate', 'High']
    "Physical_Activity_Level",  # ['Low', 'Moderate', 'High']
    "Previous_Diagnosis",  # [nan, 'Diagnosed', 'Pre-disease']
]

for col in categorical_columns:
    df_processed[col] = df_processed[col].astype("category")

# Оставшиеся колонки нужно преобразовать в тип int32 и float32

# Целочисленные -> int32
int_columns = [
    "Age",
    "Blood_Pressure_Systolic",
    "Blood_Pressure_Diastolic",
    "Cholesterol_Level",
    "Glucose_Level",
]
for col in int_columns:
    df_processed[col] = df_processed[col].astype("int32")

# Дробные -> float32
float_columns = ["BMI", "Genetic_Risk_Score"]
for col in float_columns:
    df_processed[col] = df_processed[col].astype("float32")

# 4. Patient_ID можно оставить как object

# 5. Проверка что типы преобразованы
print("=== ИНФОРМАЦИЯ О ДАННЫХ ===")
print(df_processed.info())

# Сохранение препроцессированных данных
df_processed.to_parquet("data/processed/disease_risk_processed.parquet", index=False)
