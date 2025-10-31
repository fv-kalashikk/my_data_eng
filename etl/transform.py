import pandas as pd
import numpy as np
import os


def remove_unrealistic_bmi(data_processed):
    """Функция удаляет строчки с нереалистичными
    показателями BMI (< 15) в соотвествии с рекомендацией EDA"""

    bmi_threshold = 15  # нереалистичный BMI

    initial_count = len(data_processed)
    unrealistic_count = (data_processed["BMI"] < bmi_threshold).sum()

    if unrealistic_count > 0:
        print(f"Найдено {unrealistic_count} строк с BMI < {bmi_threshold}")
        data_processed = data_processed[data_processed["BMI"] >= bmi_threshold]
        removed_count = initial_count - len(data_processed)
        print(f"Удалено {removed_count} строк с нереалистичным BMI")

    print(f"Осталось строк: {len(data_processed)} из {initial_count}")

    return data_processed


def fix_blood_pressure(data_processed):
    """Исправление показателей давления (если систолическое < диастолического)
    в соотвествии с рекомендацией EDA"""

    # колонка-флаг
    data_processed["Pressure_Anomaly"] = False

    # количество ошибочных записей
    pressure_issues = (
        data_processed["Blood_Pressure_Systolic"]
        <= data_processed["Blood_Pressure_Diastolic"]
    ).sum()

    if pressure_issues > 0:
        print(f"Найдено {pressure_issues} записей с некорректным соотношением давлений")

        # разделяем на типы аномалий (систолическое < диастолического ИЛИ давления равны)
        equal_mask = (
            data_processed["Blood_Pressure_Systolic"]
            == data_processed["Blood_Pressure_Diastolic"]
        )
        reversed_mask = (
            data_processed["Blood_Pressure_Systolic"]
            < data_processed["Blood_Pressure_Diastolic"]
        )

        # подсчет количества аномалий каждого типа
        equal_count = equal_mask.sum()
        reversed_count = reversed_mask.sum()
        data_processed.loc[equal_mask | reversed_mask, "Pressure_Anomaly"] = True

        # меняем местами ошибочные значения
        mask = (
            data_processed["Blood_Pressure_Systolic"]
            <= data_processed["Blood_Pressure_Diastolic"]
        )
        temp_systolic = data_processed.loc[mask, "Blood_Pressure_Systolic"].copy()
        temp_diastolic = data_processed.loc[mask, "Blood_Pressure_Diastolic"].copy()

        data_processed.loc[mask, "Blood_Pressure_Systolic"] = temp_diastolic
        data_processed.loc[mask, "Blood_Pressure_Diastolic"] = temp_systolic

        fixed_count = reversed_count  # обратные случаи исправляются заменой
        remaining_equal = equal_count  # равные случаи остаются

        print(f"  - Исправлено заменой: {fixed_count} записей")
        print(f"  - Осталось равных давлений: {remaining_equal} записей")

    return data_processed


def handle_missing_values(data_processed):
    """Заполнение пропусков значением "Unknown"
    в колонках 'Alcohol_Consumption' и 'Previous_Diagnosis'
    в соотвествии с рекомендацией EDA"""

    # Alcohol_Consumption
    if "Alcohol_Consumption" in data_processed.columns:
        alc_missing = data_processed["Alcohol_Consumption"].isnull().sum()
        data_processed["Alcohol_Consumption"] = data_processed[
            "Alcohol_Consumption"
        ].fillna("Unknown")
        print(f"'Alcohol_Consumption': заполнено {alc_missing} пропусков")

    # Previous_Diagnosis
    if "Previous_Diagnosis" in data_processed.columns:
        diag_missing = data_processed["Previous_Diagnosis"].isnull().sum()
        data_processed["Previous_Diagnosis"] = data_processed[
            "Previous_Diagnosis"
        ].fillna("Unknown")
        print(f"'Previous_Diagnosis': заполнено {diag_missing} пропусков")

    return data_processed


def transform_types(df_processed):
    # 1. преобразование "Family_History" и "Disease_Risk" в булевый тип данных
    bool_columns = ["Family_History", "Disease_Risk"]
    for col in bool_columns:
        df_processed[col] = (
            df_processed[col].map({"Yes": True, "No": False}).astype(bool)
        )

    # 2. преобразование "Gender", "Smoking_Status", "Alcohol_Consumption",
    # "Physical_Activity_Level" и "Previous_Diagnosis"
    # в категориальный тип данных

    categorical_columns = [
        "Gender",  # ['Female', 'Male', 'Other']
        "Smoking_Status",  # ['Never', 'Former', 'Current']
        "Alcohol_Consumption",  # [Unknown, 'Moderate', 'High']
        "Physical_Activity_Level",  # ['Low', 'Moderate', 'High']
        "Previous_Diagnosis",  # [Unknown, 'Diagnosed', 'Pre-disease']
    ]
    for col in categorical_columns:
        df_processed[col] = df_processed[col].astype("category")

    # 3. преобразование оставшихся колонок в тип int32 и float32
    # целочисленные -> int32
    int_columns = [
        "Age",
        "Blood_Pressure_Systolic",
        "Blood_Pressure_Diastolic",
        "Cholesterol_Level",
        "Glucose_Level",
    ]
    for col in int_columns:
        df_processed[col] = df_processed[col].astype("int32")

    # дробные -> float32
    float_columns = ["BMI", "Genetic_Risk_Score"]
    for col in float_columns:
        df_processed[col] = df_processed[col].astype("float32")

    # 4. Patient_ID остается как object

    return df_processed


def transform_data(raw_data):
    """Полный процесс трансформации данных с сохранением в parquet"""
    data_processed = raw_data.copy()

    data_processed = remove_unrealistic_bmi(data_processed)
    data_processed = fix_blood_pressure(data_processed)
    data_processed = handle_missing_values(data_processed)
    data_processed = transform_types(data_processed)

    return data_processed
