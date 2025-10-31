import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


def validate_raw_file_exists():
    """Проверка что файл скачан в data/raw"""
    raw_file_path = "data/raw/disease_risk_dataset.csv"

    file_exists = os.path.exists(raw_file_path)
    if file_exists:
        file_size = os.path.getsize(raw_file_path)
        print(f"Файл в data/raw существует. Размер файла: {file_size}")
    else:
        print("!!!! Файл отсутствует !!!!")

    return file_exists


def validate_bmi_cleaned(data_processed):
    """Проверка что удалены значения с BMI < 15"""
    if "BMI" not in data_processed.columns:
        print("!!!! Колонка BMI не найдена !!!!")
        return False

    bmi_violations = (data_processed["BMI"] < 15).sum()
    is_data_clean = bmi_violations == 0

    if is_data_clean:
        print(f"В Dataset НЕТ записей с BMI < 15):")
    else:
        print(f"!!!! В Dataset ЕСТЬ записи с BMI < 15) !!!!")
        print(f"Количество таких записей: {bmi_violations}")
        print(
            f"Диапазон BMI: {data_processed['BMI'].min():.1f} - {data_processed['BMI'].max():.1f}"
        )

    return is_data_clean


def validate_no_missing_values(data_processed):
    """Проверка что нет пропусков"""
    missing_by_col = data_processed.isnull().sum()
    missing_count = missing_by_col.sum()
    is_data_clean = missing_count == 0

    if is_data_clean:
        print("Пропусков НЕТ :)")
    else:
        print(f"Пропуски ЕСТЬ :(. Всего {missing_count} пропусков")

        missing_data = missing_by_col[missing_by_col > 0]
        if len(missing_data) > 0:
            missing_df = pd.DataFrame(
                {"Колонка": missing_data.index, "Пропуски": missing_data.values}
            )
            print("\n Колонки с пропусками:")
            print(missing_df.to_string(index=False))

    return is_data_clean


def validate_blood_pressure_correct(data_processed):
    """Проверка что нет неправильно записанного давления"""
    if (
        "Blood_Pressure_Systolic" not in data_processed.columns
        or "Blood_Pressure_Diastolic" not in data_processed.columns
    ):
        print("!!!! Колонки давления не найдены !!!!")
        return False

    # поиск непомеченных аномалий
    current_pressure_errors = (
        data_processed["Blood_Pressure_Systolic"]
        <= data_processed["Blood_Pressure_Diastolic"]
    )

    # если есть колонка с помеченными аномалиями (Pressure_Anomaly), то исключаем помечанные случаи
    if "Pressure_Anomaly" in data_processed.columns:
        # только новые (не помеченные) аномалии представляют интерес
        new_errors = current_pressure_errors & ~data_processed["Pressure_Anomaly"]
        pressure_errors_count = new_errors.sum()

    else:
        # если нет колонки Pressure_Anomaly, считаем все ошибки
        pressure_errors_count = current_pressure_errors.sum()
        print("Колонка Pressure_Anomaly не найдена - проверяем все записи")

    is_data_clean = pressure_errors_count == 0

    if is_data_clean:
        print("Соотношение давлений корректно :)")
    else:
        print(
            f"Соотношение давлений НЕкорректно :(. Новых ошибок давления: {pressure_errors_count}"
        )

    return is_data_clean


def validate_data_types(data_processed):
    """Проверка преобразования типов данных"""
    expected_types = {
        "Age": "int32",
        "BMI": "float32",
        "Blood_Pressure_Systolic": "int32",
        "Blood_Pressure_Diastolic": "int32",
        "Cholesterol_Level": "int32",
        "Glucose_Level": "int32",
        "Genetic_Risk_Score": "float32",
        "Family_History": "bool",
        "Disease_Risk": "bool",
        "Gender": "category",
        "Smoking_Status": "category",
        "Alcohol_Consumption": "category",
        "Physical_Activity_Level": "category",
        "Previous_Diagnosis": "category",
        "Patient_ID": "object",
        "Pressure_Anomaly": "bool",
    }
    type_checks = []
    incorrect_types = []

    for col, expected_type in expected_types.items():
        if col not in data_processed.columns:
            print(f"Колонка {col} не найдена")
            type_checks.append(False)
            incorrect_types.append(col)
            continue

        actual_type = str(data_processed[col].dtype)
        is_correct = actual_type == expected_type
        type_checks.append(is_correct)

        if is_correct:
            print(f"{col}: {actual_type} ✅")
        else:
            print(f"{col}: {actual_type} ❌ (ожидается: {expected_type})")
            incorrect_types.append(col)

    is_data_clean = all(type_checks)

    if is_data_clean:
        print("Все типы данных преобразованы правильно")
    else:
        print(f"Обнаружены ошибки в {len(incorrect_types)} колонках:")
        for col in incorrect_types:
            print(f"  - {col}")

    return is_data_clean


def validate_parquet_file():
    """Проверка что датафрейм сохранился в parquet и файл не пустой"""
    parquet_path = "data/processed/disease_risk_processed.parquet"

    file_exists = os.path.exists(parquet_path)
    if file_exists:
        print("Parquet файл существует!")
    else:
        print("!!!! Ошибка !!!! Не существует Parquet файла")
        return False

    try:
        df_parquet = pd.read_parquet(parquet_path)
        is_empty = df_parquet.empty
        file_size = os.path.getsize(parquet_path)
        if not is_empty:
            print("Файл непустой")
            print(f"Размер файла: {file_size} байт")
            print(
                f"Данные в parquet: {len(df_parquet)} строк, {len(df_parquet.columns)} колонок"
            )
        else:
            print("!!!! Ошибка !!!! Файл пустой!")
        return file_exists and not is_empty

    except Exception as e:
        print(f"!!!! Ошибка чтения parquet: {e} !!!!")
        return False


def validate_db_data(max_rows=100):
    """Проверка что данные записаны в БД"""
    load_dotenv()

    db_user = os.getenv("db_user")
    db_password = os.getenv("db_password")
    db_url = os.getenv("db_url")
    db_port = os.getenv("db_port")
    db_name = os.getenv("db_name")

    try:
        engine = create_engine(
            f"postgresql+psycopg2://{db_user}:{db_password}@{db_url}:{db_port}/{db_name}"
        )

        with engine.begin() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM kalashnikova"))
            count = result.scalar()

        if count == max_rows:
            print(f"В базе записано {count} строк (все верно!)")
            return True
        else:
            print(f"!!!! В БД записано {count} строк вместо {max_rows}")
            return False

    except Exception as e:
        print(f"!!!! Ошибка проверки данных в БД: {e}")
        return False


def validate_all(data_processed=None):
    """Запуск всех валидаций"""
    print("Запуск полной валидации ETL процесса")
    print("-" * 60)

    validations = []
    validations.append(validate_raw_file_exists())

    if data_processed is not None:
        validations.extend(
            [
                validate_bmi_cleaned(data_processed),
                validate_no_missing_values(data_processed),
                validate_blood_pressure_correct(data_processed),
                validate_data_types(data_processed),
                validate_parquet_file(),
                validate_db_data(),
            ]
        )
    else:
        print("Не выполнено преобразование данных. Проверка только сырых данных")

    all_valid = all(validations)
    print("-" * 60)
    return all_valid
