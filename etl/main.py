# etl/__init__.py
import argparse
import sys
from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_data
from etl.validate import validate_all


def main():
    """Главная функция ETL процесса"""
    parser = argparse.ArgumentParser(
        description="ETL процесс для медицинских данных Disease Risk Prediction"
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=100,
        help="Максимальное количество строк для загрузки в БД",
    )

    args = parser.parse_args()

    print("Запуск ETL процесса...")
    print(f"Максимальное количество строк для БД: {args.max_rows}")
    print("-" * 50)

    try:
        # extract - извлечение данных
        print("1. Загрузка данных...")
        raw_data = extract_data()
        print("✅ Данные успешно загружены\n")

        # transform - преобразование данных
        print("2. Преобразование данных...")
        processed_data = transform_data(raw_data)
        print("✅ Преобразование данных завершено\n")

        # load - сохранение и загрузка данных
        print("3. Сохранение и загрузка данных в БД...")
        success = load_data(processed_data, max_rows=args.max_rows)

        if not success:
            print("!!!! Ошибка на этапе load!")
            sys.exit(1)
        print("✅ Данные сохранены и загружены в БД\n")

        # validate - валидация
        print("4. Проверка данных...")
        is_valid = validate_all(processed_data)

        if not is_valid:
            print("!!!! Валидация не пройдена!")
            sys.exit(1)
        print("✅ Валидация завершена успешно\n")

        print("ETL процесс успешно завершен!")
        sys.exit(0)

    except Exception as e:
        print(f"!!!! Критическая ошибка: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
