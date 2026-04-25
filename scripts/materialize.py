import os
from datetime import datetime
from dotenv import load_dotenv
from feast import FeatureStore

# 1. Загружаем переменные (чтобы YAML увидел ${DB_PASSWORD})
load_dotenv()


def run_materialization():
    # Путь к папке, где лежит feature_store.yaml
    repo_path = "local_repo/feature_repo"

    # 2. Инициализируем стор
    store = FeatureStore(repo_path=repo_path)

    # 3. Определяем дату (до какого момента «заливаем» данные в онлайн)
    # Обычно это "сейчас"
    end_date = datetime.now()

    print(f"Начинаю материализацию до {end_date}...")

    # 4. Запуск материализации (аналог materialize-incremental)
    store.materialize_incremental(end_date=end_date)

    print("Материализация успешно завершена!")


if __name__ == "__main__":
    run_materialization()
