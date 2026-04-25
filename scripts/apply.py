from dotenv import load_dotenv
import subprocess
import os

# 1. Загружаем секреты из .env в окружение процесса
load_dotenv()

# 2. Запускаем feast apply через этот же процесс
# Он автоматически подставит все ${DB_...} в YAML
result = subprocess.run(["feast", "-c", "local_repo/feature_repo", "apply"])