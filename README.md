# Airflow Currency Parser for CIS Countries 🇰🇿🇧🇾🇺🇿🇹🇯🇰🇬🇦🇲🇦🇿🇬🇪🇲🇳

Этот проект собирает курсы валют (USD, EUR) из центральных банков стран СНГ и соседних стран. Используется Apache Airflow для оркестрации задач, развёрнут в Docker.

## 📦 Структура проекта

- `dags/` — DAG-файлы (в т.ч. `currency_dag.py`)
- `exchange_parsing/` — модули с парсерами для каждой страны
- `docker-compose.yaml` — запуск Airflow через Docker
- `logs/` — лог-файлы Airflow (игнорируются Git)
- `.gitignore` — исключения для Git
- `requirements.txt` — зависимости (если используешь)

## 🚀 Запуск (Docker)

```bash
docker-compose up
