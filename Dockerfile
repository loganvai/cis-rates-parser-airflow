FROM apache/airflow:3.0.2

# 1) Ставим системные зависимости от root
USER root

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      wget unzip ca-certificates \
      chromium chromium-driver \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# 2) Переключаемся на airflow‑пользователя
USER airflow

# 3) Устанавливаем Python‑библиотеки от airflow
RUN pip install --no-cache-dir \
      requests \
      beautifulsoup4 \
      pandas \
      openpyxl \
      selenium \
      lxml \
      webdriver-manager