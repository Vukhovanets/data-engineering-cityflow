
## Status
Lab 1 completed successfully.
## Лабораторна робота №2: PostgreSQL у пайплайні даних (S02)

- **Середовище:** PostgreSQL 17.12, Python 3.15.2 (`psycopg[binary]`, `python-dotenv`).
- **Налаштування:** скопіювати `.env.example` у `.env` та вказати параметри підключення до локальної БД `cityflow_s02`.
- **Порядок запуску:**
  1. Ініціалізація схем і таблиць (`stg`, `core`, `audit`):
     `psql -U postgres -h localhost -d cityflow_s02 -f sql/lab02/01_init_schemas_and_tables.sql`
  2. Завантаження у staging, валідація та публікація у core:
     `python src/lab02/pipeline.py S02_v1`
  3. Контрольні SQL-перевірки:
     `psql -U postgres -h localhost -d cityflow_s02 -f sql/lab02/04_verification_queries.sql`
  4. Тести надійності (зламаний заголовок і транзакційний ROLLBACK):
     `python tests/test_lab02_reliability.py`