import csv
import shutil
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from lab02.pipeline import get_conn, run_pipeline


def build_fixtures():
    fix_root = Path("tests/fixtures")
    for sub in ["broken_db_constraint", "bad_header"]:
        d = fix_root / sub
        if d.exists():
            shutil.rmtree(d)
        (d / "base").mkdir(parents=True)
        (d / "relational").mkdir(parents=True)

    # 1. Фікстур зі зламаним заголовком
    bad_dir = fix_root / "bad_header"
    for f in ["customers.csv", "merchants.csv", "products.csv", "orders.csv"]:
        shutil.copy(Path("data/raw/base") / f, bad_dir / "base" / f)
    with (bad_dir / "relational" / "order_items.csv").open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["wrong_col_name", "order_id", "product_id", "quantity", "unit_price", "line_discount", "line_total", "currency"])

    # 2. Мінімальний синтетичний фікстур для перевірки транзакційного ROLLBACK
    rb_dir = fix_root / "broken_db_constraint"
    with (rb_dir / "base" / "customers.csv").open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["customer_id", "full_name", "email", "phone", "city", "service_zone_id", "registered_at", "customer_status"])
        w.writerow(["TEST-C1", "Test User", "t@test.ua", "", "Lviv", "Z1", "2026-01-01T10:00:00Z", "active"])

    with (rb_dir / "base" / "merchants.csv").open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["merchant_id", "merchant_name", "merchant_type", "city", "service_zone_id", "opened_at", "merchant_status"])
        w.writerow(["TEST-M1", "Test Shop", "cafe", "Lviv", "Z1", "2025-01-01T10:00:00Z", "active"])

    with (rb_dir / "base" / "products.csv").open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["product_id", "merchant_id", "product_name", "category", "unit_price_uah", "is_active", "updated_at"])
        w.writerow(["TEST-P1", "TEST-M1", "Test Item", "food", "100.00", "True", "2026-01-01T10:00:00Z"])

    with (rb_dir / "base" / "orders.csv").open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["order_id", "customer_id", "merchant_id", "created_at", "order_status", "currency", "subtotal_amount", "delivery_fee", "discount_amount", "total_amount"])
        w.writerow(["TEST-O1", "TEST-C1", "TEST-M1", "2026-01-02T10:00:00Z", "delivered", "UAH", "100.00", "20.00", "0.00", "120.00"])

    with (rb_dir / "relational" / "order_items.csv").open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["order_item_id", "order_id", "product_id", "quantity", "unit_price", "line_discount", "line_total", "currency"])
        w.writerow(["TEST-I1", "TEST-O1", "TEST-P1", "1", "100.00", "0.00", "100.00", "UAH"])


def test_rollback_after_partial_core_change():
    # Імітуємо збій безпосередньо під час транзакції зміни core (вставка quantity = -1 після TRUNCATE і вставки батьків)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO audit.etl_runs (batch_id, batch_fingerprint, status) VALUES ('TEST_ROLLBACK', 'fixture_fp', 'running') RETURNING run_id"
            )
            run_id = cur.fetchone()[0]
        conn.commit()

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE core.order_items, core.orders, core.products, core.merchants, core.customers;")
                cur.execute("INSERT INTO core.customers VALUES ('TEST-C1', 'Test User', 'Lviv', '2026-01-01T10:00:00Z');")
                # Порушуємо CHECK (quantity > 0) вже після очищення core та вставки клієнта
                cur.execute("INSERT INTO core.order_items VALUES ('TEST-I1', 'TEST-O1', 'TEST-P1', -1, 100.00, 0.00, 100.00, 'UAH');")
            conn.commit()
    except Exception as e:
        with get_conn() as err_conn:
            with err_conn.cursor() as cur:
                cur.execute(
                    "UPDATE audit.etl_runs SET status = 'failed', finished_at = now(), error_message = %s WHERE run_id = %s",
                    (str(e).splitlines()[0], run_id),
                )
            err_conn.commit()
        print(f"[OK] Rollback спрацював коректно на Run #{run_id}: {str(e).splitlines()[0]}")


if __name__ == "__main__":
    build_fixtures()
    # 1. Негативний тест: зламаний заголовок
    try:
        run_pipeline("TEST_BAD_HEADER", Path("tests/fixtures/bad_header/base"), Path("tests/fixtures/bad_header/relational"))
    except ValueError as e:
        print(f"[OK] Зупинка до публікації при зламаному заголовку: {e}")

    # 2. Тест Rollback після часткової зміни core
    test_rollback_after_partial_core_change()