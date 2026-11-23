import csv
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
import psycopg
from dotenv import load_dotenv

load_dotenv()

EXPECTED_HEADERS = {
    "customers": ["customer_id", "full_name", "email", "phone", "city", "service_zone_id", "registered_at", "customer_status"],
    "merchants": ["merchant_id", "merchant_name", "merchant_type", "city", "service_zone_id", "opened_at", "merchant_status"],
    "products": ["product_id", "merchant_id", "product_name", "category", "unit_price_uah", "is_active", "updated_at"],
    "orders": ["order_id", "customer_id", "merchant_id", "created_at", "order_status", "currency", "subtotal_amount", "delivery_fee", "discount_amount", "total_amount"],
    "order_items": ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "line_discount", "line_total", "currency"],
}

ALLOWED_CURRENCIES = {"UAH", "USD", "EUR"}


def get_conn():
    return psycopg.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "cityflow_s02"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", ""),
    )


def compute_fingerprint(file_paths: dict[str, Path]) -> str:
    h = hashlib.sha256()
    for name in sorted(file_paths.keys()):
        h.update(name.encode())
        h.update(file_paths[name].read_bytes())
    return h.hexdigest()


def parse_ts(val: str | None) -> datetime | None:
    if val is None or val.strip() == "":
        return None
    s = val.strip()
    try:
        if len(s) >= 16 and s[2] == "/" and s[5] == "/":
            dt = datetime.strptime(s, "%d/%m/%Y %H:%M")
            return dt.replace(tzinfo=timezone.utc)
        if s.endswith("Z"):
            s_iso = s[:-1] + "+00:00"
        else:
            s_iso = s
        if "." in s_iso:
            main_part, rest = s_iso.split(".", 1)
            tz_idx = next((i for i, ch in enumerate(rest) if ch in "+-"), len(rest))
            frac = rest[:tz_idx][:6]
            tz_part = rest[tz_idx:]
            s_iso = f"{main_part}.{frac}{tz_part}"
        dt = datetime.fromisoformat(s_iso)
        if dt.tzinfo is None:
            return None
        return dt.astimezone(timezone.utc)
    except ValueError:
        return None


def parse_dec(val: str | None) -> Decimal | None:
    if val is None or val.strip() == "":
        return None
    try:
        d = Decimal(val.strip())
        if not d.is_finite():
            return None
        return d.quantize(Decimal("0.01"))
    except InvalidOperation:
        return None


def parse_int(val: str | None) -> int | None:
    if val is None or val.strip() == "":
        return None
    try:
        return int(val.strip())
    except ValueError:
        return None


def parse_bool(val: str | None) -> bool | None:
    if val is None:
        return None
    s = val.strip().lower()
    if s in {"true", "1", "yes"}:
        return True
    if s in {"false", "0", "no"}:
        return False
    return None


def read_and_validate_structure(base_dir: Path, rel_dir: Path) -> dict[str, list[dict]]:
    paths = {
        "customers": base_dir / "customers.csv",
        "merchants": base_dir / "merchants.csv",
        "products": base_dir / "products.csv",
        "orders": base_dir / "orders.csv",
        "order_items": rel_dir / "order_items.csv",
    }
    raw_data = {}
    for entity, path in paths.items():
        if not path.exists():
            raise FileNotFoundError(f"Відсутній обов'язковий файл: {path}")
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            try:
                header = next(reader)
            except StopIteration:
                raise ValueError(f"Порожній файл: {path}")
            expected = EXPECTED_HEADERS[entity]
            if header != expected:
                raise ValueError(f"Неправильний заголовок у {path.name}: очікувалось {expected}, отримано {header}")
            rows = []
            for idx, row in enumerate(reader, start=1):
                if len(row) != len(expected):
                    raise ValueError(f"Зламана структура рядка {idx} у {path.name}")
                rows.append(dict(zip(expected, row)))
            raw_data[entity] = rows
    return raw_data, paths


def resolve_duplicates(records: list[dict], key_field: str, biz_fields: list[str]):
    by_key: dict[str, list[dict]] = {}
    for r in records:
        if r["reasons"]:
            continue
        by_key.setdefault(r["clean"][key_field], []).append(r)

    for key, group in by_key.items():
        if len(group) == 1:
            continue
        signatures = {tuple(item["clean"][f] for f in biz_fields) for item in group}
        if len(signatures) == 1:
            for dup in group[1:]:
                dup["status"] = "duplicate"
                dup["reasons"].append("EXACT_DUPLICATE")
        else:
            for conflict in group:
                conflict["status"] = "rejected"
                conflict["reasons"].append("KEY_CONFLICT")


def run_pipeline(batch_id: str, base_dir: Path, rel_dir: Path):
    raw_data, paths = read_and_validate_structure(base_dir, rel_dir)
    fingerprint = compute_fingerprint(paths)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT batch_fingerprint FROM audit.etl_runs WHERE batch_id = %s AND status = 'success'",
                (batch_id,),
            )
            prev = cur.fetchall()
            if prev and any(row[0] != fingerprint for row in prev):
                raise RuntimeError(f"Конфлікт batch_id='{batch_id}': вміст файлів змінився відносно попереднього успішного запуску!")

            cur.execute(
                "INSERT INTO audit.etl_runs (batch_id, batch_fingerprint, status) VALUES (%s, %s, 'running') RETURNING run_id",
                (batch_id, fingerprint),
            )
            run_id = cur.fetchone()[0]
        conn.commit()

    try:
        # 1. Завантаження у staging
        with get_conn() as conn:
            with conn.cursor() as cur:
                for entity, rows in raw_data.items():
                    cols = EXPECTED_HEADERS[entity]
                    all_cols = cols + ["batch_id", "source_file", "source_row_number"]
                    placeholders = ", ".join(["%s"] * len(all_cols))
                    sql = f"INSERT INTO stg.{entity} ({', '.join(all_cols)}) VALUES ({placeholders})"
                    params = [
                        [r[c] for c in cols] + [batch_id, f"{entity}.csv", i]
                        for i, r in enumerate(rows, start=1)
                    ]
                    if params:
                        cur.executemany(sql, params)
            conn.commit()

        # 2. Валідація у пам'яті за контрактом
        validated: dict[str, list[dict]] = {k: [] for k in EXPECTED_HEADERS}

        # Customers
        for i, r in enumerate(raw_data["customers"], start=1):
            reasons = []
            cid = r["customer_id"].strip()
            fname = r["full_name"].strip()
            city = r["city"].strip()
            reg_at = parse_ts(r["registered_at"])
            if not cid:
                reasons.append("EMPTY_PK")
            if not fname:
                reasons.append("EMPTY_FULL_NAME")
            if not city:
                reasons.append("EMPTY_CITY")
            if reg_at is None:
                reasons.append("INVALID_TIMESTAMP")
            validated["customers"].append({
                "row_num": i, "key": cid or None, "raw": r, "reasons": reasons,
                "status": "rejected" if reasons else "accepted",
                "clean": {"customer_id": cid, "full_name": fname, "city": city, "registered_at": reg_at}
            })
        resolve_duplicates(validated["customers"], "customer_id", ["full_name", "city", "registered_at"])
        valid_customers = {x["clean"]["customer_id"] for x in validated["customers"] if x["status"] == "accepted"}

        # Merchants
        for i, r in enumerate(raw_data["merchants"], start=1):
            reasons = []
            mid = r["merchant_id"].strip()
            mname = r["merchant_name"].strip()
            city = r["city"].strip()
            opened = parse_ts(r["opened_at"])
            if not mid:
                reasons.append("EMPTY_PK")
            if not mname:
                reasons.append("EMPTY_MERCHANT_NAME")
            if not city:
                reasons.append("EMPTY_CITY")
            if opened is None:
                reasons.append("INVALID_TIMESTAMP")
            validated["merchants"].append({
                "row_num": i, "key": mid or None, "raw": r, "reasons": reasons,
                "status": "rejected" if reasons else "accepted",
                "clean": {"merchant_id": mid, "merchant_name": mname, "city": city, "opened_at": opened}
            })
        resolve_duplicates(validated["merchants"], "merchant_id", ["merchant_name", "city", "opened_at"])
        valid_merchants = {x["clean"]["merchant_id"] for x in validated["merchants"] if x["status"] == "accepted"}

        # Products
        for i, r in enumerate(raw_data["products"], start=1):
            reasons = []
            pid = r["product_id"].strip()
            mid = r["merchant_id"].strip()
            pname = r["product_name"].strip()
            price = parse_dec(r["unit_price_uah"])
            active = parse_bool(r["is_active"])
            if not pid:
                reasons.append("EMPTY_PK")
            if not pname:
                reasons.append("EMPTY_PRODUCT_NAME")
            if price is None or price < 0:
                reasons.append("INVALID_PRICE")
            if active is None:
                reasons.append("INVALID_BOOLEAN")
            if active is True and price is not None and price <= 0:
                reasons.append("ACTIVE_ZERO_PRICE")
            if mid not in valid_merchants:
                reasons.append("MISSING_FK_MERCHANT")
            validated["products"].append({
                "row_num": i, "key": pid or None, "raw": r, "reasons": reasons,
                "status": "rejected" if reasons else "accepted",
                "clean": {"product_id": pid, "merchant_id": mid, "product_name": pname, "unit_price_uah": price, "is_active": active}
            })
        resolve_duplicates(validated["products"], "product_id", ["merchant_id", "product_name", "unit_price_uah", "is_active"])
        valid_products = {x["clean"]["product_id"] for x in validated["products"] if x["status"] == "accepted"}

        # Orders
        for i, r in enumerate(raw_data["orders"], start=1):
            reasons = []
            oid = r["order_id"].strip()
            cid = r["customer_id"].strip()
            mid = r["merchant_id"].strip()
            created = parse_ts(r["created_at"])
            status = r["order_status"].strip()
            curr = r["currency"].strip()
            sub = parse_dec(r["subtotal_amount"])
            fee = parse_dec(r["delivery_fee"])
            disc = parse_dec(r["discount_amount"])
            tot = parse_dec(r["total_amount"])
            if not oid:
                reasons.append("EMPTY_PK")
            if created is None:
                reasons.append("INVALID_TIMESTAMP")
            if not status:
                reasons.append("EMPTY_STATUS")
            if curr not in ALLOWED_CURRENCIES:
                reasons.append("INVALID_CURRENCY")
            if any(x is None or x < 0 for x in (sub, fee, disc, tot)):
                reasons.append("INVALID_AMOUNT")
            if cid not in valid_customers:
                reasons.append("MISSING_FK_CUSTOMER")
            if mid not in valid_merchants:
                reasons.append("MISSING_FK_MERCHANT")
            validated["orders"].append({
                "row_num": i, "key": oid or None, "raw": r, "reasons": reasons,
                "status": "rejected" if reasons else "accepted",
                "clean": {
                    "order_id": oid, "customer_id": cid, "merchant_id": mid, "created_at": created,
                    "order_status": status, "currency": curr, "subtotal_amount": sub,
                    "delivery_fee": fee, "discount_amount": disc, "total_amount": tot
                }
            })
        resolve_duplicates(
            validated["orders"], "order_id",
            ["customer_id", "merchant_id", "created_at", "order_status", "currency", "subtotal_amount", "delivery_fee", "discount_amount", "total_amount"]
        )
        valid_orders = {x["clean"]["order_id"] for x in validated["orders"] if x["status"] == "accepted"}

        # Order items
        for i, r in enumerate(raw_data["order_items"], start=1):
            reasons = []
            oi_id = r["order_item_id"].strip()
            oid = r["order_id"].strip()
            pid = r["product_id"].strip()
            qty = parse_int(r["quantity"])
            uprice = parse_dec(r["unit_price"])
            ldisc = parse_dec(r["line_discount"])
            ltot = parse_dec(r["line_total"])
            curr = r["currency"].strip()
            if not oi_id:
                reasons.append("EMPTY_PK")
            if qty is None or qty <= 0:
                reasons.append("INVALID_QUANTITY")
            if any(x is None or x < 0 for x in (uprice, ldisc, ltot)):
                reasons.append("INVALID_AMOUNT")
            if curr not in ALLOWED_CURRENCIES:
                reasons.append("INVALID_CURRENCY")
            if oid not in valid_orders:
                reasons.append("MISSING_FK_ORDER")
            if pid not in valid_products:
                reasons.append("MISSING_FK_PRODUCT")
            validated["order_items"].append({
                "row_num": i, "key": oi_id or None, "raw": r, "reasons": reasons,
                "status": "rejected" if reasons else "accepted",
                "clean": {
                    "order_item_id": oi_id, "order_id": oid, "product_id": pid, "quantity": qty,
                    "unit_price": uprice, "line_discount": ldisc, "line_total": ltot, "currency": curr
                }
            })
        resolve_duplicates(
            validated["order_items"], "order_item_id",
            ["order_id", "product_id", "quantity", "unit_price", "line_discount", "line_total", "currency"]
        )

        # 3. Атомарна транзакція публікації в core + запис аудиту
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE core.order_items, core.orders, core.products, core.merchants, core.customers;")

                for entity in ["customers", "merchants", "products", "orders", "order_items"]:
                    accepted_rows = [x["clean"] for x in validated[entity] if x["status"] == "accepted"]
                    if accepted_rows:
                        cols = list(accepted_rows[0].keys())
                        placeholders = ", ".join(["%s"] * len(cols))
                        sql = f"INSERT INTO core.{entity} ({', '.join(cols)}) VALUES ({placeholders})"
                        cur.executemany(sql, [[row[c] for c in cols] for row in accepted_rows])

                    # Контрольна звірка кількості рядків
                    cur.execute(f"SELECT COUNT(*) FROM core.{entity}")
                    db_count = cur.fetchone()[0]
                    if db_count != len(accepted_rows):
                        raise RuntimeError(f"Помилка звірки {entity}: у core {db_count}, очікувалось {len(accepted_rows)}")

                    # Статистика по файлу
                    in_cnt = len(validated[entity])
                    acc_cnt = len(accepted_rows)
                    rej_cnt = sum(1 for x in validated[entity] if x["status"] == "rejected")
                    dup_cnt = sum(1 for x in validated[entity] if x["status"] == "duplicate")
                    cur.execute(
                        """INSERT INTO audit.etl_file_stats
                           (run_id, source_file, input_count, accepted_count, rejected_count, duplicate_count)
                           VALUES (%s, %s, %s, %s, %s, %s)""",
                        (run_id, f"{entity}.csv", in_cnt, acc_cnt, rej_cnt, dup_cnt),
                    )

                    # Детальні рішення по кожному рядку
                    dec_params = [
                        (run_id, batch_id, f"{entity}.csv", x["row_num"], x["key"], x["status"], x["reasons"], json.dumps(x["raw"]))
                        for x in validated[entity]
                    ]
                    if dec_params:
                        cur.executemany(
                            """INSERT INTO audit.row_decisions
                               (run_id, batch_id, source_file, source_row_number, business_key, status, reason_codes, raw_payload)
                               VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)""",
                            dec_params,
                        )

                cur.execute(
                    "UPDATE audit.etl_runs SET status = 'success', finished_at = now() WHERE run_id = %s",
                    (run_id,),
                )
            conn.commit()
        print(f"[OK] Run #{run_id} (batch={batch_id}) успішно опубліковано в core!")

    except Exception as exc:
        with get_conn() as err_conn:
            with err_conn.cursor() as cur:
                cur.execute(
                    "UPDATE audit.etl_runs SET status = 'failed', finished_at = now(), error_message = %s WHERE run_id = %s",
                    (str(exc), run_id),
                )
            err_conn.commit()
        print(f"[FAILED] Run #{run_id} відкочено (ROLLBACK): {exc}", file=sys.stderr)
        raise


if __name__ == "__main__":
    b_id = sys.argv[1] if len(sys.argv) > 1 else "S02_v1"
    b_dir = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("data/raw/base")
    r_dir = Path(sys.argv[3]) if len(sys.argv) > 3 else Path("data/raw/relational")
    run_pipeline(b_id, b_dir, r_dir)