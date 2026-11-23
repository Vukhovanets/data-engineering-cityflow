CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS audit;

-- STAGING
DROP TABLE IF EXISTS stg.customers, stg.merchants, stg.products, stg.orders, stg.order_items CASCADE;

CREATE TABLE stg.customers (
    customer_id TEXT, full_name TEXT, email TEXT, phone TEXT, city TEXT,
    service_zone_id TEXT, registered_at TEXT, customer_status TEXT,
    batch_id TEXT NOT NULL, source_file TEXT NOT NULL,
    source_row_number INTEGER NOT NULL, loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE stg.merchants (
    merchant_id TEXT, merchant_name TEXT, merchant_type TEXT, city TEXT,
    service_zone_id TEXT, opened_at TEXT, merchant_status TEXT,
    batch_id TEXT NOT NULL, source_file TEXT NOT NULL,
    source_row_number INTEGER NOT NULL, loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE stg.products (
    product_id TEXT, merchant_id TEXT, product_name TEXT, category TEXT,
    unit_price_uah TEXT, is_active TEXT, updated_at TEXT,
    batch_id TEXT NOT NULL, source_file TEXT NOT NULL,
    source_row_number INTEGER NOT NULL, loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE stg.orders (
    order_id TEXT, customer_id TEXT, merchant_id TEXT, created_at TEXT,
    order_status TEXT, currency TEXT, subtotal_amount TEXT,
    delivery_fee TEXT, discount_amount TEXT, total_amount TEXT,
    batch_id TEXT NOT NULL, source_file TEXT NOT NULL,
    source_row_number INTEGER NOT NULL, loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE stg.order_items (
    order_item_id TEXT, order_id TEXT, product_id TEXT, quantity TEXT,
    unit_price TEXT, line_discount TEXT, line_total TEXT, currency TEXT,
    batch_id TEXT NOT NULL, source_file TEXT NOT NULL,
    source_row_number INTEGER NOT NULL, loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- CORE
DROP TABLE IF EXISTS core.order_items, core.orders, core.products, core.merchants, core.customers CASCADE;

CREATE TABLE core.customers (
    customer_id TEXT PRIMARY KEY CHECK (btrim(customer_id) <> ''),
    full_name TEXT NOT NULL CHECK (btrim(full_name) <> ''),
    city TEXT NOT NULL CHECK (btrim(city) <> ''),
    registered_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE core.merchants (
    merchant_id TEXT PRIMARY KEY CHECK (btrim(merchant_id) <> ''),
    merchant_name TEXT NOT NULL CHECK (btrim(merchant_name) <> ''),
    city TEXT NOT NULL CHECK (btrim(city) <> ''),
    opened_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE core.products (
    product_id TEXT PRIMARY KEY CHECK (btrim(product_id) <> ''),
    merchant_id TEXT NOT NULL REFERENCES core.merchants(merchant_id),
    product_name TEXT NOT NULL CHECK (btrim(product_name) <> ''),
    unit_price_uah NUMERIC(12,2) NOT NULL CHECK (unit_price_uah >= 0),
    is_active BOOLEAN NOT NULL,
    CONSTRAINT chk_active_product_price CHECK (NOT is_active OR unit_price_uah > 0)
);

CREATE TABLE core.orders (
    order_id TEXT PRIMARY KEY CHECK (btrim(order_id) <> ''),
    customer_id TEXT NOT NULL REFERENCES core.customers(customer_id),
    merchant_id TEXT NOT NULL REFERENCES core.merchants(merchant_id),
    created_at TIMESTAMPTZ NOT NULL,
    order_status TEXT NOT NULL CHECK (btrim(order_status) <> ''),
    currency TEXT NOT NULL CHECK (currency IN ('UAH', 'USD', 'EUR')),
    subtotal_amount NUMERIC(12,2) NOT NULL CHECK (subtotal_amount >= 0),
    delivery_fee NUMERIC(12,2) NOT NULL CHECK (delivery_fee >= 0),
    discount_amount NUMERIC(12,2) NOT NULL CHECK (discount_amount >= 0),
    total_amount NUMERIC(12,2) NOT NULL CHECK (total_amount >= 0)
);

CREATE TABLE core.order_items (
    order_item_id TEXT PRIMARY KEY CHECK (btrim(order_item_id) <> ''),
    order_id TEXT NOT NULL REFERENCES core.orders(order_id),
    product_id TEXT NOT NULL REFERENCES core.products(product_id),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0),
    line_discount NUMERIC(12,2) NOT NULL CHECK (line_discount >= 0),
    line_total NUMERIC(12,2) NOT NULL CHECK (line_total >= 0),
    currency TEXT NOT NULL CHECK (currency IN ('UAH', 'USD', 'EUR'))
);

-- AUDIT
DROP TABLE IF EXISTS audit.row_decisions, audit.etl_file_stats, audit.etl_runs CASCADE;

CREATE TABLE audit.etl_runs (
    run_id SERIAL PRIMARY KEY,
    batch_id TEXT NOT NULL,
    batch_fingerprint TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    error_message TEXT
);

CREATE TABLE audit.etl_file_stats (
    run_id INTEGER NOT NULL REFERENCES audit.etl_runs(run_id) ON DELETE CASCADE,
    source_file TEXT NOT NULL,
    input_count INTEGER NOT NULL CHECK (input_count >= 0),
    accepted_count INTEGER NOT NULL CHECK (accepted_count >= 0),
    rejected_count INTEGER NOT NULL CHECK (rejected_count >= 0),
    duplicate_count INTEGER NOT NULL CHECK (duplicate_count >= 0),
    PRIMARY KEY (run_id, source_file),
    CONSTRAINT chk_counts_sum CHECK (input_count = accepted_count + rejected_count + duplicate_count)
);

CREATE TABLE audit.row_decisions (
    decision_id BIGSERIAL PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES audit.etl_runs(run_id) ON DELETE CASCADE,
    batch_id TEXT NOT NULL,
    source_file TEXT NOT NULL,
    source_row_number INTEGER NOT NULL,
    business_key TEXT,
    status TEXT NOT NULL CHECK (status IN ('accepted', 'rejected', 'duplicate')),
    reason_codes TEXT[] NOT NULL DEFAULT '{}',
    raw_payload JSONB NOT NULL
);