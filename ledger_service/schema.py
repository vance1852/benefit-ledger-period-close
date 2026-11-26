"""账本服务在原始资料表之外使用的扩展结构。

原始的 account / period / opening_entries / source_cuts 表保持不动；本模块只创建
``le_`` 前缀的新表。这样一份装载好的 SQLite 资料可以直接升级为可写账本库。
"""

SCHEMA_VERSION = "1"

LEDGER_SCHEMA = """
PRAGMA foreign_keys = ON;

-- 以下四张是仓库资料中的原始关系表。使用 IF NOT EXISTS，因此装载 fixture
-- 后不会覆盖合成数据；直接对空库初始化时也能得到最小可用结构。
CREATE TABLE IF NOT EXISTS account (
    id TEXT PRIMARY KEY,
    org TEXT,
    kind TEXT,
    currency TEXT
);
CREATE TABLE IF NOT EXISTS period (
    org TEXT NOT NULL,
    month TEXT NOT NULL,
    state TEXT,
    actor TEXT,
    PRIMARY KEY(org, month)
);
CREATE TABLE IF NOT EXISTS opening_entries (
    voucher TEXT,
    month TEXT,
    account TEXT,
    amount INTEGER,
    currency TEXT
);
CREATE TABLE IF NOT EXISTS source_cuts (
    source TEXT PRIMARY KEY,
    final_through INTEGER
);

CREATE TABLE IF NOT EXISTS le_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS le_voucher (
    voucher_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    source TEXT,
    source_seq INTEGER,
    idempotency_key TEXT UNIQUE,
    fingerprint TEXT NOT NULL,
    requested_month TEXT,
    effective_month TEXT NOT NULL,
    currency TEXT NOT NULL,
    original_voucher_id TEXT REFERENCES le_voucher(voucher_id),
    period_policy TEXT NOT NULL DEFAULT 'error',
    status TEXT NOT NULL DEFAULT 'posted',
    chain_seq INTEGER UNIQUE,
    prev_hash TEXT NOT NULL,
    voucher_hash TEXT NOT NULL UNIQUE,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    UNIQUE(source, source_seq)
);

CREATE TABLE IF NOT EXISTS le_entry (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    voucher_id TEXT NOT NULL REFERENCES le_voucher(voucher_id),
    line_no INTEGER NOT NULL,
    account TEXT NOT NULL REFERENCES account(id),
    org TEXT NOT NULL,
    month TEXT NOT NULL,
    currency TEXT NOT NULL,
    amount INTEGER NOT NULL,
    UNIQUE(voucher_id, line_no)
);

CREATE INDEX IF NOT EXISTS idx_le_entry_period_account
    ON le_entry(org, month, account);
CREATE INDEX IF NOT EXISTS idx_le_entry_voucher ON le_entry(voucher_id);

CREATE TABLE IF NOT EXISTS le_request (
    source TEXT NOT NULL,
    source_seq INTEGER NOT NULL,
    request_id TEXT NOT NULL UNIQUE,
    month TEXT NOT NULL,
    currency TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL,
    error_code TEXT,
    error_message TEXT,
    error_details_json TEXT,
    voucher_id TEXT REFERENCES le_voucher(voucher_id),
    received_count INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY(source, source_seq)
);

CREATE INDEX IF NOT EXISTS idx_le_request_status ON le_request(status);

CREATE TABLE IF NOT EXISTS le_report (
    report_id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL UNIQUE REFERENCES le_request(request_id),
    outcome TEXT NOT NULL DEFAULT 'cleared',
    fingerprint TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS le_refund (
    refund_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL DEFAULT 'refund',
    original_voucher_id TEXT NOT NULL REFERENCES le_voucher(voucher_id),
    amount INTEGER NOT NULL CHECK(amount >= 0),
    currency TEXT NOT NULL,
    requested_month TEXT NOT NULL,
    effective_month TEXT,
    fingerprint TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL,
    error_code TEXT,
    error_message TEXT,
    error_details_json TEXT,
    voucher_id TEXT UNIQUE REFERENCES le_voucher(voucher_id),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_le_refund_original
    ON le_refund(original_voucher_id, status);

CREATE TABLE IF NOT EXISTS le_refund_line (
    refund_id TEXT NOT NULL REFERENCES le_refund(refund_id),
    line_no INTEGER NOT NULL,
    original_line_id INTEGER REFERENCES le_entry(id),
    account TEXT NOT NULL,
    org TEXT NOT NULL,
    month TEXT NOT NULL,
    currency TEXT NOT NULL,
    amount INTEGER NOT NULL,
    PRIMARY KEY(refund_id, line_no)
);

CREATE TABLE IF NOT EXISTS le_close_job (
    org TEXT NOT NULL,
    month TEXT NOT NULL,
    actor TEXT NOT NULL,
    close_key TEXT NOT NULL UNIQUE,
    cuts_json TEXT NOT NULL,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    certificate_id TEXT,
    result_json TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY(org, month)
);

CREATE TABLE IF NOT EXISTS le_close_certificate (
    certificate_id TEXT PRIMARY KEY,
    org TEXT NOT NULL,
    month TEXT NOT NULL,
    actor TEXT NOT NULL,
    close_key TEXT NOT NULL,
    cuts_json TEXT NOT NULL,
    snapshot_json TEXT NOT NULL,
    certificate_hash TEXT NOT NULL UNIQUE,
    credential_token TEXT NOT NULL UNIQUE,
    chain_seq INTEGER,
    chain_tip TEXT,
    issued_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    UNIQUE(org, month)
);

CREATE TABLE IF NOT EXISTS le_delivery_event (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ref_type TEXT NOT NULL,
    ref_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    detail_json TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS le_operation (
    operation_key TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    status TEXT NOT NULL,
    response_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
"""
