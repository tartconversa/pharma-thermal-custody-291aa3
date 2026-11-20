"""DuckDB 表结构。

设计要点：
- raw_*：原始行的完整接收历史（append-only），永不更新删除；同一业务键的多次整行
  修订通过 batch_seq 选择当前生效行。
- dim_* / fact_* / mart_* / mv_*：全部带 version_id 不可变快照，重建版本时
  未受影响的行从旧版本复制，保证一次查询属于同一版。
- review_refs：复核引用单独持久化，不随版本重建失效。
"""
from __future__ import annotations

SCHEMA_STATEMENTS = [
    # ---------- 元数据 ----------
    """
    CREATE TABLE IF NOT EXISTS wh_meta (
        key   VARCHAR PRIMARY KEY,
        value VARCHAR
    )
    """,
    # ---------- 接收批次与文件 ----------
    """
    CREATE TABLE IF NOT EXISTS ingest_batches (
        batch_id    VARCHAR PRIMARY KEY,
        received_at TIMESTAMP NOT NULL,
        note        VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ingest_files (
        file_id     VARCHAR PRIMARY KEY,
        batch_id    VARCHAR NOT NULL REFERENCES ingest_batches(batch_id),
        table_name  VARCHAR NOT NULL,
        file_name   VARCHAR NOT NULL,
        sha256      VARCHAR NOT NULL,
        byte_size   BIGINT NOT NULL,
        archived_at TIMESTAMP NOT NULL,
        row_count   INTEGER NOT NULL,
        outcome     VARCHAR NOT NULL  -- applied | duplicate
    )
    """,
    # ---------- 原始行历史（append-only，含来源文件） ----------
    """
    CREATE TABLE IF NOT EXISTS raw_rows (
        seq         BIGINT PRIMARY KEY,
        file_id     VARCHAR NOT NULL REFERENCES ingest_files(file_id),
        batch_id    VARCHAR NOT NULL,
        batch_seq   INTEGER NOT NULL,
        table_name  VARCHAR NOT NULL,
        record_key  VARCHAR NOT NULL,
        row_json    VARCHAR NOT NULL,
        received_at TIMESTAMP NOT NULL
    )
    """,
    # ---------- 版本 ----------
    """
    CREATE TABLE IF NOT EXISTS versions (
        version_id  INTEGER PRIMARY KEY,
        created_at  TIMESTAMP NOT NULL,
        received_at TIMESTAMP NOT NULL,
        basis       VARCHAR NOT NULL,          -- initial | correction | new_files
        batch_ids   VARCHAR[] NOT NULL,
        note        VARCHAR,
        affected_flows VARCHAR[]
    )
    """,
    # ---------- 维度：药批与支流 ----------
    """
    CREATE TABLE IF NOT EXISTS dim_lots (
        version_id INTEGER NOT NULL, lot_id VARCHAR, sku VARCHAR, quantity INTEGER
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_flows (
        version_id INTEGER NOT NULL, flow_id VARCHAR, lot_id VARCHAR,
        parent_id VARCHAR, quantity INTEGER, split_at TIMESTAMP
    )
    """,
    # ---------- 事实：交接（transfer 配对与争议） ----------
    """
    CREATE TABLE IF NOT EXISTS fact_handoffs (
        version_id INTEGER NOT NULL, transfer_id VARCHAR, flow_id VARCHAR,
        sent_receipt VARCHAR, received_receipt VARCHAR,
        sender VARCHAR, receiver VARCHAR,
        sent_at TIMESTAMP, received_at TIMESTAMP,
        effective_received_at TIMESTAMP,   -- 应用补正后的接管时点
        sent_qty INTEGER, received_qty INTEGER,
        matched_qty INTEGER, disputed_qty INTEGER,
        state VARCHAR,                     -- closed | sent_only | received_only | quantity_mismatch
        correction_id VARCHAR,             -- 兼容列：优先接管侧补正
        sent_correction_id VARCHAR,        -- 发出回执的补正
        received_correction_id VARCHAR,    -- 接收回执的补正
        pending_review BOOLEAN,            -- 补正时间与拆分实物时点矛盾
        pending_reason VARCHAR
    )
    """,
    # ---------- 事实：包装归属时间线（持有方，含在途空档） ----------
    """
    CREATE TABLE IF NOT EXISTS fact_ownership (
        version_id INTEGER NOT NULL, flow_id VARCHAR,
        seg_start TIMESTAMP, seg_end TIMESTAMP,
        holder VARCHAR,                    -- NULL 表示在途/无人确认接管
        basis VARCHAR                      -- initial | sent | received | flow_end
    )
    """,
    # ---------- 事实：责任区段（仅已确认接管区间） ----------
    """
    CREATE TABLE IF NOT EXISTS fact_responsibility (
        version_id INTEGER NOT NULL, flow_id VARCHAR,
        carrier_party VARCHAR, seg_start TIMESTAMP, seg_end TIMESTAMP,
        transfer_id VARCHAR, end_pending BOOLEAN
    )
    """,
    # ---------- 事实：仪器装载（版本快照） ----------
    """
    CREATE TABLE IF NOT EXISTS fact_loads (
        version_id INTEGER NOT NULL, load_id VARCHAR, logger_id VARCHAR,
        flow_id VARCHAR, load_start TIMESTAMP, load_end TIMESTAMP
    )
    """,
    # ---------- 事实：温控区间（同一份事实，质量与责任两种口径都从这里取） ----------
    """
    CREATE TABLE IF NOT EXISTS fact_temp_intervals (
        version_id INTEGER NOT NULL, flow_id VARCHAR, lot_id VARCHAR, sku VARCHAR,
        seg_start TIMESTAMP, seg_end TIMESTAMP,
        status VARCHAR,                    -- normal | excursion | conflict | unknown
        unknown_reason VARCHAR,            -- no_handoff | no_load | no_reading | no_rule | NULL
        loggers VARCHAR[],                 -- 该区间有有效读数覆盖的仪器
        evidence_json VARCHAR              -- 每台仪器的读数/单位/取值/判定，供追查
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_conflicts (
        version_id INTEGER NOT NULL, flow_id VARCHAR,
        conflict_start TIMESTAMP, conflict_end TIMESTAMP,
        loggers VARCHAR[], detail_json VARCHAR
    )
    """,
    # ---------- 集市：质量口径（按批与按支流，盒·秒，含单盒秒数） ----------
    """
    CREATE TABLE IF NOT EXISTS mart_quality_lot (
        version_id INTEGER NOT NULL, lot_id VARCHAR,
        normal_box_seconds BIGINT, excursion_box_seconds BIGINT,
        conflict_box_seconds BIGINT, unknown_box_seconds BIGINT,
        normal_seconds BIGINT, excursion_seconds BIGINT,
        conflict_seconds BIGINT, unknown_seconds BIGINT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS mart_quality_flow (
        version_id INTEGER NOT NULL, lot_id VARCHAR, flow_id VARCHAR, quantity INTEGER,
        normal_seconds BIGINT, excursion_seconds BIGINT,
        conflict_seconds BIGINT, unknown_seconds BIGINT,
        normal_box_seconds BIGINT, excursion_box_seconds BIGINT,
        conflict_box_seconds BIGINT, unknown_box_seconds BIGINT
    )
    """,
    # ---------- 集市：承运方责任口径 ----------
    """
    CREATE TABLE IF NOT EXISTS mart_carrier (
        version_id INTEGER NOT NULL, carrier_party VARCHAR,
        normal_box_seconds BIGINT, excursion_box_seconds BIGINT,
        conflict_box_seconds BIGINT, unknown_box_seconds BIGINT,
        normal_seconds BIGINT, excursion_seconds BIGINT,
        conflict_seconds BIGINT, unknown_seconds BIGINT
    )
    """,
    # ---------- 集市：跨午夜日集市（两种口径各一张，同一事实切日） ----------
    """
    CREATE TABLE IF NOT EXISTS mart_daily_quality (
        version_id INTEGER NOT NULL, day DATE, lot_id VARCHAR, flow_id VARCHAR,
        status VARCHAR, seconds BIGINT, box_seconds BIGINT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS mart_daily_carrier (
        version_id INTEGER NOT NULL, day DATE, carrier_party VARCHAR,
        flow_id VARCHAR, status VARCHAR, seconds BIGINT, box_seconds BIGINT
    )
    """,
    # ---------- 集市：当前可用量（证据完整、无冲突未知的量；不替代放行） ----------
    """
    CREATE TABLE IF NOT EXISTS mart_available (
        version_id INTEGER NOT NULL, lot_id VARCHAR, flow_id VARCHAR,
        flow_quantity INTEGER, available_quantity INTEGER,
        unknown_seconds BIGINT, excursion_seconds BIGINT, conflict_seconds BIGINT
    )
    """,
    # ---------- 复核引用（钉版） ----------
    """
    CREATE TABLE IF NOT EXISTS review_refs (
        ref_id VARCHAR PRIMARY KEY,
        version_id INTEGER NOT NULL,
        subject_type VARCHAR NOT NULL,      -- flow | lot | carrier | transfer
        subject_id VARCHAR NOT NULL,
        created_at TIMESTAMP NOT NULL,
        note VARCHAR
    )
    """,
]

# 版本化事实/集市/维度表：重建时未受影响行从旧版本复制
VERSIONED_TABLES = [
    "dim_lots",
    "dim_flows",
    "fact_handoffs",
    "fact_ownership",
    "fact_responsibility",
    "fact_loads",
    "fact_temp_intervals",
    "fact_conflicts",
    "mart_quality_lot",
    "mart_quality_flow",
    "mart_carrier",
    "mart_daily_quality",
    "mart_daily_carrier",
    "mart_available",
]

# 原始资料表名与业务键
RAW_TABLE_KEYS = {
    "lots": "lot_id",
    "flows": "flow_id",
    "handoffs": "receipt_id",
    "logger_loads": "load_id",
    "readings": "reading_id",
    "temperature_rules": "rule_id",
    "time_corrections": "correction_id",
}
RAW_TABLE_ORDER = list(RAW_TABLE_KEYS.keys())
