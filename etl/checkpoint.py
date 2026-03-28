"""
checkpoint.py
────────────────────────────────────────────────────────────────────────────────
Checkpoint-based pipeline resumability.

Stores step completion state in PostgreSQL so that a crashed or interrupted
pipeline run can resume from the last completed step instead of restarting
from scratch.

Schema (run once):
──────────────────
    CREATE TABLE IF NOT EXISTS pipeline_checkpoints (
        run_date     DATE        NOT NULL,
        order_type   TEXT        NOT NULL,
        step         TEXT        NOT NULL,
        status       TEXT        NOT NULL,   -- 'done' | 'failed'
        completed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        detail       TEXT,                   -- optional: blob name, row count, etc.
        PRIMARY KEY (run_date, order_type, step)
    );

Usage:
──────
    from checkpoint import Checkpoint

    cp = Checkpoint(run_date="2026-03-26", order_type="online")

    # Skip if already done, otherwise run and mark done
    cp.run("bronze_upload", bronze_upload_fn)

    # Check manually
    if cp.is_done("silver_write"):
        df = azure_staging.silver_read(date_str)
    else:
        df = do_silver_write(...)
        cp.mark_done("silver_write", detail=silver_blob)

    # Clear all checkpoints for a date (force full re-run)
    cp.clear()
────────────────────────────────────────────────────────────────────────────────
"""

import os
import psycopg2
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


_DB_CONFIG = {
    "host":     _require_env("DB_HOST"),
    "port":     int(os.getenv("DB_PORT", 5432)),
    "database": _require_env("DB_NAME"),
    "user":     _require_env("DB_USER"),
    "password": _require_env("DB_PASSWORD"),
}

# All pipeline steps in execution order — used for clear() and status display
PIPELINE_STEPS = [
    "bronze_upload",
    "bronze_download",
    "schema_validation",
    "transform",
    "silver_write",
    "billing_data_load",
    "aggregate_load",
    "gold_write",
]


def ensure_checkpoint_table():
    """
    Create the pipeline_checkpoints table if it does not exist.
    Safe to call on every pipeline start — idempotent.
    """
    conn = psycopg2.connect(**_DB_CONFIG)
    cur  = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_checkpoints (
            run_date     DATE        NOT NULL,
            order_type   TEXT        NOT NULL,
            step         TEXT        NOT NULL,
            status       TEXT        NOT NULL,
            completed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            detail       TEXT,
            PRIMARY KEY (run_date, order_type, step)
        )
    """)
    conn.commit()
    cur.close()
    conn.close()


class Checkpoint:
    """
    Manages step-level checkpoints for a single pipeline run
    identified by (run_date, order_type).
    """

    def __init__(self, run_date: str, order_type: str):
        """
        Parameters
        ----------
        run_date   : ISO date string e.g. '2026-03-26'
        order_type : 'online' or 'offline'
        """
        self.run_date   = run_date
        self.order_type = order_type
        ensure_checkpoint_table()
        self._print_status()

    # ── Public API ────────────────────────────────────────────────────────────

    def is_done(self, step: str) -> bool:
        """Return True if this step completed successfully in a prior run."""
        conn = psycopg2.connect(**_DB_CONFIG)
        cur  = conn.cursor()
        cur.execute("""
            SELECT 1 FROM pipeline_checkpoints
            WHERE run_date = %s AND order_type = %s AND step = %s AND status = 'done'
        """, (self.run_date, self.order_type, step))
        result = cur.fetchone() is not None
        cur.close()
        conn.close()
        return result

    def mark_done(self, step: str, detail: str = None):
        """Record a step as successfully completed."""
        self._upsert(step, "done", detail)
        print(f"  ✓ Checkpoint saved: {step}")

    def mark_failed(self, step: str, detail: str = None):
        """Record a step as failed (for observability — does not block re-run)."""
        self._upsert(step, "failed", detail)
        print(f"  ✗ Checkpoint failed: {step}")

    def run(self, step: str, fn, detail_fn=None):
        """
        Run fn() only if step is not already done.
        Marks done on success, failed on exception (then re-raises).

        Parameters
        ----------
        step      : Step name from PIPELINE_STEPS
        fn        : Zero-argument callable to execute
        detail_fn : Optional callable that receives fn()'s return value
                    and returns a string to store as the checkpoint detail.

        Returns
        -------
        The return value of fn(), or None if the step was skipped.
        """
        if self.is_done(step):
            print(f"  ⏭  Skipping '{step}' — already completed in prior run.")
            return None
        try:
            result = fn()
            detail = detail_fn(result) if detail_fn and result is not None else None
            self.mark_done(step, detail)
            return result
        except Exception as exc:
            self.mark_failed(step, str(exc)[:500])
            raise

    def clear(self, steps: list = None):
        """
        Delete checkpoints so the pipeline re-runs those steps.

        Parameters
        ----------
        steps : List of step names to clear. Clears ALL steps if None.
        """
        conn = psycopg2.connect(**_DB_CONFIG)
        cur  = conn.cursor()
        if steps:
            placeholders = ",".join(["%s"] * len(steps))
            cur.execute(
                f"DELETE FROM pipeline_checkpoints "
                f"WHERE run_date=%s AND order_type=%s AND step IN ({placeholders})",
                [self.run_date, self.order_type] + steps
            )
        else:
            cur.execute(
                "DELETE FROM pipeline_checkpoints WHERE run_date=%s AND order_type=%s",
                (self.run_date, self.order_type)
            )
        conn.commit()
        cleared = cur.rowcount
        cur.close()
        conn.close()
        print(f"  Cleared {cleared} checkpoint(s) for {self.run_date}/{self.order_type}")

    def status(self) -> dict:
        """Return a dict of step → status for this run."""
        conn = psycopg2.connect(**_DB_CONFIG)
        cur  = conn.cursor()
        cur.execute("""
            SELECT step, status, completed_at, detail
            FROM pipeline_checkpoints
            WHERE run_date = %s AND order_type = %s
            ORDER BY completed_at
        """, (self.run_date, self.order_type))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return {row[0]: {"status": row[1], "at": row[2], "detail": row[3]} for row in rows}

    # ── Internal ──────────────────────────────────────────────────────────────

    def _upsert(self, step: str, status: str, detail: str = None):
        conn = psycopg2.connect(**_DB_CONFIG)
        cur  = conn.cursor()
        cur.execute("""
            INSERT INTO pipeline_checkpoints (run_date, order_type, step, status, completed_at, detail)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_date, order_type, step)
            DO UPDATE SET status=%s, completed_at=%s, detail=%s
        """, (
            self.run_date, self.order_type, step, status,
            datetime.now(timezone.utc), detail,
            status, datetime.now(timezone.utc), detail
        ))
        conn.commit()
        cur.close()
        conn.close()

    def _print_status(self):
        current = self.status()
        if not current:
            print(f"\n  [Checkpoint] Fresh run — no prior steps found for "
                  f"{self.run_date}/{self.order_type}")
            return
        done  = [s for s, v in current.items() if v["status"] == "done"]
        failed = [s for s, v in current.items() if v["status"] == "failed"]
        print(f"\n  [Checkpoint] Resuming {self.run_date}/{self.order_type}")
        print(f"    Done   : {done}")
        if failed:
            print(f"    Failed : {failed}")
        pending = [s for s in PIPELINE_STEPS if s not in current]
        print(f"    Pending: {pending}")