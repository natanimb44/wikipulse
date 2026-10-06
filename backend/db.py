import os
from contextlib import contextmanager

import psycopg2
import psycopg2.extras
from psycopg2.pool import SimpleConnectionPool

# Hosted (batch-mode) deployment passes one connection string; local docker
# compose still uses the separate TIMESCALE_* vars below.
DATABASE_URL = os.environ.get("DATABASE_URL")
PG_HOST = os.environ.get("TIMESCALE_HOST", "timescaledb")
PG_PORT = os.environ.get("TIMESCALE_PORT", "5432")
PG_DB = os.environ.get("TIMESCALE_DB", "wikipulse")
PG_USER = os.environ.get("TIMESCALE_USER", "wikipulse")
PG_PASSWORD = os.environ.get("TIMESCALE_PASSWORD", "wikipulse")

_pool: SimpleConnectionPool | None = None


def init_pool():
    global _pool
    if _pool is None:
        if DATABASE_URL:
            _pool = SimpleConnectionPool(1, 10, DATABASE_URL)
        else:
            _pool = SimpleConnectionPool(
                1, 10,
                host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD,
            )


@contextmanager
def get_cursor():
    init_pool()
    conn = _pool.getconn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            yield cur
        conn.commit()
    except psycopg2.OperationalError:
        # On serverless the instance can outlive its DB connection (the hosted
        # pooler drops idle ones). Throw it away instead of handing it out again.
        _pool.putconn(conn, close=True)
        raise
    except Exception:
        conn.rollback()
        _pool.putconn(conn)
        raise
    else:
        _pool.putconn(conn)


def healthcheck() -> bool:
    with get_cursor() as cur:
        cur.execute("SELECT 1")
        return cur.fetchone() is not None


# "Now" as far as the data is concerned: the end of the newest window. The hosted
# pipeline runs every 15 minutes, so "the last 10 minutes" means the last 10
# minutes of data, not wall-clock time (which would often be empty).
LATEST_WINDOW = "(SELECT coalesce(max(window_start) + interval '1 minute', now()) FROM window_stats)"
