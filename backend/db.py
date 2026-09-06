import os
from contextlib import contextmanager

import psycopg2
import psycopg2.extras
from psycopg2.pool import SimpleConnectionPool

PG_HOST = os.environ.get("TIMESCALE_HOST", "timescaledb")
PG_PORT = os.environ.get("TIMESCALE_PORT", "5432")
PG_DB = os.environ.get("TIMESCALE_DB", "wikipulse")
PG_USER = os.environ.get("TIMESCALE_USER", "wikipulse")
PG_PASSWORD = os.environ.get("TIMESCALE_PASSWORD", "wikipulse")

_pool: SimpleConnectionPool | None = None


def init_pool():
    global _pool
    if _pool is None:
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
    finally:
        _pool.putconn(conn)


def healthcheck() -> bool:
    with get_cursor() as cur:
        cur.execute("SELECT 1")
        return cur.fetchone() is not None
