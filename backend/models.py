from datetime import datetime

from pydantic import BaseModel


class Anomaly(BaseModel):
    id: int
    detected_at: datetime
    window_start: datetime
    entity_type: str
    entity_key: str
    metric: str
    value: float
    baseline: float
    z_score: float
    severity: str


class WindowStat(BaseModel):
    window_start: datetime
    window_end: datetime
    entity_type: str
    entity_key: str
    edit_count: int
    revert_count: int
    anon_ratio: float | None
    baseline_ewma: float | None
    z_score: float | None


class GlobalStat(BaseModel):
    window_start: datetime
    edit_count: int


class TrendingPage(BaseModel):
    entity_key: str
    edit_count: int
    revert_count: int
    anon_ratio: float | None
