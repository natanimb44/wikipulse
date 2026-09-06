import type { Anomaly } from '../api'

const SEVERITY_RANK: Record<Anomaly['severity'], number> = { high: 3, medium: 2, low: 1 }

// The lead story: highest severity first, most recent as the tiebreaker -
// an editor picking the day's top story, recomputed on every poll.
export function pickLead(anomalies: Anomaly[] | null): Anomaly | null {
  if (!anomalies || anomalies.length === 0) return null
  return [...anomalies].sort((a, b) => {
    const rank = SEVERITY_RANK[b.severity] - SEVERITY_RANK[a.severity]
    if (rank !== 0) return rank
    return new Date(b.detected_at).getTime() - new Date(a.detected_at).getTime()
  })[0]
}
