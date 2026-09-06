import type { Anomaly } from '../api'

export function SeverityBadge({ severity }: { severity: Anomaly['severity'] }) {
  return <span className={`severity-badge severity-${severity}`}>{severity}</span>
}
