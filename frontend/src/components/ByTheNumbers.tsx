import { formatCompact } from '../lib/format'

export function ByTheNumbers({
  currentRate,
  anomalyCount1h,
  highSeverityCount,
}: {
  currentRate: number | null
  anomalyCount1h: number
  highSeverityCount: number
}) {
  return (
    <div className="numeral-row">
      <div className="numeral-cell">
        <span className="numeral-value">{currentRate === null ? 'N/A' : formatCompact(currentRate)}</span>
        <span className="numeral-label">Edits / minute</span>
      </div>
      <div className="numeral-cell">
        <span className="numeral-value">{formatCompact(anomalyCount1h)}</span>
        <span className="numeral-label">Anomalies, 1h</span>
      </div>
      <div className="numeral-cell">
        <span className={`numeral-value${highSeverityCount > 0 ? ' numeral-value-alert' : ''}`}>
          {formatCompact(highSeverityCount)}
        </span>
        <span className="numeral-label">High severity, 1h</span>
      </div>
    </div>
  )
}
