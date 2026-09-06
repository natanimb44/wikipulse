import { useMemo, useState } from 'react'
import { wikiContribsUrl, wikiPageUrl, type Anomaly } from '../api'
import { timeAgo } from '../lib/format'

const FILTERS = ['all', 'high', 'medium', 'low'] as const
type Filter = (typeof FILTERS)[number]

export function WireBriefs({
  anomalies,
  error,
  onPromote,
}: {
  anomalies: Anomaly[] | null
  error: string | null
  onPromote: (a: Anomaly) => void
}) {
  const [filter, setFilter] = useState<Filter>('all')

  const filtered = useMemo(() => {
    if (!anomalies) return []
    return filter === 'all' ? anomalies : anomalies.filter((a) => a.severity === filter)
  }, [anomalies, filter])

  return (
    <div>
      <div className="filter-tabs" role="tablist" aria-label="Filter by severity" style={{ marginBottom: '0.75rem' }}>
        {FILTERS.map((f) => (
          <button
            key={f}
            role="tab"
            aria-selected={filter === f}
            className={`filter-tab${filter === f ? ' active' : ''}`}
            onClick={() => setFilter(f)}
          >
            {f}
          </button>
        ))}
      </div>

      {error && <p className="error">{error}</p>}
      {anomalies && filtered.length === 0 && !error && (
        <p className="muted empty-state">
          {filter === 'all' ? 'Nothing else flagged right now.' : `No ${filter}-severity items right now.`}
        </p>
      )}

      {filtered.length > 0 && (
        <div className="brief-table-scroll">
          <table className="brief-table">
            <thead>
              <tr>
                <th>Severity</th>
                <th>Item</th>
                <th>Time</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map((a) => (
                <tr key={a.id} onClick={() => onPromote(a)}>
                  <td>
                    <span className={`severity-badge severity-${a.severity}`}>{a.severity}</span>
                  </td>
                  <td>
                    <span className="brief-tag">{a.entity_type}</span>
                    <span className="brief-title">
                      {a.entity_key}
                      <a
                        className="external-link"
                        href={a.entity_type === 'page' ? wikiPageUrl(a.entity_key) : wikiContribsUrl(a.entity_key)}
                        target="_blank"
                        rel="noreferrer"
                        onClick={(e) => e.stopPropagation()}
                        title={a.entity_type === 'page' ? 'Open article on Wikipedia' : "Open editor's contributions"}
                      >
                        ↗
                      </a>
                    </span>
                  </td>
                  <td className="brief-time">{timeAgo(a.detected_at)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
