import { api, wikiPageUrl, type TrendingPage } from '../api'
import { usePolling } from '../hooks/usePolling'

const POLL_INTERVAL_MS = 15000

export function TrendingPages() {
  const { data: pages, error } = usePolling(() => api.trendingPages(10, 8), POLL_INTERVAL_MS)

  return (
    <div>
      <p className="panel-subtitle" style={{ marginBottom: '0.75rem' }}>
        Last 10 minutes
      </p>
      {error && <p className="error">{error}</p>}
      {pages && pages.length === 0 && !error && <p className="muted empty-state">No activity yet.</p>}
      <ul className="trending-list">
        {(pages ?? []).map((p, i) => (
          <li key={p.entity_key} className="trending-item">
            <span className="trending-rank">{String(i + 1).padStart(2, '0')}</span>
            <div className="trending-body">
              <div className="trending-row">
                <a className="trending-title" href={wikiPageUrl(p.entity_key)} target="_blank" rel="noreferrer">
                  {p.entity_key}
                </a>
                <span className="trending-count">
                  {p.edit_count} edit{p.edit_count === 1 ? '' : 's'}
                </span>
              </div>
              <TrendingMeta meta={p} />
            </div>
          </li>
        ))}
      </ul>
    </div>
  )
}

function TrendingMeta({ meta }: { meta: TrendingPage }) {
  const bits: string[] = []
  if (meta.revert_count > 0) bits.push(`${meta.revert_count} revert${meta.revert_count === 1 ? '' : 's'}`)
  if (meta.anon_ratio !== null && meta.anon_ratio > 0) bits.push(`${Math.round(meta.anon_ratio * 100)}% anonymous`)
  if (bits.length === 0) return null
  return <div className="trending-meta">{bits.join(' · ')}</div>
}
