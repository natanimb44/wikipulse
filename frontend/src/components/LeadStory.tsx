import { useEffect, useState } from 'react'
import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import {
  api,
  fetchWikiSummary,
  wikiContribsUrl,
  wikiPageUrl,
  type Anomaly,
  type WikiSummary,
  type WindowStat,
} from '../api'
import { describeAnomaly, timeAgo } from '../lib/format'
import { ChartTooltip } from './ChartTooltip'

function eyebrowFor(a: Anomaly): string {
  const severity = `${a.severity} severity`
  if (a.metric === 'revert_rate') {
    return `Edit war signal, ${severity}`
  }
  const direction = a.value < a.baseline ? 'Activity drop' : 'Activity surge'
  return `${direction}, ${severity}`
}

export function LeadStory({
  anomaly,
  isPinned,
  onReset,
}: {
  anomaly: Anomaly | null
  isPinned: boolean
  onReset: () => void
}) {
  const [stats, setStats] = useState<WindowStat[]>([])
  const [error, setError] = useState<string | null>(null)
  const [summary, setSummary] = useState<WikiSummary | null>(null)

  useEffect(() => {
    setSummary(null)
    setError(null)
    setStats([])
    if (!anomaly) return
    let cancelled = false

    const fetchStats = anomaly.entity_type === 'page' ? api.pageStats : api.editorStats
    fetchStats(anomaly.entity_key, 120)
      .then((data) => !cancelled && setStats(data))
      .catch((e) => !cancelled && setError((e as Error).message))

    if (anomaly.entity_type === 'page') {
      fetchWikiSummary(anomaly.entity_key)
        .then((s) => !cancelled && setSummary(s))
        .catch(() => {
          /* nice-to-have; fail quietly */
        })
    }
    return () => {
      cancelled = true
    }
  }, [anomaly])

  if (!anomaly) {
    return (
      <section className="lead-story">
        <p className="lead-eyebrow">Standby</p>
        <h2 className="lead-headline">Watching the feed</h2>
        <p className="lead-dek">No anomalies detected yet this session. The global pulse below is still live.</p>
      </section>
    )
  }

  const data = stats.map((s) => ({
    time: new Date(s.window_start).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
    edits: s.edit_count,
    baseline: s.baseline_ewma,
  }))
  const latest = stats[stats.length - 1]
  const entityHref =
    anomaly.entity_type === 'page' ? wikiPageUrl(anomaly.entity_key) : wikiContribsUrl(anomaly.entity_key)

  return (
    <section className="lead-story">
      <div className="panel-header">
        <p className={`lead-eyebrow lead-eyebrow-${anomaly.severity}`}>{eyebrowFor(anomaly)}</p>
        {isPinned && (
          <button className="reset-link" onClick={onReset}>
            Reset to top story
          </button>
        )}
      </div>
      <h2 className="lead-headline">
        <a href={entityHref} target="_blank" rel="noreferrer">
          {anomaly.entity_key}
        </a>
      </h2>
      <p className="lead-byline">
        {anomaly.entity_type} &middot; z&nbsp;{anomaly.z_score.toFixed(2)} &middot; {timeAgo(anomaly.detected_at)}
      </p>

      <div className={`lead-grid${summary?.thumbnailUrl ? ' has-thumb' : ''}`}>
        {summary?.thumbnailUrl && <img className="lead-thumb" src={summary.thumbnailUrl} alt="" />}
        <p className="lead-dek">{summary?.extract ?? describeAnomaly(anomaly)}</p>
      </div>

      {latest && (
        <div className="detail-grid">
          <DetailStat label="Edits this window" value={latest.edit_count.toString()} />
          <DetailStat label="Reverts" value={latest.revert_count.toString()} />
          <DetailStat
            label="Anonymous"
            value={latest.anon_ratio === null ? 'N/A' : `${Math.round(latest.anon_ratio * 100)}%`}
          />
          <DetailStat label="Z-score" value={anomaly.z_score.toFixed(2)} />
        </div>
      )}

      {error ? (
        <p className="error">{error}</p>
      ) : (
        <ResponsiveContainer width="100%" height={220}>
          <LineChart data={data} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
            <CartesianGrid vertical={false} stroke="var(--chart-grid)" />
            <XAxis
              dataKey="time"
              tick={{ fontSize: 11, fill: 'var(--chart-muted)', fontFamily: 'var(--font)' }}
              tickLine={false}
              axisLine={{ stroke: 'var(--chart-axis)' }}
              minTickGap={48}
            />
            <YAxis
              tick={{ fontSize: 11, fill: 'var(--chart-muted)', fontFamily: 'var(--font)' }}
              tickLine={false}
              axisLine={false}
              width={32}
              allowDecimals={false}
            />
            <Tooltip content={<ChartTooltip />} />
            <Legend
              verticalAlign="bottom"
              align="left"
              height={28}
              iconType="plainline"
              wrapperStyle={{ fontSize: 12, fontWeight: 600, color: 'var(--ink-secondary)' }}
            />
            <Line
              type="monotone"
              dataKey="edits"
              name="Edit count"
              stroke="var(--chart-line)"
              strokeWidth={2}
              dot={false}
              activeDot={{ r: 4, stroke: 'var(--chart-surface)', strokeWidth: 2 }}
            />
            <Line
              type="monotone"
              dataKey="baseline"
              name="Baseline (EWMA)"
              stroke="var(--chart-muted)"
              strokeWidth={2}
              strokeDasharray="5 4"
              dot={false}
              activeDot={{ r: 4, stroke: 'var(--chart-surface)', strokeWidth: 2 }}
            />
          </LineChart>
        </ResponsiveContainer>
      )}
    </section>
  )
}

function DetailStat({ label, value }: { label: string; value: string }) {
  return (
    <div className="detail-stat">
      <span className="detail-stat-value">{value}</span>
      <span className="detail-stat-label">{label}</span>
    </div>
  )
}
