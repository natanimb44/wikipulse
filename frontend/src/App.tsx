import { useMemo, useState } from 'react'
import './App.css'
import { api, type Anomaly } from './api'
import { ByTheNumbers } from './components/ByTheNumbers'
import { GlobalPulseChart } from './components/GlobalPulseChart'
import { LeadStory } from './components/LeadStory'
import { TrendingPages } from './components/TrendingPages'
import { WireBriefs } from './components/WireBriefs'
import { usePolling } from './hooks/usePolling'
import { pickLead } from './lib/anomaly'
import { isWithinMinutes } from './lib/format'

const DATELINE = new Date().toLocaleDateString('en-US', {
  weekday: 'long',
  year: 'numeric',
  month: 'long',
  day: 'numeric',
})

function App() {
  const [pinned, setPinned] = useState<Anomaly | null>(null)

  const { data: stats, error: statsError } = usePolling(() => api.globalStats(60), 10000)
  const { data: anomalies, error: anomaliesError } = usePolling(() => api.recentAnomalies(50), 7000)

  const lead = pinned ?? pickLead(anomalies)
  const briefs = useMemo(() => (anomalies ?? []).filter((a) => a.id !== lead?.id), [anomalies, lead])

  const { currentRate, anomalyCount1h, highSeverityCount } = useMemo(() => {
    const currentRate = stats && stats.length > 0 ? stats[stats.length - 1].edit_count : null
    const lastHour = (anomalies ?? []).filter((a) => isWithinMinutes(a.detected_at, 60))
    return {
      currentRate,
      anomalyCount1h: lastHour.length,
      highSeverityCount: lastHour.filter((a) => a.severity === 'high').length,
    }
  }, [stats, anomalies])

  return (
    <div className="app">
      <div className="spine" />

      <div className="masthead-row">
        <span className="brand-mark" />
        <h1 className="brand">WikiPulse</h1>
      </div>
      <div className="masthead-meta">
        <span>Real-time edit-anomaly monitor &middot; en.wikipedia.org</span>
        <span>{DATELINE}</span>
        <span className="live-indicator">
          <span className="live-dot" />
          Live
        </span>
      </div>
      <div className="rule-heavy" />

      <main className="front-page">
        <div className="lead-col">
          <div className="section-head">
            <span className="section-index">01</span>
            <h2>Top anomaly</h2>
          </div>
          <LeadStory anomaly={lead} isPinned={pinned !== null} onReset={() => setPinned(null)} />

          <div className="lead-col-divider" />

          <div className="section-head">
            <span className="section-index">02</span>
            <h2>Global pulse</h2>
          </div>
          <GlobalPulseChart stats={stats} error={statsError} />
        </div>

        <aside className="sidebar-col">
          <div>
            <div className="section-head">
              <span className="section-index">03</span>
              <h2>By the numbers</h2>
            </div>
            <ByTheNumbers
              currentRate={currentRate}
              anomalyCount1h={anomalyCount1h}
              highSeverityCount={highSeverityCount}
            />
          </div>

          <div>
            <div className="section-head">
              <span className="section-index">04</span>
              <h2>Most edited</h2>
            </div>
            <TrendingPages />
          </div>

          <div>
            <div className="section-head">
              <span className="section-index">05</span>
              <h2>In brief</h2>
            </div>
            <WireBriefs anomalies={briefs} error={anomaliesError} onPromote={setPinned} />
          </div>
        </aside>
      </main>
    </div>
  )
}

export default App
