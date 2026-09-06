import {
  Area,
  AreaChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import type { GlobalStat } from '../api'
import { ChartTooltip } from './ChartTooltip'

export function GlobalPulseChart({ stats, error }: { stats: GlobalStat[] | null; error: string | null }) {
  const data = (stats ?? []).map((s) => ({
    time: new Date(s.window_start).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
    edits: s.edit_count,
  }))

  return (
    <div>
      <p className="panel-subtitle" style={{ marginBottom: '0.85rem' }}>
        Edits per minute, English Wikipedia
      </p>
      {error ? (
        <p className="error">{error}</p>
      ) : (
        <ResponsiveContainer width="100%" height={200}>
          <AreaChart data={data} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
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
              width={36}
              allowDecimals={false}
            />
            <Tooltip content={<ChartTooltip valueLabel="edits" />} />
            <Area
              type="monotone"
              dataKey="edits"
              stroke="var(--chart-line)"
              strokeWidth={2}
              fill="var(--chart-fill)"
              activeDot={{ r: 4, stroke: 'var(--chart-surface)', strokeWidth: 2 }}
            />
          </AreaChart>
        </ResponsiveContainer>
      )}
    </div>
  )
}
