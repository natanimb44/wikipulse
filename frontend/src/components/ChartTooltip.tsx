interface Payload {
  name: string
  value: number
  color?: string
}

export function ChartTooltip({
  active,
  label,
  payload,
  valueLabel,
}: {
  active?: boolean
  label?: string
  payload?: Payload[]
  valueLabel?: string
}) {
  if (!active || !payload || payload.length === 0) return null

  return (
    <div className="chart-tooltip">
      <div className="chart-tooltip-label">{label}</div>
      {payload.map((p) => (
        <div className="chart-tooltip-row" key={p.name}>
          <span className="chart-tooltip-swatch" style={{ background: p.color }} />
          <span className="chart-tooltip-name">{valueLabel ?? p.name}</span>
          <span className="chart-tooltip-value">{p.value.toLocaleString()}</span>
        </div>
      ))}
    </div>
  )
}
