export function formatCompact(n: number): string {
  return new Intl.NumberFormat('en', { notation: 'compact', maximumFractionDigits: 1 }).format(n)
}

import type { Anomaly } from '../api'

export function describeAnomaly(a: Anomaly): string {
  if (a.metric === 'revert_rate') {
    return `${Math.round(a.value * 100)}% of edits reverted (>${Math.round(a.baseline * 100)}% threshold)`
  }
  const baseline = a.baseline < 1 ? a.baseline.toFixed(1) : Math.round(a.baseline).toString()
  return `${Math.round(a.value)} edits this minute vs. usual ~${baseline}`
}

export function isWithinMinutes(iso: string, minutes: number): boolean {
  return Date.now() - new Date(iso).getTime() <= minutes * 60_000
}

export function timeAgo(iso: string): string {
  const deltaSec = Math.max(0, (Date.now() - new Date(iso).getTime()) / 1000)
  if (deltaSec < 60) return 'just now'
  const min = Math.floor(deltaSec / 60)
  if (min < 60) return `${min}m ago`
  const hr = Math.floor(min / 60)
  return `${hr}h ago`
}
