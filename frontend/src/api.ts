const API_BASE = import.meta.env.VITE_API_BASE_URL || '/api'

export interface Anomaly {
  id: number
  detected_at: string
  window_start: string
  entity_type: 'page' | 'editor'
  entity_key: string
  metric: string
  value: number
  baseline: number
  z_score: number
  severity: 'low' | 'medium' | 'high'
}

export interface WindowStat {
  window_start: string
  window_end: string
  entity_type: string
  entity_key: string
  edit_count: number
  revert_count: number
  anon_ratio: number | null
  baseline_ewma: number | null
  z_score: number | null
}

export interface GlobalStat {
  window_start: string
  edit_count: number
}

export interface TrendingPage {
  entity_key: string
  edit_count: number
  revert_count: number
  anon_ratio: number | null
}

async function getJson<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`)
  if (!res.ok) {
    throw new Error(`request to ${path} failed: ${res.status}`)
  }
  return res.json() as Promise<T>
}

export const api = {
  recentAnomalies: (limit = 50) => getJson<Anomaly[]>(`/anomalies/recent?limit=${limit}`),
  anomaliesBySeverity: (level: string, limit = 50) =>
    getJson<Anomaly[]>(`/anomalies/severity/${level}?limit=${limit}`),
  pageStats: (title: string, minutes = 60) =>
    getJson<WindowStat[]>(`/stats/page/${encodeURIComponent(title)}?minutes=${minutes}`),
  editorStats: (username: string, minutes = 60) =>
    getJson<WindowStat[]>(`/stats/editor/${encodeURIComponent(username)}?minutes=${minutes}`),
  globalStats: (minutes = 60) => getJson<GlobalStat[]>(`/stats/global?minutes=${minutes}`),
  trendingPages: (minutes = 10, limit = 8) =>
    getJson<TrendingPage[]>(`/stats/trending?minutes=${minutes}&limit=${limit}`),
  health: () => getJson<{ status: string }>(`/health`),
}

export interface WikiSummary {
  title: string
  description: string | null
  extract: string
  thumbnailUrl: string | null
  pageUrl: string
}

// Wikipedia's public REST summary API - CORS-enabled, no key needed. Used to
// enrich an anomaly's page with real article content (thumbnail + blurb).
export async function fetchWikiSummary(title: string): Promise<WikiSummary | null> {
  const res = await fetch(`https://en.wikipedia.org/api/rest_v1/page/summary/${encodeURIComponent(title)}`)
  if (!res.ok) return null
  const data = await res.json()
  return {
    title: data.title,
    description: data.description ?? null,
    extract: data.extract,
    thumbnailUrl: data.thumbnail?.source ?? null,
    pageUrl: data.content_urls?.desktop?.page ?? `https://en.wikipedia.org/wiki/${encodeURIComponent(title)}`,
  }
}

export function wikiPageUrl(title: string): string {
  return `https://en.wikipedia.org/wiki/${encodeURIComponent(title.replace(/ /g, '_'))}`
}

export function wikiContribsUrl(username: string): string {
  return `https://en.wikipedia.org/wiki/Special:Contributions/${encodeURIComponent(username)}`
}
