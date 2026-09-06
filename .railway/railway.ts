// WARNING: `railway config plan` will always show one expected diff -
// "Update producer deploy.restartPolicyType (ALWAYS -> null)" - because this
// IaC helper has no field for restart policy. That live ALWAYS setting (set
// via the Railway API, see the comment on `producer` below) is intentional
// and load-bearing. Do NOT run `railway config apply` for that diff alone;
// if you apply this file for an unrelated change, immediately re-set
// restartPolicyType back to ALWAYS on the producer service afterward.
import { defineRailway, image, preserve, project, service, volume } from "railway/iac";

export default defineRailway(() => {
  const timescaleData = volume("timescale-data", { alerts: { usage: { "100": {}, "80": {}, "95": {} } }, allowOnlineResize: true, region: "ams", sizeMB: 500 });
  const timescaledb = service("timescaledb", {
    source: image("timescale/timescaledb:2.15.3-pg15"),
    replicas: { "ams": 1 },
    volumeMounts: { "/var/lib/postgresql/railway-data": timescaleData },
    env: { PGDATA: preserve(), POSTGRES_DB: preserve(), POSTGRES_PASSWORD: preserve(), POSTGRES_USER: preserve() },
  });
  const redpanda = service("redpanda", {
    source: image("docker.redpanda.com/redpandadata/redpanda:v24.2.18"),
    start: "/entrypoint.sh redpanda start --smp=1 --memory=512M --overprovisioned --node-id=0 --kafka-addr=PLAINTEXT://0.0.0.0:9092 --advertise-kafka-addr=PLAINTEXT://redpanda.railway.internal:9092",
    replicas: { "ams": 1 },
  });
  // restartPolicyType is set to ALWAYS on this service via the Railway API/dashboard
  // (not exposed through this IaC helper) so it self-heals from the memory-pressure
  // restarts documented in the README, instead of giving up after 10 retries.
  const producer = service("producer", {
    replicas: { "ams": 1 },
    env: { EWMA_ALPHA: preserve(), KAFKA_BROKERS: preserve(), REVERT_RATIO_THRESHOLD: preserve(), TIMESCALE_DB: preserve(), TIMESCALE_HOST: preserve(), TIMESCALE_PASSWORD: preserve(), TIMESCALE_PORT: preserve(), TIMESCALE_USER: preserve(), WIKI_EDITS_TOPIC: preserve(), Z_THRESHOLD: preserve() },
  });
  const backend = service("backend", {
    replicas: { "ams": 1 },
    env: { PORT: preserve(), TIMESCALE_DB: preserve(), TIMESCALE_HOST: preserve(), TIMESCALE_PASSWORD: preserve(), TIMESCALE_PORT: preserve(), TIMESCALE_USER: preserve() },
  });

  return project("wikipulse", {
    resources: [timescaledb, redpanda, producer, backend, timescaleData],
  });
});
