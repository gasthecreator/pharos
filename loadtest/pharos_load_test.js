// Pharos load test (PLAN.md Slice 16: Load testing).
//
// Two scenarios run concurrently against real Central Ingestion, real
// Kafka, and real Cassandra (no mocks) -- exactly this project's existing
// verification discipline, just under load instead of a single request:
//
//   steady_state: 9 sites each submitting at a modest, realistic constant
//   rate (well under the per-site rate limiter's 10 tokens/sec sustained
//   refill -- see internal/ratelimit and cmd/pharos-ingestion's
//   --rate-limit-capacity/--rate-limit-refill flags), for the whole test
//   duration.
//
//   burst_site: a single, separate site stays idle for the first 30s, then
//   for 15s receives a genuine 30 req/s *arrival rate* (constant-arrival-rate,
//   not a sequential loop -- a sequential single VU's requests are paced by
//   real backend latency to well under the rate limit and never actually
//   trip it, which is itself a real finding recorded in WORKLOG.md), well
//   past its own 100-token bucket + 10/s refill, then goes idle again.
//   Running this *alongside* steady_state (not as a separate test) is the
//   point: it answers "does one site's burst degrade every other site's
//   baseline latency," not just "what happens to the bursting site itself."
//
// Requires: loadtest/sites.json (scripts/loadtest_setup.sh) and Central
// Ingestion running with --enable-auth --tls-cert --tls-key --ca-cert
// (see loadtest/README.md).
//
// Run: k6 run loadtest/pharos_load_test.js -e PHAROS_URL=https://localhost:8443/api/v1/events
//
// options.insecureSkipTLSVerify below skips chain validation against the
// project's self-signed CA (k6 has no simple way to add a custom trusted
// root); the TLS handshake and encryption are still real, this is a
// concession specific to the load-testing tool, not the application --
// internal/tlsutil's own tests already prove real CA validation works.

import http from "k6/http";
import { check, sleep } from "k6";
import { Counter, Trend } from "k6/metrics";
import { SharedArray } from "k6/data";

const sites = new SharedArray("sites", function () {
  return JSON.parse(open("./sites.json"));
});

const BASE_URL = __ENV.PHAROS_URL || "https://localhost:8443/api/v1/events";

// The last site in sites.json is reserved for the burst scenario; the rest
// run the steady baseline. loadtest_setup.sh's default (10 sites) gives 9
// steady + 1 burst.
const steadySites = sites.slice(0, sites.length - 1);
const burstSiteCreds = sites[sites.length - 1];

export const rateLimitRejections = new Counter("pharos_rate_limit_rejections");
export const burstRateLimitRejections = new Counter("pharos_burst_rate_limit_rejections");
// Two separate Trends (not one Trend with a scenario tag) specifically so
// k6's default CLI summary prints both independently -- the whole point of
// running burst_site alongside steady_state is comparing these two
// against each other, and a single tagged metric doesn't surface that
// comparison without extra JSON post-processing.
export const steadyAcceptedDuration = new Trend("pharos_steady_accepted_duration", true);
export const burstAcceptedDuration = new Trend("pharos_burst_accepted_duration", true);

export const options = {
  insecureSkipTLSVerify: true,
  scenarios: {
    steady_state: {
      executor: "per-vu-iterations",
      vus: steadySites.length,
      iterations: 40, // ~80s at one submission every 2s per site
      maxDuration: "120s",
      exec: "steadyState",
    },
    // constant-arrival-rate (not per-vu-iterations): the point is a genuine
    // request *arrival rate* that exceeds the site's token bucket refill,
    // independent of how long each individual request takes. A single VU
    // looping sequentially doesn't do this -- real per-request backend
    // latency (~120ms observed) naturally paces a sequential loop to
    // ~8 req/s, which is *below* the 10 tokens/sec sustained refill, so the
    // limiter never engages no matter how many requests that one VU sends.
    // This executor spins up extra VUs as needed to actually hit 30 req/s.
    burst_site: {
      executor: "constant-arrival-rate",
      rate: 30,
      timeUnit: "1s",
      duration: "15s",
      preAllocatedVUs: 20,
      maxVUs: 40,
      startTime: "30s",
      exec: "burstSite",
    },
  },
  thresholds: {
    // Steady-state sites must never be rate-limited by an unrelated site's
    // burst -- if this fails, the per-site token bucket isn't isolating
    // sites the way §2.3 says it should.
    pharos_rate_limit_rejections: ["count==0"],
  },
};

function adverseEventPayload(siteID, seq) {
  return JSON.stringify({
    site_id: siteID,
    events: [
      {
        resourceType: "AdverseEvent",
        identifier: [{ system: "urn:pharos:idempotency-key", value: `${siteID}:${seq}` }],
        actuality: "actual",
        subject: { reference: `Patient/LOADTEST-${seq}` },
        event: {
          coding: [{ system: "http://hl7.org/fhir/sid/meddra", code: "10013661", display: "Rash" }],
        },
        date: new Date().toISOString(),
        recordedDate: new Date().toISOString(),
        severity: { coding: [{ code: "mild" }] },
        study: [{ reference: "ResearchStudy/LOADTEST" }],
        location: { reference: `Location/${siteID}` },
      },
    ],
  });
}

function submit(site, seq) {
  return http.post(BASE_URL, adverseEventPayload(site.site_id, seq), {
    headers: {
      "Content-Type": "application/json",
      "X-Site-ID": site.site_id,
      "X-API-Key": site.api_key,
    },
  });
}

// steadyState: one VU per steady site, one submission every ~2s (well
// within the 10 tokens/sec sustained refill), for the whole test -- 9
// steady sites together offer ~4.5 req/s sustained, a realistic multi-site
// baseline rather than a stress test of steady-state alone (that's what
// burst_site is for).
export function steadyState() {
  const site = steadySites[(__VU - 1) % steadySites.length];
  const seq = __ITER + 1;
  const res = submit(site, seq);
  const ok = check(res, {
    "steady: status is 200": (r) => r.status === 200,
  });
  if (res.status === 429) {
    rateLimitRejections.add(1);
  }
  if (ok) {
    steadyAcceptedDuration.add(res.timings.duration);
  }
  sleep(2);
}

// burstSite: one call per arrival (the constant-arrival-rate executor
// invokes this at a genuine 30/s for 15s, starting at t=30s, using
// however many concurrent VUs it needs to hit that rate regardless of
// per-request latency) -- 450 requests total against a site whose bucket
// starts full at 100 tokens and refills at 10/sec, so it should exhaust
// its burst capacity a few seconds in and start seeing 429s for the rest.
export function burstSite() {
  // local_seq must parse as a plain uint64 (internal/model.ParseIdempotencyKey
  // splits the idempotency key on ":" and requires the last segment to be
  // numeric) -- a first attempt using `${__VU}-${__ITER}` produced a
  // non-numeric segment and got every non-rate-limited burst request
  // rejected with 422, not 200, silently invalidating that run's results
  // until caught by checking pharos_ingestion_validation_failures_total.
  const seq = __VU * 100000 + __ITER;
  const res = submit(burstSiteCreds, seq);
  check(res, { "burst: got a response": (r) => r.status !== 0 });
  if (res.status === 429) {
    burstRateLimitRejections.add(1);
  } else if (res.status === 200) {
    burstAcceptedDuration.add(res.timings.duration);
  }
}
