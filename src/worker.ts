/**
 * Background worker.
 *
 * Runs enrichment and feed ingestion, and owns the recurring schedule. Shares
 * src/lib with the Next app, so domain logic exists once — which is exactly why
 * nothing in src/lib/ioc, src/lib/feeds or src/lib/enrichment may be marked
 * `server-only` (it throws on import outside a bundler).
 *
 *   npm run worker
 */
import "dotenv/config";

import { DelayedError, Worker, type Job } from "bullmq";
import { db } from "@/lib/db";
import { createQueueConnection } from "@/lib/redis";
import {
  QUEUE_NAMES,
  enqueueFeed,
  feedQueue,
  huntQueue,
  reportQueue,
  type EnrichmentJob,
  type FeedJob,
  type HuntJob,
  type ReportJob,
} from "@/lib/queue/queues";
import { enrichAll, enrichOne, recomputeIndicatorConfidence } from "@/lib/enrichment/enrich";
import { loadCredentialCache } from "@/lib/enrichment/secrets";
import { runFeed } from "@/lib/feeds/run";
import { runHunt } from "@/lib/hunting/run";
import { runScheduledReport } from "@/lib/reports/run";

const log = (...args: unknown[]) =>
  console.log(new Date().toISOString(), ...args);

// --- Enrichment worker ----------------------------------------------------

const enrichmentWorker = new Worker<EnrichmentJob>(
  QUEUE_NAMES.enrichment,
  async (job: Job<EnrichmentJob>) => {
    const { indicatorId, provider, force } = job.data;

    const outcomes = provider
      ? [await enrichOne(indicatorId, provider, { force })]
      : await enrichAll(indicatorId, { force });

    // A rate-limited job is not a failure — it is the system working as
    // designed. Reschedule it past the reset instead of burning an attempt.
    const limited = outcomes.find((o) => o.status === "rate_limited");
    if (limited && limited.status === "rate_limited") {
      await job.moveToDelayed(Date.now() + Math.max(1000, limited.retryAfterMs));
      // BullMQ requires the processor to exit by throwing DelayedError after
      // moveToDelayed. Returning normally would make the worker finalize the
      // job as completed, so the deferred retry would never run.
      throw new DelayedError();
    }

    if (outcomes.some((o) => o.status === "fetched")) {
      await recomputeIndicatorConfidence(indicatorId);
    }

    return { outcomes };
  },
  {
    connection: createQueueConnection(),
    // Deliberately low. The providers' own quotas are the real constraint, and
    // high concurrency would just pile up rate-limit deferrals.
    concurrency: 4,
  },
);

// --- Feed worker ----------------------------------------------------------

const feedWorker = new Worker<FeedJob>(
  QUEUE_NAMES.feeds,
  async (job: Job<FeedJob>) => {
    const result = await runFeed(job.data.sourceId);
    log(
      `feed  ${result.ok ? "ok  " : "FAIL"} ${result.sourceName} — ${result.message}`,
    );
    if (!result.ok) throw new Error(result.message);
    return result;
  },
  {
    connection: createQueueConnection(),
    // Feeds are network-bound and hit distinct hosts, so a little parallelism
    // is safe; too much and we look like a scraper.
    concurrency: 3,
  },
);

// --- Hunt worker ----------------------------------------------------------

const huntWorker = new Worker<HuntJob>(
  QUEUE_NAMES.hunts,
  async (job: Job<HuntJob>) => {
    const result = await runHunt(job.data.huntId);
    if (!result.ok) {
      log(`hunt   FAIL ${job.data.huntId} — ${result.error}`);
      throw new Error(result.error);
    }
    log(
      `hunt   ok   ${job.data.huntId} — ${result.matchCount} match(es), ` +
        `${result.newCount} new${result.alerted ? ", alerted" : ""}`,
    );
    return result;
  },
  {
    connection: createQueueConnection(),
    // Hunts are single count/select queries; a little parallelism is fine.
    concurrency: 2,
  },
);

// --- Report worker ----------------------------------------------------------

const reportWorker = new Worker<ReportJob>(
  QUEUE_NAMES.reports,
  async (job: Job<ReportJob>) => {
    const result = await runScheduledReport(job.data.scheduledReportId);
    if (!result.ok) {
      log(`report FAIL ${job.data.scheduledReportId} — ${result.error}`);
      throw new Error(result.error);
    }
    log(`report ok   ${job.data.scheduledReportId} — filed ${result.reportId}`);
    return result;
  },
  {
    connection: createQueueConnection(),
    // Reports aggregate several counts/queries; keep it low so a burst of
    // schedules firing together doesn't hammer the database at once.
    concurrency: 2,
  },
);

// --- Scheduling -----------------------------------------------------------

/**
 * Installs a repeatable job per enabled source, using each source's own cron.
 *
 * Re-syncs on every boot so schedule edits in the UI take effect, and removes
 * schedulers for sources that were disabled or deleted — otherwise a disabled
 * feed keeps firing forever.
 */
async function syncSchedules() {
  const sources = await db.source.findMany({
    where: { enabled: true, schedule: { not: null } },
    select: { id: true, name: true, schedule: true },
  });

  const wanted = new Set(sources.map((s) => `src:${s.id}`));

  const existing = await feedQueue.getJobSchedulers();
  for (const sched of existing) {
    if (sched.key && !wanted.has(sched.key)) {
      await feedQueue.removeJobScheduler(sched.key);
      log(`schedule removed ${sched.key}`);
    }
  }

  for (const s of sources) {
    await feedQueue.upsertJobScheduler(
      `src:${s.id}`,
      { pattern: s.schedule! },
      { name: "run-feed", data: { sourceId: s.id, scheduled: true } },
    );
  }

  log(`scheduled ${sources.length} feed(s)`);
  return sources.length;
}

/**
 * Installs a repeatable job per scheduled hunt, using each hunt's own cron.
 * Mirrors syncSchedules for feeds: re-syncs on boot so UI edits take effect,
 * and removes schedulers for hunts whose schedule was cleared or deleted.
 */
async function syncHuntSchedules() {
  const hunts = await db.huntQuery.findMany({
    where: { schedule: { not: null } },
    select: { id: true, name: true, schedule: true },
  });

  const wanted = new Set(hunts.map((h) => `hunt:${h.id}`));

  const existing = await huntQueue.getJobSchedulers();
  for (const sched of existing) {
    if (sched.key && !wanted.has(sched.key)) {
      await huntQueue.removeJobScheduler(sched.key);
      log(`hunt schedule removed ${sched.key}`);
    }
  }

  for (const h of hunts) {
    await huntQueue.upsertJobScheduler(
      `hunt:${h.id}`,
      { pattern: h.schedule! },
      { name: "run-hunt", data: { huntId: h.id, scheduled: true } },
    );
  }

  log(`scheduled ${hunts.length} hunt(s)`);
  return hunts.length;
}

/**
 * Installs a repeatable job per enabled scheduled report, using each report's
 * own cron. Mirrors syncHuntSchedules: re-syncs on boot so UI edits take
 * effect, and removes schedulers for reports that were disabled or deleted.
 */
async function syncReportSchedules() {
  const reports = await db.scheduledReport.findMany({
    where: { enabled: true },
    select: { id: true, name: true, schedule: true },
  });

  const wanted = new Set(reports.map((r) => `report:${r.id}`));

  const existing = await reportQueue.getJobSchedulers();
  for (const sched of existing) {
    if (sched.key && !wanted.has(sched.key)) {
      await reportQueue.removeJobScheduler(sched.key);
      log(`report schedule removed ${sched.key}`);
    }
  }

  for (const r of reports) {
    await reportQueue.upsertJobScheduler(
      `report:${r.id}`,
      { pattern: r.schedule },
      { name: "run-report", data: { scheduledReportId: r.id, scheduled: true } },
    );
  }

  log(`scheduled ${reports.length} report(s)`);
  return reports.length;
}

/** Runs every enabled feed immediately — used on boot and by `--run-now`. */
async function runAllFeedsNow() {
  const sources = await db.source.findMany({
    where: { enabled: true },
    select: { id: true, name: true },
  });
  for (const s of sources) await enqueueFeed(s.id, false);
  log(`queued ${sources.length} feed(s) for immediate run`);
  return sources.length;
}

// --- Lifecycle ------------------------------------------------------------

enrichmentWorker.on("failed", (job, err) =>
  log(`enrich FAIL ${job?.data.indicatorId ?? "?"} — ${err.message}`),
);
feedWorker.on("failed", (job, err) =>
  log(`feed   FAIL ${job?.data.sourceId ?? "?"} — ${err.message}`),
);
huntWorker.on("failed", (job, err) =>
  log(`hunt   FAIL ${job?.data.huntId ?? "?"} — ${err.message}`),
);
reportWorker.on("failed", (job, err) =>
  log(`report FAIL ${job?.data.scheduledReportId ?? "?"} — ${err.message}`),
);

async function shutdown(signal: string) {
  log(`${signal} received, draining…`);
  await Promise.allSettled([
    enrichmentWorker.close(),
    feedWorker.close(),
    huntWorker.close(),
    reportWorker.close(),
  ]);
  await db.$disconnect();
  process.exit(0);
}
process.on("SIGINT", () => void shutdown("SIGINT"));
process.on("SIGTERM", () => void shutdown("SIGTERM"));

async function main() {
  log("Pulse worker starting");
  // Decrypt DB-stored provider keys into the process cache before any job can
  // run — providers check isConfigured() synchronously.
  await loadCredentialCache();
  await syncSchedules();
  await syncHuntSchedules();
  await syncReportSchedules();

  if (process.argv.includes("--run-now")) {
    await runAllFeedsNow();
  }

  // Re-read schedules periodically so changes made in the UI are picked up
  // without a restart.
  setInterval(() => {
    void syncSchedules().catch((e) => log("schedule sync failed", e));
    void syncHuntSchedules().catch((e) => log("hunt schedule sync failed", e));
    void syncReportSchedules().catch((e) => log("report schedule sync failed", e));
    // Same for keys added via Settings — no restart required.
    void loadCredentialCache().catch((e) => log("credential sync failed", e));
  }, 5 * 60_000);

  log("worker ready — enrichment + feeds + hunts + reports");
}

main().catch((err) => {
  console.error("worker failed to start:", err);
  process.exit(1);
});
