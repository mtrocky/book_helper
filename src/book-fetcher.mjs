import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { access, copyFile, mkdir, readdir, rename, rm, stat, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import {
  executeAgentBrowserPlaybook,
  scoreSearchResult,
  searchAgentBrowserPlaybook,
} from "./agent-browser-playbook.mjs";
import { openCacheStore } from "./cache-store.mjs";
import { debug, info } from "./debug.mjs";
import { supportsPlaywrightZLibrary } from "./playwright-zlibrary.mjs";
import { estimateDownloadTimeoutMs } from "./playbook-runtime-utils.mjs";
import {
  configureRemoteQueue,
  enqueueRemoteTask,
  getRemoteQueueSnapshot,
  getRemoteTaskState,
} from "./remote-queue.mjs";
import {
  DEFAULT_BROWSER_PROFILE_PATH,
  DEFAULT_LIBRARY_ROOT,
  describeStatus as describePlaybookStatus,
  getStatusSnapshot as getPlaybookStatusSnapshot,
  getPlaybookInitUrl,
  loadAgentBrowserSessionConfig,
  parsePluginConfig,
  resolvePlaybook,
} from "./playbooks.mjs";

export const DEFAULT_TOOL_NAME = "library_book_fetch";
export const CACHE_LOOKUP_TOOL_NAME = "library_cache_lookup";
export const SEARCH_TOOL_NAME = "library_book_search";
export const DOWNLOAD_TOOL_NAME = "library_book_download";
export const JOB_SUBMIT_TOOL_NAME = "library_job_submit";
export const JOB_STATUS_TOOL_NAME = "library_job_status";
export const JOB_RESULT_TOOL_NAME = "library_job_result";
export const PLUGIN_ID = "library-fetcher";
export const TOOL_GROUP = "group:plugins";
const TOOL_NAMES = [
  CACHE_LOOKUP_TOOL_NAME,
  SEARCH_TOOL_NAME,
  DOWNLOAD_TOOL_NAME,
  JOB_SUBMIT_TOOL_NAME,
  JOB_STATUS_TOOL_NAME,
  JOB_RESULT_TOOL_NAME,
  DEFAULT_TOOL_NAME,
];

const DEFAULT_DOWNLOAD_TIMEOUT_MS = 120000;
const MAX_STORED_JOBS = 200;
const JOB_RETENTION_MS = 24 * 60 * 60 * 1000;
const jobStore = new Map();
const recoveredJobRoots = new Set();

export { parsePluginConfig };

export function withToolEnabled(config) {
  const current = cloneObject(config);
  const tools = cloneObject(current.tools);

  for (const [pathName, rawList] of [
    ["tools.allow", tools.allow],
    ["tools.alsoAllow", tools.alsoAllow],
  ]) {
    const list = cloneStringList(rawList);
    if (
      TOOL_NAMES.every((toolName) => list.includes(toolName)) ||
      list.includes(PLUGIN_ID) ||
      list.includes(TOOL_GROUP)
    ) {
      return { nextConfig: current, changed: false, policyPath: pathName };
    }
  }

  if (Array.isArray(tools.allow)) {
    const allow = cloneStringList(tools.allow);
    for (const toolName of TOOL_NAMES) {
      if (!allow.includes(toolName)) allow.push(toolName);
    }
    return {
      nextConfig: {
        ...current,
        tools: {
          ...tools,
          allow,
        },
      },
      changed: true,
      policyPath: `tools.allow (${TOOL_NAMES.join(", ")})`,
    };
  }

  const alsoAllow = cloneStringList(tools.alsoAllow);
  for (const toolName of TOOL_NAMES) {
    if (!alsoAllow.includes(toolName)) alsoAllow.push(toolName);
  }
  return {
    nextConfig: {
      ...current,
      tools: {
        ...tools,
        alsoAllow,
      },
    },
    changed: true,
    policyPath: `tools.alsoAllow (${TOOL_NAMES.join(", ")})`,
  };
}

export async function describeStatus(pluginConfig, toolPolicySource = null) {
  const snapshot = await getStatusSnapshot(pluginConfig, toolPolicySource);
  return [
    await describePlaybookStatus(pluginConfig, toolPolicySource),
    "",
    "Remote queue:",
    `- concurrency: ${snapshot.remoteQueue.concurrency}`,
    `- max queued: ${snapshot.remoteQueue.maxQueued}`,
    `- running: ${snapshot.remoteQueue.running}`,
    `- queued: ${snapshot.remoteQueue.queued}`,
    "",
    "Jobs:",
    `- total tracked: ${snapshot.jobs.total}`,
    `- queued: ${snapshot.jobs.queued}`,
    `- running: ${snapshot.jobs.running}`,
    `- completed: ${snapshot.jobs.completed}`,
    `- failed: ${snapshot.jobs.failed}`,
  ].join("\n");
}

export async function getStatusSnapshot(pluginConfig, toolPolicySource = null) {
  const config = parsePluginConfig(pluginConfig);
  configureRemoteQueue({
    concurrency: config.remoteQueueConcurrency,
    maxQueued: config.remoteQueueMaxQueued,
  });
  const libraryRoot = path.resolve(config.defaultLibraryRoot ?? DEFAULT_LIBRARY_ROOT);
  const persistedJobCounts = await withPersistentJobStore(libraryRoot, async (cacheStore) => {
    await ensurePersistentJobsRecovered(cacheStore, libraryRoot);
    cacheStore.pruneJobs({ maxStored: MAX_STORED_JOBS, retentionMs: JOB_RETENTION_MS });
    return cacheStore.getJobCounts();
  });
  return {
    ...(await getPlaybookStatusSnapshot(pluginConfig, toolPolicySource)),
    remoteQueue: getRemoteQueueSnapshot(),
    jobs: persistedJobCounts,
  };
}

export async function submitLibraryJob(rawParams, rawPluginConfig) {
  const params = validateParams(rawParams);
  const pluginConfig = parsePluginConfig(rawPluginConfig);
  configureRemoteQueue({
    concurrency: pluginConfig.remoteQueueConcurrency,
    maxQueued: pluginConfig.remoteQueueMaxQueued,
  });
  const libraryRoot = path.resolve(
    params.libraryRoot ?? pluginConfig.defaultLibraryRoot ?? DEFAULT_LIBRARY_ROOT,
  );
  await withPersistentJobStore(libraryRoot, async (cacheStore) => {
    await ensurePersistentJobsRecovered(cacheStore, libraryRoot);
    cacheStore.pruneJobs({ maxStored: MAX_STORED_JOBS, retentionMs: JOB_RETENTION_MS });
  });

  const jobId = randomUUID();
  const queueSnapshot = getRemoteQueueSnapshot();
  const createdAt = new Date().toISOString();
  const job = {
    id: jobId,
    kind: "fetch",
    status: "submitted",
    createdAt,
    updatedAt: createdAt,
    params: sanitizeJobParams(params),
    queueTaskId: "",
    queueKind: "",
    queueInitialPosition: 0,
    queueTasksAheadAtEnqueue: 0,
    result: null,
    error: null,
  };
  jobStore.set(jobId, job);
  await persistJob(libraryRoot, job);

  const cacheProbe = params.forceRefresh
    ? null
    : await lookupCachedBook(rawParams, rawPluginConfig).catch(() => null);
  if (cacheProbe?.ok && cacheProbe?.found) {
    job.status = "completed";
    job.updatedAt = new Date().toISOString();
    job.result = {
      ...cacheProbe,
      jobId,
    };
    await persistJob(libraryRoot, job);
    return buildToolResult({
      ok: true,
      found: true,
      reason: "job_submitted",
      jobId,
      status: job.status,
      fromCache: true,
      queueStatus: null,
      resultPreview: summarizeJobResult(job.result),
    });
  }

  fetchBookToLibrary(rawParams, rawPluginConfig, undefined, {
    onRemoteQueueEvent(event) {
      if (!event || typeof event !== "object") return;
      if (event.taskId) {
        job.queueTaskId = event.taskId;
      }
      if (event.kind) {
        job.queueKind = event.kind;
      }
      if (Number.isInteger(event.initialPosition)) {
        job.queueInitialPosition = event.initialPosition;
      }
      if (Number.isInteger(event.tasksAhead)) {
        job.queueTasksAheadAtEnqueue = event.tasksAhead;
      }
      if (event.type === "queued") {
        job.status = "queued";
      } else if (event.type === "started") {
        job.status = "running";
      }
      job.updatedAt = new Date().toISOString();
      void persistJob(libraryRoot, job);
    },
  })
    .then((result) => {
      job.status = "completed";
      job.updatedAt = new Date().toISOString();
      job.result = {
        ...result,
        jobId,
      };
      return persistJob(libraryRoot, job);
    })
    .catch((error) => {
      job.status = "failed";
      job.updatedAt = new Date().toISOString();
      job.error = buildToolErrorResult(error, { jobId });
      return persistJob(libraryRoot, job);
    });

  return buildToolResult({
    ok: true,
    found: true,
    reason: "job_submitted",
    jobId,
    status: "queued",
    fromCache: false,
    queueStatus: {
      initialPosition: job.queueInitialPosition || queueSnapshot.running + queueSnapshot.queued + 1,
      tasksAheadAtEnqueue:
        job.queueTasksAheadAtEnqueue || queueSnapshot.running + queueSnapshot.queued,
    },
    resultPreview: null,
  });
}

export async function getLibraryJobStatus(rawParams, rawPluginConfig) {
  const jobId = typeof rawParams?.jobId === "string" ? rawParams.jobId.trim() : "";
  if (!jobId) {
    throw new Error("jobId is required.");
  }
  const job = await loadJob(rawParams, rawPluginConfig, jobId);
  if (!job) {
    throw new Error(`Unknown jobId: ${jobId}`);
  }

  const remoteTask = job.queueTaskId ? getRemoteTaskState(job.queueTaskId) : null;
  const status =
    job.status === "completed" || job.status === "failed"
      ? job.status
      : remoteTask?.state === "running"
      ? "running"
      : remoteTask?.state === "queued"
      ? "queued"
      : job.status;

  return buildToolResult({
    ok: true,
    found: true,
    reason: "job_status",
    jobId,
    status,
    createdAt: job.createdAt,
    updatedAt: job.updatedAt,
    params: job.params,
    queueStatus: remoteTask
      ? {
          currentPosition: remoteTask.currentPosition,
          tasksAhead: remoteTask.tasksAhead,
          waitSeconds: remoteTask.waitSeconds,
          runningSeconds: remoteTask.runningSeconds,
          initialPosition: job.queueInitialPosition || null,
          tasksAheadAtEnqueue: job.queueTasksAheadAtEnqueue || null,
        }
      : job.queueTaskId
      ? {
          currentPosition: null,
          tasksAhead: null,
          waitSeconds: null,
          runningSeconds: null,
          initialPosition: job.queueInitialPosition || null,
          tasksAheadAtEnqueue: job.queueTasksAheadAtEnqueue || null,
        }
      : null,
    resultPreview: job.result ? summarizeJobResult(job.result) : null,
    error: job.error
      ? {
          reason: job.error.reason ?? "job_failed",
          errorMessage: job.error.errorMessage ?? "Job failed.",
        }
      : null,
  });
}

export async function getLibraryJobResult(rawParams, rawPluginConfig) {
  const jobId = typeof rawParams?.jobId === "string" ? rawParams.jobId.trim() : "";
  if (!jobId) {
    throw new Error("jobId is required.");
  }
  const job = await loadJob(rawParams, rawPluginConfig, jobId);
  if (!job) {
    throw new Error(`Unknown jobId: ${jobId}`);
  }
  if (job.status === "failed") {
    return buildToolResult({
      ...job.error,
      jobId,
      status: "failed",
      reason: job.error?.reason ?? "job_failed",
    });
  }
  if (job.status !== "completed" || !job.result) {
    return buildToolResult({
      ok: true,
      found: false,
      reason: "job_not_ready",
      jobId,
      status: job.status,
    });
  }
  return buildToolResult({
    ...job.result,
    ok: true,
    found: true,
    reason: "job_result_ready",
    jobId,
    status: "completed",
  });
}

export async function auditLibraryCache(rawParams, rawPluginConfig) {
  const params =
    rawParams && typeof rawParams === "object" && !Array.isArray(rawParams) ? rawParams : {};
  const pluginConfig = parsePluginConfig(rawPluginConfig);
  configureRemoteQueue({
    concurrency: pluginConfig.remoteQueueConcurrency,
    maxQueued: pluginConfig.remoteQueueMaxQueued,
  });
  const libraryRoot = path.resolve(
    params.libraryRoot ?? pluginConfig.defaultLibraryRoot ?? DEFAULT_LIBRARY_ROOT,
  );
  const repair = Boolean(params.repair);

  await mkdir(libraryRoot, { recursive: true });
  const cacheStore = await openCacheStore({ libraryRoot });
  try {
    const audit = await cacheStore.auditConsistency({ repair });
    debug("cache audit finished", {
      cacheDbPath: cacheStore.dbPath,
      libraryRoot,
      ...audit,
      repair,
    });
    return buildToolResult({
      ok: true,
      found: true,
      reason: audit.invalidEntries > 0 ? "cache_audit_issues_found" : "cache_audit_clean",
      libraryRoot,
      cacheDbPath: cacheStore.dbPath,
      migratedLegacyJson: cacheStore.migration.migrated,
      importedLegacyEntries: cacheStore.migration.imported,
      repair,
      ...audit,
    });
  } finally {
    cacheStore.close();
  }
}

export async function resetLibraryCache(rawParams, rawPluginConfig) {
  const params =
    rawParams && typeof rawParams === "object" && !Array.isArray(rawParams) ? rawParams : {};
  if (!params.confirm) {
    throw new Error("resetLibraryCache requires confirm=true.");
  }

  const pluginConfig = parsePluginConfig(rawPluginConfig);
  const libraryRoot = path.resolve(
    params.libraryRoot ?? pluginConfig.defaultLibraryRoot ?? DEFAULT_LIBRARY_ROOT,
  );

  await mkdir(libraryRoot, { recursive: true });
  const entries = await readdir(libraryRoot, { withFileTypes: true });
  const removedPaths = [];
  for (const entry of entries) {
    const targetPath = path.join(libraryRoot, entry.name);
    await rm(targetPath, { recursive: true, force: true });
    removedPaths.push(targetPath);
  }

  return buildToolResult({
    ok: true,
    found: true,
    reason: "library_reset_complete",
    libraryRoot,
    removedCount: removedPaths.length,
    removedPaths,
  });
}

export async function startLibraryLogin(rawParams, rawPluginConfig) {
  const params =
    rawParams && typeof rawParams === "object" && !Array.isArray(rawParams) ? rawParams : {};
  const pluginConfig = parsePluginConfig(rawPluginConfig);
  const { playbook, playbookPath } = await resolvePlaybook({
    siteId: typeof params.siteId === "string" ? params.siteId.trim() : undefined,
    playbookPath: typeof params.playbookPath === "string" ? params.playbookPath : undefined,
    pluginConfig,
  });

  if (!playbook || !playbookPath) {
    throw new Error("siteId or playbookPath is required to start login.");
  }

  const sessionRuntime = await loadAgentBrowserSessionConfig(pluginConfig);
  const sessionName =
    (typeof params.sessionName === "string" && params.sessionName.trim()) ||
    sessionRuntime.config.sessionName ||
    playbook.browser.sessionName ||
    "clawbot";
  const profilePath = path.resolve(
    (typeof params.profilePath === "string" && params.profilePath.trim()) ||
      sessionRuntime.config.profilePath ||
      pluginConfig.browserProfilePath ||
      playbook.browser.profilePath ||
      DEFAULT_BROWSER_PROFILE_PATH,
  );
  const sessionConfigPath = sessionRuntime.sessionConfigPath;
  const initUrl = getPlaybookInitUrl(playbook);
  const binary = pluginConfig.agentBrowserPath ?? "agent-browser";

  await mkdir(path.dirname(profilePath), { recursive: true });
  await mkdir(path.dirname(sessionConfigPath), { recursive: true });
  await writeFile(
    sessionConfigPath,
    `${JSON.stringify({ sessionName, profilePath }, null, 2)}\n`,
    "utf8",
  );

  await runAgentBrowserCommand(binary, ["close"]).catch(() => {});
  await runAgentBrowserCommand(binary, ["--session", sessionName, "close"]).catch(() => {});

  const child = spawn(
    binary,
    ["--headed", "--session", sessionName, "--profile", profilePath, "open", initUrl],
    {
      cwd: process.cwd(),
      detached: true,
      stdio: "ignore",
    },
  );
  child.unref();

  return buildToolResult({
    ok: true,
    found: true,
    reason: "login_session_started",
    started: true,
    mode: "headed-login",
    sessionName,
    profilePath,
    sessionConfigPath,
    playbookId: playbook.id,
    playbookPath,
    initUrl,
    nextStep: "Complete login in the opened browser, close the window, then run the fetch/search tools normally.",
  });
}

export async function lookupCachedBook(rawParams, rawPluginConfig) {
  const startedAt = Date.now();
  const params = validateParams(rawParams);
  const pluginConfig = parsePluginConfig(rawPluginConfig);
  const libraryRoot = path.resolve(
    params.libraryRoot ?? pluginConfig.defaultLibraryRoot ?? DEFAULT_LIBRARY_ROOT,
  );

  await mkdir(libraryRoot, { recursive: true });
  const cacheStore = await openCacheStore({ libraryRoot });
  try {
    info("cache lookup started", {
      query: params.query,
      titleHint: params.titleHint,
      authorHint: params.authorHint,
      languageHint: params.languageHint,
      libraryRoot,
    });
    const cacheHit = await cacheStore.findCacheHit({
      query: params.query,
      titleHint: params.titleHint,
      authorHint: params.authorHint,
      languageHint: params.languageHint,
    });
    if (!cacheHit) {
      info("cache lookup missed", {
        query: params.query,
        libraryRoot,
      });
      return buildToolResult({
        ok: true,
        found: false,
        backend: "cache",
        libraryRoot,
        reason: "cache_miss",
        elapsedSeconds: roundElapsedSeconds(startedAt),
      });
    }

    const shareInfo = await buildShareInfo(cacheHit.filePath, pluginConfig);
    info("cache lookup hit", {
      title: cacheHit.title,
      author: cacheHit.author,
      filePath: cacheHit.filePath,
      deliveryReady: shareInfo.deliveryReady,
    });

    return buildToolResult({
      ok: true,
      found: true,
      backend: "cache",
      title: cacheHit.title,
      author: cacheHit.author,
      language: cacheHit.language ?? "",
      filePath: cacheHit.filePath,
      libraryRoot,
      sourceUrl: sanitizeExternalUrl(cacheHit.sourceUrl),
      downloadUrl: cacheHit.downloadUrl,
      playbookId: cacheHit.playbookId ?? "",
      ...shareInfo,
      userStatus: buildUserStatus({
        stage: "cache_hit",
        title: cacheHit.title,
        author: cacheHit.author,
      }),
      reason: "cache_hit",
      elapsedSeconds: roundElapsedSeconds(startedAt),
    });
  } finally {
    cacheStore.close();
  }
}

export async function searchBooks(rawParams, rawPluginConfig, signal) {
  const startedAt = Date.now();
  const params = validateParams(rawParams);
  const pluginConfig = parsePluginConfig(rawPluginConfig);
  configureRemoteQueue({
    concurrency: pluginConfig.remoteQueueConcurrency,
    maxQueued: pluginConfig.remoteQueueMaxQueued,
  });
  const libraryRoot = path.resolve(
    params.libraryRoot ?? pluginConfig.defaultLibraryRoot ?? DEFAULT_LIBRARY_ROOT,
  );
  const { playbook, playbookPath } = await resolvePlaybook({
    siteId: params.siteId,
    playbookPath: params.playbookPath,
    pluginConfig,
  });
  if (!playbook || !playbookPath) {
    throw new Error("siteId or playbookPath is required for remote search.");
  }

  const downloadTimeoutMs =
    params.timeoutMs ?? pluginConfig.downloadTimeoutMs ?? DEFAULT_DOWNLOAD_TIMEOUT_MS;
  info("remote search started", {
    query: params.query,
    titleHint: params.titleHint,
    authorHint: params.authorHint,
    languageHint: params.languageHint,
    playbookId: playbook.id,
    playbookPath,
    timeoutMs: downloadTimeoutMs,
  });
  const tempRoot = path.join(os.tmpdir(), `openclaw-library-search-${randomUUID()}`);
  await mkdir(tempRoot, { recursive: true });

  const cacheStore = await openCacheStore({ libraryRoot });
  try {
    const queued = await enqueueRemoteTask(
      {
        kind: "remote-search",
        signal,
        meta: {
          query: params.query,
          playbookId: playbook.id,
        },
      },
      () =>
        searchAgentBrowserPlaybook({
          params,
          pluginConfig,
          playbook,
          tempRoot,
          downloadTimeoutMs,
          signal,
        }),
    );
    const result = queued.value;
    const backend = inferDownloadBackend(playbook);
    const results = (result.results ?? [])
      .map((entry, index) =>
        formatSearchResult(entry, {
          params,
          playbookId: playbook.id,
          playbookPath,
          resultIndex: index + 1,
          estimate: buildResultEstimate({
            cacheStore,
            playbookId: playbook.id,
            backend,
            format: entry.format ?? "",
          }),
        }),
      )
      .sort((left, right) => right.score - left.score);
    info("remote search finished", {
      query: params.query,
      resultCount: results.length,
      topResult: results[0]
        ? {
            title: results[0].title,
            author: results[0].author,
            language: results[0].language ?? "",
            publisher: results[0].publisher,
            format: results[0].format,
            score: results[0].score,
          }
        : null,
    });

    return buildToolResult({
      ok: true,
      found: results.length > 0,
      backend: result.backend ?? "agent-browser-search",
      query: params.query,
      titleHint: params.titleHint,
      authorHint: params.authorHint,
      languageHint: params.languageHint,
      playbookId: playbook.id,
      playbookPath,
      results,
      currentUrl: result.currentUrl,
      currentTitle: result.currentTitle,
      llmFallbackUsed: result.llmFallbackUsed,
      queueStatus: queued.queueStatus,
      userStatus: results[0]
        ? buildUserStatus({
            stage: "found",
            title: results[0].title,
            author: results[0].author,
            format: results[0].formatLabel,
            sizeLabel: results[0].sizeLabel,
            estimatedSeconds: results[0].estimatedDownloadSeconds,
          })
        : null,
      reason: results.length > 0 ? "search_results_found" : "no_matching_search_result",
      elapsedSeconds: roundElapsedSeconds(startedAt),
    });
  } finally {
    cacheStore.close();
    await rm(tempRoot, { recursive: true, force: true });
  }
}

export async function downloadBookToLibrary(rawParams, rawPluginConfig, signal) {
  const params =
    rawParams && typeof rawParams === "object" && !Array.isArray(rawParams) ? rawParams : {};
  const resolved = resolveSelectionTokenParams(params.selectionToken);
  const hasSelectionToken = Boolean(params.selectionToken);

  if (!hasSelectionToken) {
    const query = typeof params.query === "string" ? params.query.trim() : "";
    if (!query) {
      throw new Error("library_book_download requires selectionToken or query.");
    }
  }

  const requestParams = hasSelectionToken
    ? {
        libraryRoot: params.libraryRoot,
        timeoutMs: params.timeoutMs,
        keepSessionOnError: params.keepSessionOnError,
        forceRefresh: Boolean(params.forceRefresh),
        ...resolved,
      }
    : params;

  return fetchBookToLibrary(
    {
      ...requestParams,
      cacheOnly: false,
    },
    rawPluginConfig,
    signal,
  );
}

export async function fetchBookToLibrary(rawParams, rawPluginConfig, signal, hooks = {}) {
  const startedAt = Date.now();
  const params = validateParams(rawParams);
  const pluginConfig = parsePluginConfig(rawPluginConfig);
  configureRemoteQueue({
    concurrency: pluginConfig.remoteQueueConcurrency,
    maxQueued: pluginConfig.remoteQueueMaxQueued,
  });
  debug("starting fetch", {
    query: params.query,
    titleHint: params.titleHint,
    authorHint: params.authorHint,
    languageHint: params.languageHint,
    siteId: params.siteId,
    playbookPath: params.playbookPath,
    libraryRoot: params.libraryRoot,
    timeoutMs: params.timeoutMs,
    forceRefresh: params.forceRefresh,
    cacheOnly: params.cacheOnly,
    keepSessionOnError: params.keepSessionOnError,
  });
  const libraryRoot = path.resolve(
    params.libraryRoot ?? pluginConfig.defaultLibraryRoot ?? DEFAULT_LIBRARY_ROOT,
  );
  const downloadTimeoutMs = params.timeoutMs ?? pluginConfig.downloadTimeoutMs ?? DEFAULT_DOWNLOAD_TIMEOUT_MS;
  info("book fetch started", {
    query: params.query,
    titleHint: params.titleHint,
    authorHint: params.authorHint,
    languageHint: params.languageHint,
    libraryRoot,
    timeoutMs: downloadTimeoutMs,
    forceRefresh: params.forceRefresh,
    cacheOnly: params.cacheOnly,
  });
  debug("resolved runtime config", {
    libraryRoot,
    downloadTimeoutMs,
  });

  await mkdir(libraryRoot, { recursive: true });
  const cacheStore = await openCacheStore({ libraryRoot });
  debug("opened cache store", {
    cacheDbPath: cacheStore.dbPath,
    entries: cacheStore.getEntryCount(),
    migratedLegacyJson: cacheStore.migration.migrated,
    importedLegacyEntries: cacheStore.migration.imported,
  });

  try {
    const existingCacheEntry = await cacheStore.findCacheHit({
      query: params.query,
      titleHint: params.titleHint,
      authorHint: params.authorHint,
      languageHint: params.languageHint,
    });

    if (!params.forceRefresh) {
      const cacheHit = existingCacheEntry;
      if (cacheHit) {
        debug("cache hit", {
          title: cacheHit.title,
          filePath: cacheHit.filePath,
        });
        const shareInfo = await buildShareInfo(cacheHit.filePath, pluginConfig);
        info("book fetch served from cache", {
          title: cacheHit.title,
          author: cacheHit.author,
          filePath: cacheHit.filePath,
          deliveryReady: shareInfo.deliveryReady,
        });
        return buildToolResult({
          ok: true,
          fromCache: true,
          found: true,
          backend: "cache",
          title: cacheHit.title,
          author: cacheHit.author,
          language: cacheHit.language ?? "",
          filePath: cacheHit.filePath,
          libraryRoot,
          sourceUrl: sanitizeExternalUrl(cacheHit.sourceUrl),
          downloadUrl: cacheHit.downloadUrl,
          playbookId: cacheHit.playbookId ?? "",
          ...shareInfo,
          userStatus: buildUserStatus({
            stage: "cache_hit",
            title: cacheHit.title,
            author: cacheHit.author,
          }),
          reason: "cache_hit",
          elapsedSeconds: roundElapsedSeconds(startedAt),
        });
      }
    }

    if (params.cacheOnly) {
      debug("cache only miss");
      throw new Error(`No cached book matched "${params.query}". cacheOnly=true prevented remote access.`);
    }

    const { playbook, playbookPath } = await resolvePlaybook({
      siteId: params.siteId,
      playbookPath: params.playbookPath,
      pluginConfig,
    });
    debug("resolved playbook", {
      playbookPath,
      playbookId: playbook?.id ?? null,
      stepCount: playbook?.steps?.length ?? 0,
    });
    if (!playbook || !playbookPath) {
      throw new Error("siteId or playbookPath is required when the book is not already cached.");
    }
    info("book fetch using remote playbook", {
      playbookId: playbook.id,
      playbookPath,
    });

    const tempRoot = path.join(os.tmpdir(), `openclaw-library-fetch-${randomUUID()}`);
    await mkdir(tempRoot, { recursive: true });

    try {
      const downloadTimingModel = {
        estimateTimeout: async ({ playbookId, backend, expectedBytes }) =>
          cacheStore.estimateDownloadWindow({ playbookId, backend, expectedBytes }),
        recordSample: (sample) => cacheStore.recordDownloadSample(sample),
      };
      const queued = await enqueueRemoteTask(
        {
          kind: "remote-fetch",
          signal,
          meta: {
            query: params.query,
            playbookId: playbook.id,
          },
          onEvent: hooks.onRemoteQueueEvent,
        },
        () =>
          executeAgentBrowserPlaybook({
            params,
            pluginConfig,
            playbook,
            tempRoot,
            downloadTimeoutMs,
            downloadTimingModel,
            signal,
          }),
      );
      const download = queued.value;
      debug("playbook execution finished", download);

      const finalPath = await moveToLibrary({
        libraryRoot,
        title: download.title,
        filePath: download.filePath,
        preferredExtension: path.extname(download.filePath) || ".bin",
        replacementPath: chooseReplacementPath({
          existingEntry: existingCacheEntry,
          download,
        }),
      });
      debug("moved file to library", {
        sourcePath: download.filePath,
        finalPath,
      });

      const entry = {
        id: randomUUID(),
        query: params.query,
        title: download.title,
        author: download.author ?? "",
        language: download.language ?? "",
        sourceUrl: download.sourceUrl ?? "",
        downloadUrl: download.downloadUrl ?? "",
        filePath: finalPath,
        aliases: compactUnique([params.query, params.titleHint, download.title]),
        downloadedAt: new Date().toISOString(),
        playbookId: playbook.id,
        playbookPath,
      };
      cacheStore.upsert(entry);
      await recordDownloadTelemetry({
        cacheStore,
        playbookId: playbook.id,
        download,
        finalPath,
      });
      debug("updated cache store", {
        cacheDbPath: cacheStore.dbPath,
        entries: cacheStore.getEntryCount(),
      });
      const shareInfo = await buildShareInfo(finalPath, pluginConfig);
      info("book fetch downloaded and stored", {
        title: download.title,
        author: download.author ?? "",
        filePath: finalPath,
        deliveryReady: shareInfo.deliveryReady,
        deliveryError: shareInfo.deliveryError,
      });

      return buildToolResult({
        ok: true,
        fromCache: false,
        found: true,
        backend: download.backend ?? "agent-browser",
        title: download.title,
        author: download.author,
        language: download.language ?? "",
        filePath: finalPath,
        libraryRoot,
        sourceUrl: sanitizeExternalUrl(download.sourceUrl),
        downloadUrl: download.downloadUrl,
        playbookId: playbook.id,
        llmFallbackUsed: download.llmFallbackUsed,
        queueStatus: queued.queueStatus,
        ...shareInfo,
        userStatus: buildUserStatus({
          stage: "download_completed",
          title: download.title,
          author: download.author,
          format: extractFormatSummary(download.downloadMetrics?.format ?? "").formatLabel,
          sizeLabel:
            extractFormatSummary(download.downloadMetrics?.format ?? "").sizeLabel ||
            formatBytes(download.downloadMetrics?.fileSizeBytes ?? null),
          estimatedSeconds: download.downloadMetrics?.predictedSeconds ?? null,
        }),
        reason: "downloaded",
        elapsedSeconds: roundElapsedSeconds(startedAt),
      });
    } finally {
      debug("cleaning temp root", { tempRoot });
      await rm(tempRoot, { recursive: true, force: true });
    }
  } finally {
    cacheStore.close();
  }
}

async function recordDownloadTelemetry({ cacheStore, playbookId, download, finalPath }) {
  if (!download?.downloadMetrics) {
    return;
  }

  const finalSizeBytes = await stat(finalPath)
    .then((details) => details.size)
    .catch(() => null);
  cacheStore.recordDownloadSample({
    id: randomUUID(),
    recordedAt: new Date().toISOString(),
    playbookId,
    backend: download.downloadMetrics.backend ?? download.backend ?? "",
    format: download.downloadMetrics.format ?? "",
    fileSizeBytes: finalSizeBytes ?? download.downloadMetrics.fileSizeBytes ?? null,
    expectedBytes: download.downloadMetrics.expectedBytes ?? null,
    predictedSeconds: download.downloadMetrics.predictedSeconds ?? null,
    predictedTimeoutMs: download.downloadMetrics.predictedTimeoutMs ?? null,
    durationSeconds: download.downloadMetrics.durationSeconds,
  });
}

function validateParams(rawParams) {
  if (!rawParams || typeof rawParams !== "object" || Array.isArray(rawParams)) {
    throw new Error("Tool parameters must be an object.");
  }

  const params = rawParams;
  const explicitTitle =
    typeof params.title === "string" ? params.title.trim() : "";
  const explicitAuthor =
    typeof params.author === "string" ? params.author.trim() : "";
  const query =
    typeof params.query === "string" ? params.query.trim() : explicitTitle;
  if (!query) {
    throw new Error("query or title is required.");
  }
  const explicitTitleHint =
    typeof params.titleHint === "string" ? params.titleHint.trim() : "";
  const explicitAuthorHint =
    typeof params.authorHint === "string" ? params.authorHint.trim() : "";
  const explicitLanguage =
    typeof params.language === "string" ? params.language.trim() : "";
  const explicitLanguageHint =
    typeof params.languageHint === "string" ? params.languageHint.trim() : "";

  return {
    query,
    titleHint: explicitTitleHint || explicitTitle || query,
    authorHint: explicitAuthorHint || explicitAuthor,
    languageHint: explicitLanguageHint || explicitLanguage,
    siteId: typeof params.siteId === "string" ? params.siteId.trim() : undefined,
    playbookPath: typeof params.playbookPath === "string" ? params.playbookPath : undefined,
    libraryRoot: typeof params.libraryRoot === "string" ? params.libraryRoot : undefined,
    timeoutMs:
      Number.isInteger(params.timeoutMs) && params.timeoutMs > 0 ? params.timeoutMs : undefined,
    forceRefresh: Boolean(params.forceRefresh),
    cacheOnly: Boolean(params.cacheOnly),
    keepSessionOnError: Boolean(params.keepSessionOnError),
    resultIndex:
      Number.isInteger(params.resultIndex) && params.resultIndex > 0 ? params.resultIndex : undefined,
    selectionToken: typeof params.selectionToken === "string" ? params.selectionToken.trim() : "",
  };
}

async function moveToLibrary({ libraryRoot, title, filePath, preferredExtension, replacementPath }) {
  const extension = preferredExtension || path.extname(filePath) || ".bin";
  const finalPath = replacementPath
    ? replacementPath
    : await ensureUniquePath(path.join(libraryRoot, buildSafeFileName(title, extension)));

  try {
    await rename(filePath, finalPath);
  } catch {
    await copyFile(filePath, finalPath);
  }

  return finalPath;
}

function buildSafeFileName(title, extension) {
  const stem = sanitizeFileName(title || "downloaded-book");
  const safeExtension = extension.startsWith(".") ? extension : `.${extension || "bin"}`;
  return `${stem}${safeExtension}`;
}

function sanitizeFileName(value) {
  return (value || "downloaded-book")
    .replace(/[<>:"/\\|?*\u0000-\u001f]/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 120)
    .replace(/\s/g, "_");
}

async function ensureUniquePath(targetPath) {
  if (!(await exists(targetPath))) return targetPath;
  const extension = path.extname(targetPath);
  const stem = targetPath.slice(0, -extension.length);
  for (let index = 2; index < 1000; index += 1) {
    const candidate = `${stem}_${index}${extension}`;
    if (!(await exists(candidate))) return candidate;
  }
  throw new Error(`Could not allocate a unique path for ${targetPath}`);
}

function compactUnique(values) {
  return [...new Set(values.filter((value) => typeof value === "string" && value.trim()).map((value) => value.trim()))];
}

function cloneObject(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? { ...value } : {};
}

function cloneStringList(value) {
  return Array.isArray(value) ? value.filter((entry) => typeof entry === "string") : [];
}

async function exists(targetPath) {
  try {
    await access(targetPath);
    return true;
  } catch {
    return false;
  }
}

async function buildShareInfo(filePath, pluginConfig) {
  try {
    const mediaRoot = await resolveOpenClawMediaRoot(pluginConfig);
    if (!mediaRoot) {
      return {
        deliveryReady: false,
        deliveryError: "OpenClaw media root is not configured",
        sharePath: "",
        shareRelativePath: "",
        replyMediaToken: "",
      };
    }

    const shareDir = path.join(mediaRoot, "attachments", "library-fetcher");
    const sharePath = path.join(shareDir, path.basename(filePath));
    await mkdir(shareDir, { recursive: true });
    await copyFile(filePath, sharePath);

    return {
      deliveryReady: true,
      deliveryError: "",
      sharePath,
      shareRelativePath: "",
      replyMediaToken: `MEDIA:${sharePath}`,
    };
  } catch (error) {
    debug("failed to prepare share copy", {
      filePath,
      error: error instanceof Error ? error.message : String(error),
    });
    return {
      deliveryReady: false,
      deliveryError: error instanceof Error ? error.message : String(error),
      sharePath: "",
      shareRelativePath: "",
      replyMediaToken: "",
    };
  }
}

async function resolveOpenClawMediaRoot(pluginConfig) {
  const configured = pluginConfig.openclawMediaRoot?.trim();
  if (configured) {
    return path.resolve(configured);
  }

  return path.join(os.homedir(), ".openclaw", "workspace");
}

function roundElapsedSeconds(startedAt) {
  return Math.round(((Date.now() - startedAt) / 1000) * 100) / 100;
}

function formatSearchResult(entry, { params, playbookId, playbookPath, resultIndex, estimate }) {
  const score = scoreSearchResult(entry, params);
  const summary = extractFormatSummary(entry.format ?? "");
  return {
    title: entry.title,
    author: entry.author,
    language: entry.language ?? "",
    publisher: entry.publisher ?? "",
    format: entry.format ?? "",
    formatLabel: summary.formatLabel,
    sizeLabel: summary.sizeLabel,
    estimatedDownloadSeconds: estimate?.predictedSeconds ?? null,
    resultIndex,
    score,
    selectionToken: encodeSelectionToken({
      query: params.query,
      titleHint: params.titleHint,
      authorHint: params.authorHint,
      languageHint: params.languageHint,
      playbookId,
      playbookPath,
      resultIndex,
      title: entry.title,
      author: entry.author ?? "",
      publisher: entry.publisher ?? "",
    }),
  };
}

function inferDownloadBackend(playbook) {
  return supportsPlaywrightZLibrary(playbook) ? "playwright" : "agent-browser";
}

function buildResultEstimate({ cacheStore, playbookId, backend, format }) {
  const expectedBytes = extractFormatSummary(format).sizeBytes;
  const modelEstimate = cacheStore.estimateDownloadWindow({
    playbookId,
    backend,
    expectedBytes,
  });
  if (modelEstimate?.predictedSeconds) {
    return modelEstimate;
  }

  const heuristicTimeoutMs = estimateDownloadTimeoutMs({
    baseTimeoutMs: DEFAULT_DOWNLOAD_TIMEOUT_MS,
    format,
  });
  return {
    predictedSeconds: Math.round(heuristicTimeoutMs / 1000),
    timeoutMs: heuristicTimeoutMs,
    sampleCount: 0,
    strategy: "heuristic_format_size",
  };
}

function extractFormatSummary(format) {
  const raw = String(format || "").trim();
  if (!raw) {
    return {
      formatLabel: "",
      sizeLabel: "",
      sizeBytes: null,
    };
  }

  const parts = raw.split(",").map((part) => part.trim()).filter(Boolean);
  const formatLabel = parts[0] ?? raw;
  const sizeLabel = parts.find((part) => /\b(KB|MB|GB|TB)\b/i.test(part)) ?? "";
  return {
    formatLabel,
    sizeLabel,
    sizeBytes: parseSizeLabelToBytes(sizeLabel),
  };
}

function parseSizeLabelToBytes(sizeLabel) {
  const text = String(sizeLabel || "").trim();
  const match = text.match(/(\d+(?:\.\d+)?)\s*(KB|MB|GB|TB)\b/i);
  if (!match) {
    return null;
  }
  const amount = Number(match[1]);
  if (!Number.isFinite(amount) || amount <= 0) {
    return null;
  }
  const unit = match[2].toUpperCase();
  const multipliers = {
    KB: 1024,
    MB: 1024 ** 2,
    GB: 1024 ** 3,
    TB: 1024 ** 4,
  };
  return Math.round(amount * (multipliers[unit] ?? 1));
}

function buildUserStatus({ stage, title, author, format = "", sizeLabel = "", estimatedSeconds = null }) {
  return {
    stage,
    title: String(title || "").trim(),
    author: String(author || "").trim(),
    format: String(format || "").trim(),
    sizeLabel: String(sizeLabel || "").trim(),
    estimatedSeconds:
      Number.isFinite(Number(estimatedSeconds)) && Number(estimatedSeconds) > 0
        ? Math.round(Number(estimatedSeconds))
        : null,
  };
}

function formatBytes(bytes) {
  const value = Number(bytes);
  if (!Number.isFinite(value) || value <= 0) {
    return "";
  }
  const units = ["B", "KB", "MB", "GB", "TB"];
  let index = 0;
  let current = value;
  while (current >= 1024 && index < units.length - 1) {
    current /= 1024;
    index += 1;
  }
  const rounded = index === 0 ? Math.round(current) : Math.round(current * 100) / 100;
  return `${rounded} ${units[index]}`;
}

function encodeSelectionToken(payload) {
  return Buffer.from(JSON.stringify(payload), "utf8").toString("base64url");
}

function resolveSelectionTokenParams(selectionToken) {
  if (!selectionToken) {
    return {};
  }

  try {
    const decoded = JSON.parse(Buffer.from(selectionToken, "base64url").toString("utf8"));
    if (!decoded || typeof decoded !== "object" || Array.isArray(decoded)) {
      return {};
    }

    return {
      query: typeof decoded.query === "string" ? decoded.query : undefined,
      titleHint: typeof decoded.titleHint === "string" ? decoded.titleHint : undefined,
      authorHint: typeof decoded.authorHint === "string" ? decoded.authorHint : undefined,
      languageHint:
        typeof decoded.languageHint === "string" ? decoded.languageHint : undefined,
      playbookPath: typeof decoded.playbookPath === "string" ? decoded.playbookPath : undefined,
      siteId: typeof decoded.playbookId === "string" ? decoded.playbookId : undefined,
      resultIndex:
        Number.isInteger(decoded.resultIndex) && decoded.resultIndex > 0
          ? decoded.resultIndex
          : undefined,
    };
  } catch {
    throw new Error("Invalid selectionToken.");
  }
}

function sanitizeExternalUrl(value) {
  const raw = String(value ?? "").trim();
  if (!raw || raw.startsWith("@")) {
    return "";
  }
  return raw;
}

function sanitizeJobParams(params) {
  return {
    query: params.query,
    titleHint: params.titleHint,
    authorHint: params.authorHint,
    languageHint: params.languageHint,
    libraryRoot: params.libraryRoot ?? "",
    siteId: params.siteId ?? "",
    playbookPath: params.playbookPath ?? "",
    forceRefresh: Boolean(params.forceRefresh),
  };
}

async function loadJob(rawParams, rawPluginConfig, jobId) {
  const params =
    rawParams && typeof rawParams === "object" && !Array.isArray(rawParams) ? rawParams : {};
  const libraryRoot = resolveJobLibraryRoot(params, rawPluginConfig);
  return withPersistentJobStore(libraryRoot, async (cacheStore) => {
    await ensurePersistentJobsRecovered(cacheStore, libraryRoot);
    cacheStore.pruneJobs({ maxStored: MAX_STORED_JOBS, retentionMs: JOB_RETENTION_MS });
    const persisted = cacheStore.getJob(jobId);
    if (persisted) {
      jobStore.set(jobId, persisted);
    }
    return persisted ?? jobStore.get(jobId) ?? null;
  });
}

async function persistJob(libraryRoot, job) {
  jobStore.set(job.id, job);
  return withPersistentJobStore(libraryRoot, async (cacheStore) => {
    cacheStore.upsertJob(job);
  });
}

async function withPersistentJobStore(libraryRoot, fn) {
  await mkdir(libraryRoot, { recursive: true });
  const cacheStore = await openCacheStore({ libraryRoot });
  try {
    return await fn(cacheStore);
  } finally {
    cacheStore.close();
  }
}

async function ensurePersistentJobsRecovered(cacheStore, libraryRoot) {
  if (recoveredJobRoots.has(libraryRoot)) {
    return;
  }
  cacheStore.recoverInterruptedJobs();
  recoveredJobRoots.add(libraryRoot);
}

function resolveJobLibraryRoot(params, rawPluginConfig) {
  const pluginConfig = parsePluginConfig(rawPluginConfig);
  const raw = typeof params?.libraryRoot === "string" && params.libraryRoot.trim()
    ? params.libraryRoot
    : undefined;
  return path.resolve(raw ?? pluginConfig.defaultLibraryRoot ?? DEFAULT_LIBRARY_ROOT);
}

function summarizeJobResult(result) {
  if (!result || typeof result !== "object") {
    return null;
  }
  return {
    title: result.title ?? "",
    author: result.author ?? "",
    filePath: result.filePath ?? "",
    fromCache: Boolean(result.fromCache) || result.backend === "cache" || result.reason === "cache_hit",
    deliveryReady: Boolean(result.deliveryReady),
    reason: result.reason ?? "",
    userStatus: result.userStatus ?? null,
  };
}

function pruneJobs() {
  const now = Date.now();
  const entries = [...jobStore.values()].sort(
    (left, right) => Date.parse(left.updatedAt) - Date.parse(right.updatedAt),
  );

  for (const job of entries) {
    if (
      jobStore.size > MAX_STORED_JOBS ||
      (job.status === "completed" || job.status === "failed") &&
        now - Date.parse(job.updatedAt) > JOB_RETENTION_MS
    ) {
      jobStore.delete(job.id);
    }
  }
}

function buildToolResult(payload) {
  return {
    ok: payload.ok ?? true,
    found: payload.found ?? true,
    reason: payload.reason ?? null,
    ...payload,
  };
}

export function buildToolErrorResult(error, fallback = {}) {
  const message = error instanceof Error ? error.message : String(error);
  return buildToolResult({
    ok: false,
    found: false,
    backend: fallback.backend ?? "",
    reason: mapErrorToReason(message),
    errorMessage: message,
    ...fallback,
  });
}

function mapErrorToReason(message) {
  const text = String(message || "");

  if (/query or title is required/i.test(text)) return "missing_query";
  if (/requires selectionToken or query/i.test(text)) return "missing_selection_input";
  if (/invalid selectiontoken/i.test(text)) return "invalid_selection_token";
  if (/jobId is required/i.test(text)) return "missing_job_id";
  if (/Unknown jobId:/i.test(text)) return "unknown_job_id";
  if (/remote queue is full/i.test(text)) return "remote_queue_full";
  if (/remote queue wait aborted/i.test(text)) return "remote_queue_aborted";
  if (/cacheOnly=true prevented remote access/i.test(text)) return "cache_only_miss";
  if (/No matching search result found/i.test(text)) return "no_matching_search_result";
  if (/profile path does not exist/i.test(text)) return "profile_missing";
  if (/profile path exists but appears empty or uninitialized/i.test(text))
    return "profile_uninitialized";
  if (/profile path exists but has no persisted cookies yet/i.test(text))
    return "profile_missing_cookies";
  if (/Book fetch setup is incomplete/i.test(text)) return "login_required";
  if (/HTTP download failed with status/i.test(text)) return "http_download_failed";
  if (/Redirect response missing location header/i.test(text)) return "http_redirect_invalid";
  if (/timed out waiting for browser download/i.test(text)) return "browser_download_timeout";
  if (/siteId or playbookPath is required/i.test(text)) return "playbook_required";

  return "tool_error";
}

async function runAgentBrowserCommand(binary, args) {
  await new Promise((resolve, reject) => {
    const child = spawn(binary, args, {
      cwd: process.cwd(),
      stdio: "ignore",
    });
    child.once("error", reject);
    child.once("close", (code) => {
      if (code === 0) {
        resolve();
        return;
      }
      reject(new Error(`agent-browser exited with code ${code}`));
    });
  });
}

function chooseReplacementPath({ existingEntry, download }) {
  if (!existingEntry?.filePath) {
    return null;
  }

  const existingExtension = path.extname(existingEntry.filePath).toLowerCase();
  const nextExtension = path.extname(download.filePath || "").toLowerCase();
  if (!existingExtension || !nextExtension || existingExtension !== nextExtension) {
    return null;
  }

  const sameDownloadUrl =
    existingEntry.downloadUrl &&
    download.downloadUrl &&
    existingEntry.downloadUrl === download.downloadUrl;
  if (sameDownloadUrl) {
    return existingEntry.filePath;
  }

  const existingTitle = normalizeText(existingEntry.title);
  const nextTitle = normalizeText(download.title);
  const existingAuthor = normalizeText(existingEntry.author);
  const nextAuthor = normalizeText(download.author);

  if (existingTitle && nextTitle && existingTitle === nextTitle) {
    if (!existingAuthor || !nextAuthor || existingAuthor === nextAuthor) {
      return existingEntry.filePath;
    }
  }

  return null;
}

function normalizeText(value) {
  return String(value || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[^\p{Letter}\p{Number}]+/gu, " ")
    .trim();
}
