import { DatabaseSync } from "node:sqlite";
import { access, readFile } from "node:fs/promises";
import path from "node:path";
import { debug } from "./debug.mjs";
import {
  formatPreferenceScore,
  languagePreferenceScore,
  normalizeLanguage,
} from "./playbook-runtime-utils.mjs";

export const CACHE_DB_FILE = ".openclaw-book-cache.sqlite";
const LEGACY_INDEX_FILE = ".openclaw-book-cache.json";
const CACHE_MATCH_THRESHOLD = 60;
const PREFERRED_EXTENSIONS = new Set([
  ".epub",
  ".pdf",
  ".mobi",
  ".azw3",
  ".djvu",
  ".txt",
  ".zip",
  ".fb2",
  ".rtf",
]);

export async function openCacheStore({ libraryRoot }) {
  const dbPath = path.join(libraryRoot, CACHE_DB_FILE);
  const db = new DatabaseSync(dbPath);
  try {
    db.exec("PRAGMA journal_mode = WAL;");
    db.exec("PRAGMA synchronous = NORMAL;");
    db.exec("PRAGMA foreign_keys = ON;");
    db.exec("PRAGMA busy_timeout = 5000;");
    ensureSchema(db);
    const migration = await migrateLegacyJsonIfNeeded(db, libraryRoot);

    return {
      dbPath,
      migration,
      getEntryCount() {
        return Number(db.prepare("SELECT COUNT(*) AS count FROM cache_entries").get().count ?? 0);
      },
      async auditConsistency({ repair = false, maxIssues = 50 } = {}) {
        return auditConsistency(db, libraryRoot, { repair, maxIssues });
      },
      async findCacheHit({ query, titleHint, authorHint, languageHint }) {
        const rows = loadCandidateRows(db, { query, titleHint, authorHint, languageHint });
        const candidates = [];
        const staleIds = [];
        for (const row of rows) {
          if (
            row?.file_path &&
            isPathInsideRoot(row.file_path, libraryRoot) &&
            (await exists(row.file_path))
          ) {
            candidates.push(deserializeEntry(row));
          } else if (row?.id) {
            staleIds.push(row.id);
          }
        }

        if (staleIds.length > 0) {
          removeEntryIdsSync(db, staleIds);
          debug("pruned stale cache entries", {
            dbPath,
            removed: staleIds.length,
          });
        }

        const ranked = candidates
          .map((candidate) => ({
            candidate,
            score: scoreCandidate(candidate, { query, titleHint, authorHint, languageHint }),
          }))
          .filter((entry) => entry.score >= CACHE_MATCH_THRESHOLD)
          .sort((left, right) => right.score - left.score);

        return ranked[0]?.candidate ?? null;
      },
      upsert(entry) {
        if (!isPathInsideRoot(entry.filePath, libraryRoot)) {
          throw new Error(`Refusing to cache file outside libraryRoot: ${entry.filePath}`);
        }
        upsertEntrySync(db, entry);
      },
      recordDownloadSample(sample) {
        insertDownloadHistorySync(db, sample);
      },
      estimateDownloadWindow({ playbookId, backend, expectedBytes }) {
        return estimateDownloadWindowSync(db, { playbookId, backend, expectedBytes });
      },
      upsertJob(job) {
        upsertJobSync(db, job);
      },
      getJob(jobId) {
        return loadJobSync(db, jobId);
      },
      getJobCounts() {
        return getJobCountsSync(db);
      },
      pruneJobs({ maxStored = 200, retentionMs = 24 * 60 * 60 * 1000 } = {}) {
        return pruneJobsSync(db, { maxStored, retentionMs });
      },
      recoverInterruptedJobs() {
        return recoverInterruptedJobsSync(db);
      },
      clearAll() {
        clearAllEntriesSync(db);
      },
      close() {
        db.close();
      },
    };
  } catch (error) {
    db.close();
    throw error;
  }
}

function ensureSchema(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS cache_entries (
      id TEXT PRIMARY KEY,
      query TEXT NOT NULL,
      query_norm TEXT NOT NULL,
      title TEXT NOT NULL,
      title_norm TEXT NOT NULL,
      author TEXT NOT NULL DEFAULT '',
      author_norm TEXT NOT NULL DEFAULT '',
      language TEXT NOT NULL DEFAULT '',
      language_norm TEXT NOT NULL DEFAULT '',
      source_url TEXT NOT NULL DEFAULT '',
      download_url TEXT NOT NULL DEFAULT '',
      file_path TEXT NOT NULL UNIQUE,
      file_stem_norm TEXT NOT NULL DEFAULT '',
      aliases_json TEXT NOT NULL DEFAULT '[]',
      downloaded_at TEXT NOT NULL,
      playbook_id TEXT NOT NULL DEFAULT '',
      playbook_path TEXT NOT NULL DEFAULT ''
    );

    CREATE TABLE IF NOT EXISTS cache_aliases (
      entry_id TEXT NOT NULL REFERENCES cache_entries(id) ON DELETE CASCADE,
      alias TEXT NOT NULL,
      alias_norm TEXT NOT NULL,
      PRIMARY KEY (entry_id, alias_norm)
    );

    CREATE TABLE IF NOT EXISTS download_history (
      id TEXT PRIMARY KEY,
      recorded_at TEXT NOT NULL,
      playbook_id TEXT NOT NULL DEFAULT '',
      backend TEXT NOT NULL DEFAULT '',
      format TEXT NOT NULL DEFAULT '',
      file_size_bytes INTEGER,
      expected_bytes INTEGER,
      predicted_seconds REAL,
      predicted_timeout_ms INTEGER,
      duration_seconds REAL NOT NULL
    );

    CREATE TABLE IF NOT EXISTS jobs (
      id TEXT PRIMARY KEY,
      kind TEXT NOT NULL DEFAULT 'fetch',
      status TEXT NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      params_json TEXT NOT NULL DEFAULT '{}',
      queue_task_id TEXT NOT NULL DEFAULT '',
      queue_kind TEXT NOT NULL DEFAULT '',
      queue_initial_position INTEGER NOT NULL DEFAULT 0,
      queue_tasks_ahead_at_enqueue INTEGER NOT NULL DEFAULT 0,
      result_json TEXT NOT NULL DEFAULT '',
      error_json TEXT NOT NULL DEFAULT ''
    );
  `);

  ensureTableColumn(db, "cache_entries", "language", "TEXT NOT NULL DEFAULT ''");
  ensureTableColumn(db, "cache_entries", "language_norm", "TEXT NOT NULL DEFAULT ''");
  ensureTableColumn(db, "download_history", "expected_bytes", "INTEGER");
  ensureTableColumn(db, "download_history", "predicted_seconds", "REAL");
  ensureTableColumn(db, "download_history", "predicted_timeout_ms", "INTEGER");

  db.exec(`
    CREATE INDEX IF NOT EXISTS idx_cache_entries_title_norm
      ON cache_entries(title_norm);
    CREATE INDEX IF NOT EXISTS idx_cache_entries_author_norm
      ON cache_entries(author_norm);
    CREATE INDEX IF NOT EXISTS idx_cache_entries_language_norm
      ON cache_entries(language_norm);
    CREATE INDEX IF NOT EXISTS idx_cache_entries_query_norm
      ON cache_entries(query_norm);
    CREATE INDEX IF NOT EXISTS idx_cache_entries_file_stem_norm
      ON cache_entries(file_stem_norm);
    CREATE INDEX IF NOT EXISTS idx_cache_entries_playbook_id
      ON cache_entries(playbook_id);
    CREATE INDEX IF NOT EXISTS idx_cache_entries_downloaded_at
      ON cache_entries(downloaded_at DESC);

    CREATE INDEX IF NOT EXISTS idx_cache_aliases_alias_norm
      ON cache_aliases(alias_norm);

    CREATE INDEX IF NOT EXISTS idx_download_history_playbook_backend_recorded_at
      ON download_history(playbook_id, backend, recorded_at DESC);
    CREATE INDEX IF NOT EXISTS idx_jobs_status_updated_at
      ON jobs(status, updated_at DESC);
  `);
}

async function migrateLegacyJsonIfNeeded(db, libraryRoot) {
  const currentCount = Number(db.prepare("SELECT COUNT(*) AS count FROM cache_entries").get().count ?? 0);
  if (currentCount > 0) {
    return { migrated: false, imported: 0, legacyPath: null };
  }

  const legacyPath = path.join(libraryRoot, LEGACY_INDEX_FILE);
  const legacyEntries = await loadLegacyEntries(legacyPath);
  if (legacyEntries.length === 0) {
    return { migrated: false, imported: 0, legacyPath };
  }

  for (const entry of legacyEntries) {
    upsertEntrySync(db, entry);
  }
  debug("migrated legacy cache index", {
    legacyPath,
    dbPath: db.name,
    imported: legacyEntries.length,
  });
  return { migrated: true, imported: legacyEntries.length, legacyPath };
}

async function loadLegacyEntries(legacyPath) {
  try {
    const raw = await readFile(legacyPath, "utf8");
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object" || !Array.isArray(parsed.entries)) {
      return [];
    }
    return parsed.entries.filter((entry) => entry && typeof entry === "object");
  } catch {
    return [];
  }
}

async function auditConsistency(db, libraryRoot, { repair = false, maxIssues = 50 } = {}) {
  const rows = db.prepare(`
    SELECT id, title, author, file_path
    FROM cache_entries
    ORDER BY downloaded_at DESC
  `).all();

  const issues = [];
  const invalidIds = [];
  let validEntries = 0;

  for (const row of rows) {
    const reason = await getConsistencyIssueReason(row, libraryRoot);
    if (!reason) {
      validEntries += 1;
      continue;
    }

    invalidIds.push(row.id);
    if (issues.length < maxIssues) {
      issues.push({
        id: row.id,
        title: row.title,
        author: row.author,
        filePath: row.file_path,
        reason,
      });
    }
  }

  if (repair && invalidIds.length > 0) {
    removeEntryIdsSync(db, invalidIds);
  }

  return {
    checkedEntries: rows.length,
    validEntries,
    invalidEntries: invalidIds.length,
    removedEntries: repair ? invalidIds.length : 0,
    issues,
  };
}

function loadCandidateRows(db, { query, titleHint, authorHint, languageHint }) {
  const exactTerms = compactUnique([normalizeText(titleHint), normalizeText(query)]);
  const broadTerms = compactUnique([...exactTerms, ...tokenize(titleHint), ...tokenize(query)]);
  const authorNorm = normalizeText(authorHint);
  const languageNorm = normalizeLanguage(languageHint);

  const rows = [];
  const seenIds = new Set();
  appendUnique(rows, seenIds, runExactCandidateQuery(db, exactTerms, authorNorm, languageNorm));
  if (rows.length < 50) {
    appendUnique(rows, seenIds, runBroadCandidateQuery(db, broadTerms, authorNorm, languageNorm));
  }
  return rows;
}

function runExactCandidateQuery(db, exactTerms, authorNorm, languageNorm) {
  const clauses = [];
  const params = [];

  for (const term of exactTerms) {
    clauses.push("e.title_norm = ?", "e.query_norm = ?", "e.file_stem_norm = ?", "a.alias_norm = ?");
    params.push(term, term, term, term);
  }
  if (authorNorm) {
    clauses.push("e.author_norm = ?");
    params.push(authorNorm);
  }
  if (languageNorm) {
    clauses.push("e.language_norm = ?");
    params.push(languageNorm);
  }
  if (clauses.length === 0) {
    return [];
  }

  const sql = `
    SELECT DISTINCT e.*
    FROM cache_entries e
    LEFT JOIN cache_aliases a ON a.entry_id = e.id
    WHERE ${clauses.join(" OR ")}
    ORDER BY e.downloaded_at DESC
    LIMIT 100
  `;
  return db.prepare(sql).all(...params);
}

function runBroadCandidateQuery(db, broadTerms, authorNorm, languageNorm) {
  const clauses = [];
  const params = [];

  for (const term of broadTerms) {
    const pattern = `%${term}%`;
    clauses.push("e.title_norm LIKE ?", "e.query_norm LIKE ?", "e.file_stem_norm LIKE ?", "a.alias_norm LIKE ?");
    params.push(pattern, pattern, pattern, pattern);
  }
  if (authorNorm) {
    clauses.push("e.author_norm LIKE ?");
    params.push(`%${authorNorm}%`);
  }
  if (languageNorm) {
    clauses.push("e.language_norm = ?");
    params.push(languageNorm);
  }
  if (clauses.length === 0) {
    return [];
  }

  const sql = `
    SELECT DISTINCT e.*
    FROM cache_entries e
    LEFT JOIN cache_aliases a ON a.entry_id = e.id
    WHERE ${clauses.join(" OR ")}
    ORDER BY e.downloaded_at DESC
    LIMIT 200
  `;
  return db.prepare(sql).all(...params);
}

function appendUnique(target, seenIds, rows) {
  for (const row of rows) {
    if (!row?.id || seenIds.has(row.id)) {
      continue;
    }
    seenIds.add(row.id);
    target.push(row);
  }
}

async function getConsistencyIssueReason(row, libraryRoot) {
  const filePath = String(row?.file_path ?? "").trim();
  if (!filePath) {
    return "missing_file_path";
  }
  if (!isPathInsideRoot(filePath, libraryRoot)) {
    return "outside_library_root";
  }
  if (!(await exists(filePath))) {
    return "missing_file";
  }
  return "";
}

function deserializeEntry(row) {
  return {
    id: row.id,
    query: row.query,
    title: row.title,
    author: row.author,
    language: row.language ?? "",
    sourceUrl: row.source_url,
    downloadUrl: row.download_url,
    filePath: row.file_path,
    aliases: parseAliases(row.aliases_json),
    downloadedAt: row.downloaded_at,
    playbookId: row.playbook_id,
    playbookPath: row.playbook_path,
  };
}

function upsertEntrySync(db, entry) {
  const normalized = normalizeEntry(entry);
  const currentRows = db.prepare("SELECT * FROM cache_entries").all();
  const replacedIds = currentRows
    .map(deserializeEntry)
    .filter((current) => shouldReplaceCacheEntry(current, normalized))
    .map((current) => current.id);

  withTransaction(db, () => {
    for (const id of replacedIds) {
      db.prepare("DELETE FROM cache_entries WHERE id = ?").run(id);
    }

    db.prepare(`
      INSERT INTO cache_entries (
        id, query, query_norm, title, title_norm, author, author_norm,
        language, language_norm,
        source_url, download_url, file_path, file_stem_norm, aliases_json,
        downloaded_at, playbook_id, playbook_path
      ) VALUES (
        @id, @query, @queryNorm, @title, @titleNorm, @author, @authorNorm,
        @language, @languageNorm,
        @sourceUrl, @downloadUrl, @filePath, @fileStemNorm, @aliasesJson,
        @downloadedAt, @playbookId, @playbookPath
      )
    `).run({
      id: normalized.id,
      query: normalized.query,
      queryNorm: normalizeText(normalized.query),
      title: normalized.title,
      titleNorm: normalizeText(normalized.title),
      author: normalized.author,
      authorNorm: normalizeText(normalized.author),
      language: normalized.language,
      languageNorm: normalizeLanguage(normalized.language),
      sourceUrl: normalized.sourceUrl,
      downloadUrl: normalized.downloadUrl,
      filePath: normalized.filePath,
      fileStemNorm: normalizeText(
        path.basename(normalized.filePath, path.extname(normalized.filePath)),
      ),
      aliasesJson: JSON.stringify(normalized.aliases),
      downloadedAt: normalized.downloadedAt,
      playbookId: normalized.playbookId,
      playbookPath: normalized.playbookPath,
    });

    const insertAlias = db.prepare(`
      INSERT OR REPLACE INTO cache_aliases (entry_id, alias, alias_norm)
      VALUES (?, ?, ?)
    `);
    for (const alias of normalized.aliases) {
      const aliasNorm = normalizeText(alias);
      if (!aliasNorm) {
        continue;
      }
      insertAlias.run(normalized.id, alias, aliasNorm);
    }
  });
}

function removeEntryIdsSync(db, ids) {
  const uniqueIds = [...new Set(ids.filter((value) => typeof value === "string" && value))];
  if (uniqueIds.length === 0) {
    return;
  }
  withTransaction(db, () => {
    const statement = db.prepare("DELETE FROM cache_entries WHERE id = ?");
    for (const id of uniqueIds) {
      statement.run(id);
    }
  });
}

function clearAllEntriesSync(db) {
  withTransaction(db, () => {
    db.exec("DELETE FROM jobs;");
    db.exec("DELETE FROM download_history;");
    db.exec("DELETE FROM cache_aliases;");
    db.exec("DELETE FROM cache_entries;");
  });
}

function insertDownloadHistorySync(db, sample) {
  const normalized = normalizeDownloadSample(sample);
  if (!normalized) {
    return;
  }

  db.prepare(`
    INSERT INTO download_history (
      id, recorded_at, playbook_id, backend, format, file_size_bytes,
      expected_bytes, predicted_seconds, predicted_timeout_ms, duration_seconds
    ) VALUES (
      @id, @recordedAt, @playbookId, @backend, @format, @fileSizeBytes,
      @expectedBytes, @predictedSeconds, @predictedTimeoutMs, @durationSeconds
    )
  `).run(normalized);
}

function upsertJobSync(db, job) {
  const normalized = normalizeJob(job);
  db.prepare(`
    INSERT INTO jobs (
      id, kind, status, created_at, updated_at, params_json,
      queue_task_id, queue_kind, queue_initial_position, queue_tasks_ahead_at_enqueue,
      result_json, error_json
    ) VALUES (
      @id, @kind, @status, @createdAt, @updatedAt, @paramsJson,
      @queueTaskId, @queueKind, @queueInitialPosition, @queueTasksAheadAtEnqueue,
      @resultJson, @errorJson
    )
    ON CONFLICT(id) DO UPDATE SET
      kind = excluded.kind,
      status = excluded.status,
      created_at = excluded.created_at,
      updated_at = excluded.updated_at,
      params_json = excluded.params_json,
      queue_task_id = excluded.queue_task_id,
      queue_kind = excluded.queue_kind,
      queue_initial_position = excluded.queue_initial_position,
      queue_tasks_ahead_at_enqueue = excluded.queue_tasks_ahead_at_enqueue,
      result_json = excluded.result_json,
      error_json = excluded.error_json
  `).run(normalized);
}

function loadJobSync(db, jobId) {
  const row = db.prepare(`
    SELECT *
    FROM jobs
    WHERE id = ?
    LIMIT 1
  `).get(String(jobId ?? "").trim());
  return row ? deserializeJob(row) : null;
}

function getJobCountsSync(db) {
  const rows = db.prepare(`
    SELECT status, COUNT(*) AS count
    FROM jobs
    GROUP BY status
  `).all();
  const counts = {
    total: 0,
    queued: 0,
    running: 0,
    completed: 0,
    failed: 0,
    submitted: 0,
  };
  for (const row of rows) {
    const status = String(row.status ?? "");
    const count = Number(row.count ?? 0);
    counts.total += count;
    if (status in counts) {
      counts[status] = count;
    }
  }
  return counts;
}

function pruneJobsSync(db, { maxStored, retentionMs }) {
  const max = Number.isInteger(maxStored) && maxStored > 0 ? maxStored : 200;
  const keepSince = new Date(Date.now() - Math.max(0, Number(retentionMs) || 0)).toISOString();
  const removable = db.prepare(`
    SELECT id
    FROM jobs
    WHERE status IN ('completed', 'failed') AND updated_at < ?
    ORDER BY updated_at ASC
  `).all(keepSince);

  withTransaction(db, () => {
    const removeStmt = db.prepare("DELETE FROM jobs WHERE id = ?");
    for (const row of removable) {
      removeStmt.run(row.id);
    }

    const total = Number(db.prepare("SELECT COUNT(*) AS count FROM jobs").get().count ?? 0);
    const overflow = Math.max(0, total - max);
    if (overflow > 0) {
      const extraRows = db.prepare(`
        SELECT id
        FROM jobs
        WHERE status IN ('completed', 'failed')
        ORDER BY updated_at ASC
        LIMIT ?
      `).all(overflow);
      for (const row of extraRows) {
        removeStmt.run(row.id);
      }
    }
  });
}

function recoverInterruptedJobsSync(db) {
  const now = new Date().toISOString();
  const errorPayload = JSON.stringify({
    ok: false,
    found: false,
    reason: "job_interrupted",
    errorMessage: "The OpenClaw runtime restarted before this job completed.",
  });
  const result = db.prepare(`
    UPDATE jobs
    SET status = 'failed',
        updated_at = ?,
        error_json = ?,
        queue_task_id = '',
        queue_kind = ''
    WHERE status IN ('submitted', 'queued', 'running')
  `).run(now, errorPayload);
  return Number(result.changes ?? 0);
}

function estimateDownloadWindowSync(db, { playbookId, backend, expectedBytes }) {
  const scopedRows = loadDownloadHistoryRows(db, { playbookId, backend });
  if (scopedRows.length === 0) {
    return null;
  }

  const predictedSeconds = predictDownloadSeconds(scopedRows, expectedBytes);
  if (!Number.isFinite(predictedSeconds) || predictedSeconds <= 0) {
    return null;
  }

  const errorBias = estimatePredictionErrorBias(scopedRows);
  const adjustedSeconds = predictedSeconds * errorBias;
  const timeoutSeconds = Math.max(adjustedSeconds * 1.35 + 8, adjustedSeconds + 10);
  return {
    predictedSeconds: roundNumber(adjustedSeconds, 2),
    rawPredictedSeconds: roundNumber(predictedSeconds, 2),
    errorBias: roundNumber(errorBias, 2),
    timeoutMs: Math.round(timeoutSeconds * 1000),
    sampleCount: scopedRows.length,
    strategy: expectedBytes ? "size_weighted_history" : "median_history",
  };
}

function withTransaction(db, callback) {
  db.exec("BEGIN IMMEDIATE");
  try {
    const result = callback();
    db.exec("COMMIT");
    return result;
  } catch (error) {
    try {
      db.exec("ROLLBACK");
    } catch {}
    throw error;
  }
}

function normalizeEntry(entry) {
  return {
    id: String(entry.id),
    query: String(entry.query ?? "").trim(),
    title: String(entry.title ?? "").trim(),
    author: String(entry.author ?? "").trim(),
    language: String(entry.language ?? "").trim(),
    sourceUrl: String(entry.sourceUrl ?? "").trim(),
    downloadUrl: String(entry.downloadUrl ?? "").trim(),
    filePath: String(entry.filePath ?? "").trim(),
    aliases: compactUnique([entry.query, entry.title, ...(entry.aliases ?? [])]),
    downloadedAt: String(entry.downloadedAt ?? new Date().toISOString()),
    playbookId: String(entry.playbookId ?? "").trim(),
    playbookPath: String(entry.playbookPath ?? "").trim(),
  };
}

function normalizeDownloadSample(sample) {
  const durationSeconds = Number(sample?.durationSeconds);
  if (!Number.isFinite(durationSeconds) || durationSeconds <= 0) {
    return null;
  }

  const fileSizeBytes = Number(sample?.fileSizeBytes);
  return {
    id: String(sample?.id || cryptoRandomId()),
    recordedAt: String(sample?.recordedAt || new Date().toISOString()),
    playbookId: String(sample?.playbookId ?? "").trim(),
    backend: String(sample?.backend ?? "").trim(),
    format: String(sample?.format ?? "").trim(),
    fileSizeBytes:
      Number.isFinite(fileSizeBytes) && fileSizeBytes > 0 ? Math.round(fileSizeBytes) : null,
    expectedBytes:
      Number.isFinite(Number(sample?.expectedBytes)) && Number(sample?.expectedBytes) > 0
        ? Math.round(Number(sample.expectedBytes))
        : null,
    predictedSeconds:
      Number.isFinite(Number(sample?.predictedSeconds)) && Number(sample?.predictedSeconds) > 0
        ? Number(sample.predictedSeconds)
        : null,
    predictedTimeoutMs:
      Number.isFinite(Number(sample?.predictedTimeoutMs)) && Number(sample?.predictedTimeoutMs) > 0
        ? Math.round(Number(sample.predictedTimeoutMs))
        : null,
    durationSeconds,
  };
}

function normalizeJob(job) {
  return {
    id: String(job?.id ?? "").trim(),
    kind: String(job?.kind ?? "fetch").trim() || "fetch",
    status: String(job?.status ?? "submitted").trim() || "submitted",
    createdAt: String(job?.createdAt ?? new Date().toISOString()),
    updatedAt: String(job?.updatedAt ?? new Date().toISOString()),
    paramsJson: safeJson(job?.params ?? {}),
    queueTaskId: String(job?.queueTaskId ?? "").trim(),
    queueKind: String(job?.queueKind ?? "").trim(),
    queueInitialPosition:
      Number.isInteger(job?.queueInitialPosition) && job.queueInitialPosition >= 0
        ? job.queueInitialPosition
        : 0,
    queueTasksAheadAtEnqueue:
      Number.isInteger(job?.queueTasksAheadAtEnqueue) && job.queueTasksAheadAtEnqueue >= 0
        ? job.queueTasksAheadAtEnqueue
        : 0,
    resultJson: job?.result ? safeJson(job.result) : "",
    errorJson: job?.error ? safeJson(job.error) : "",
  };
}

function deserializeJob(row) {
  return {
    id: String(row.id ?? ""),
    kind: String(row.kind ?? "fetch"),
    status: String(row.status ?? "submitted"),
    createdAt: String(row.created_at ?? ""),
    updatedAt: String(row.updated_at ?? ""),
    params: parseJsonObject(row.params_json, {}),
    queueTaskId: String(row.queue_task_id ?? ""),
    queueKind: String(row.queue_kind ?? ""),
    queueInitialPosition: Number(row.queue_initial_position ?? 0) || 0,
    queueTasksAheadAtEnqueue: Number(row.queue_tasks_ahead_at_enqueue ?? 0) || 0,
    result: parseJsonObject(row.result_json, null),
    error: parseJsonObject(row.error_json, null),
  };
}

function shouldReplaceCacheEntry(current, entry) {
  if (!current || typeof current !== "object") {
    return false;
  }

  if (current.filePath === entry.filePath) {
    return true;
  }

  const samePlaybook =
    !current.playbookId || !entry.playbookId || current.playbookId === entry.playbookId;
  if (!samePlaybook) {
    return false;
  }

  const currentTitle = normalizeText(current.title);
  const entryTitle = normalizeText(entry.title);
  const currentAuthor = normalizeText(current.author);
  const entryAuthor = normalizeText(entry.author);
  const currentLanguage = normalizeLanguage(current.language);
  const entryLanguage = normalizeLanguage(entry.language);

  if (currentLanguage && entryLanguage && currentLanguage !== entryLanguage) {
    return false;
  }

  if (currentTitle && entryTitle && currentTitle === entryTitle) {
    if (!currentAuthor || !entryAuthor || currentAuthor === entryAuthor) {
      return true;
    }
  }

  const currentAliases = new Set(
    compactUnique([current.query, current.title, ...(current.aliases ?? [])]).map(normalizeText),
  );
  const entryAliases = compactUnique([entry.query, entry.title, ...(entry.aliases ?? [])]).map(normalizeText);
  const aliasOverlap = entryAliases.some((alias) => alias && currentAliases.has(alias));

  if (aliasOverlap && (!currentAuthor || !entryAuthor || currentAuthor === entryAuthor)) {
    return true;
  }

  return false;
}

function loadDownloadHistoryRows(db, { playbookId, backend }) {
  const byBackend = db.prepare(`
    SELECT duration_seconds, file_size_bytes, expected_bytes, predicted_seconds, predicted_timeout_ms
    FROM download_history
    WHERE playbook_id = ? AND backend = ?
    ORDER BY recorded_at DESC
    LIMIT 30
  `).all(String(playbookId ?? ""), String(backend ?? ""));

  if (byBackend.length >= 3) {
    return byBackend;
  }

  return db.prepare(`
    SELECT duration_seconds, file_size_bytes, expected_bytes, predicted_seconds, predicted_timeout_ms
    FROM download_history
    WHERE playbook_id = ?
    ORDER BY recorded_at DESC
    LIMIT 30
  `).all(String(playbookId ?? ""));
}

function predictDownloadSeconds(rows, expectedBytes) {
  const durations = rows
    .map((row) => Number(row.duration_seconds))
    .filter((value) => Number.isFinite(value) && value > 0);
  if (durations.length === 0) {
    return null;
  }

  const expectedMb =
    Number.isFinite(expectedBytes) && expectedBytes > 0 ? expectedBytes / (1024 * 1024) : null;
  const sizedRows = rows
    .map((row) => ({
      durationSeconds: Number(row.duration_seconds),
      sizeMb:
        Number.isFinite(Number(row.file_size_bytes)) && Number(row.file_size_bytes) > 0
          ? Number(row.file_size_bytes) / (1024 * 1024)
          : null,
    }))
    .filter(
      (row) =>
        Number.isFinite(row.durationSeconds) &&
        row.durationSeconds > 0 &&
        Number.isFinite(row.sizeMb) &&
        row.sizeMb > 0,
    );

  if (expectedMb && expectedMb > 0 && sizedRows.length >= 3) {
    const secondsPerMb = median(
      sizedRows.map((row) => row.durationSeconds / Math.max(row.sizeMb, 0.1)),
    );
    return secondsPerMb * expectedMb;
  }

  return median(durations);
}

function estimatePredictionErrorBias(rows) {
  const ratios = rows
    .map((row) => {
      const predicted = Number(row.predicted_seconds);
      const actual = Number(row.duration_seconds);
      if (!Number.isFinite(predicted) || predicted <= 0) return null;
      if (!Number.isFinite(actual) || actual <= 0) return null;
      return Math.min(2.5, Math.max(0.6, actual / predicted));
    })
    .filter((value) => Number.isFinite(value));

  return ratios.length > 0 ? median(ratios) : 1;
}

function scoreCandidate(candidate, { query, titleHint, authorHint, languageHint }) {
  const haystacks = compactUnique([
    candidate.title,
    candidate.author,
    path.basename(candidate.filePath, path.extname(candidate.filePath)),
    ...(candidate.aliases ?? []),
  ]).map(normalizeText);

  const title = normalizeText(candidate.title);
  const author = normalizeText(candidate.author);
  const queryNorm = normalizeText(query);
  const titleHintNorm = normalizeText(titleHint);
  const authorHintNorm = normalizeText(authorHint);
  const candidateLanguageNorm = normalizeLanguage(candidate.language);
  const languageHintNorm = normalizeLanguage(languageHint);

  let score = 0;
  if (titleHintNorm && title === titleHintNorm) score += 120;
  if (authorHintNorm && author.includes(authorHintNorm)) score += 45;
  score += languagePreferenceScore(candidateLanguageNorm, languageHintNorm);
  if (title.includes(queryNorm)) score += 100;
  if (haystacks.some((entry) => entry.includes(queryNorm))) score += 70;
  score += tokenize(query).filter((token) => haystacks.some((entry) => entry.includes(token))).length * 14;
  score += extensionScore(candidate.filePath);
  return score;
}

function extensionScore(filePath) {
  const extension = path.extname(filePath || "").toLowerCase();
  if (!extension) return 0;
  if (extension === ".bin") return -25;
  if (!PREFERRED_EXTENSIONS.has(extension)) return 0;
  return formatPreferenceScore(extension, "");
}

function parseAliases(raw) {
  try {
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed.filter((entry) => typeof entry === "string") : [];
  } catch {
    return [];
  }
}

function compactUnique(values) {
  return [
    ...new Set(
      values
        .filter((value) => typeof value === "string" && value.trim())
        .map((value) => value.trim()),
    ),
  ];
}

function normalizeText(value) {
  return String(value || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[^\p{Letter}\p{Number}]+/gu, " ")
    .trim();
}

function median(values) {
  const sorted = values
    .filter((value) => Number.isFinite(value))
    .sort((left, right) => left - right);
  if (sorted.length === 0) return null;
  const middle = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 1) {
    return sorted[middle];
  }
  return (sorted[middle - 1] + sorted[middle]) / 2;
}

function roundNumber(value, digits) {
  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
}

function cryptoRandomId() {
  return `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}

function safeJson(value) {
  try {
    return JSON.stringify(value);
  } catch {
    return "{}";
  }
}

function parseJsonObject(raw, fallback) {
  if (typeof raw !== "string" || !raw.trim()) {
    return fallback;
  }
  try {
    return JSON.parse(raw);
  } catch {
    return fallback;
  }
}

function ensureTableColumn(db, tableName, columnName, columnSpec) {
  const rows = db.prepare(`PRAGMA table_info(${tableName})`).all();
  if (rows.some((row) => row?.name === columnName)) {
    return;
  }
  db.exec(`ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${columnSpec};`);
}

function tokenize(value) {
  return normalizeText(value)
    .split(/\s+/)
    .filter((token) => token.length >= 2);
}

function isPathInsideRoot(filePath, libraryRoot) {
  const resolvedRoot = path.resolve(libraryRoot);
  const resolvedFile = path.resolve(filePath || "");
  return resolvedFile === resolvedRoot || resolvedFile.startsWith(`${resolvedRoot}${path.sep}`);
}

async function exists(targetPath) {
  try {
    await access(targetPath);
    return true;
  } catch {
    return false;
  }
}
