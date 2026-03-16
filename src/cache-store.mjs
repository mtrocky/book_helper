import { DatabaseSync } from "node:sqlite";
import { access, readFile } from "node:fs/promises";
import path from "node:path";
import { debug } from "./debug.mjs";

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
      async findCacheHit({ query, titleHint, authorHint }) {
        const rows = loadCandidateRows(db, { query, titleHint, authorHint });
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
            score: scoreCandidate(candidate, { query, titleHint, authorHint }),
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
      source_url TEXT NOT NULL DEFAULT '',
      download_url TEXT NOT NULL DEFAULT '',
      file_path TEXT NOT NULL UNIQUE,
      file_stem_norm TEXT NOT NULL DEFAULT '',
      aliases_json TEXT NOT NULL DEFAULT '[]',
      downloaded_at TEXT NOT NULL,
      playbook_id TEXT NOT NULL DEFAULT '',
      playbook_path TEXT NOT NULL DEFAULT ''
    );

    CREATE INDEX IF NOT EXISTS idx_cache_entries_title_norm
      ON cache_entries(title_norm);
    CREATE INDEX IF NOT EXISTS idx_cache_entries_author_norm
      ON cache_entries(author_norm);
    CREATE INDEX IF NOT EXISTS idx_cache_entries_query_norm
      ON cache_entries(query_norm);
    CREATE INDEX IF NOT EXISTS idx_cache_entries_file_stem_norm
      ON cache_entries(file_stem_norm);
    CREATE INDEX IF NOT EXISTS idx_cache_entries_playbook_id
      ON cache_entries(playbook_id);
    CREATE INDEX IF NOT EXISTS idx_cache_entries_downloaded_at
      ON cache_entries(downloaded_at DESC);

    CREATE TABLE IF NOT EXISTS cache_aliases (
      entry_id TEXT NOT NULL REFERENCES cache_entries(id) ON DELETE CASCADE,
      alias TEXT NOT NULL,
      alias_norm TEXT NOT NULL,
      PRIMARY KEY (entry_id, alias_norm)
    );

    CREATE INDEX IF NOT EXISTS idx_cache_aliases_alias_norm
      ON cache_aliases(alias_norm);
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

function loadCandidateRows(db, { query, titleHint, authorHint }) {
  const exactTerms = compactUnique([normalizeText(titleHint), normalizeText(query)]);
  const broadTerms = compactUnique([...exactTerms, ...tokenize(titleHint), ...tokenize(query)]);
  const authorNorm = normalizeText(authorHint);

  const rows = [];
  const seenIds = new Set();
  appendUnique(rows, seenIds, runExactCandidateQuery(db, exactTerms, authorNorm));
  if (rows.length < 50) {
    appendUnique(rows, seenIds, runBroadCandidateQuery(db, broadTerms, authorNorm));
  }
  return rows;
}

function runExactCandidateQuery(db, exactTerms, authorNorm) {
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

function runBroadCandidateQuery(db, broadTerms, authorNorm) {
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
        source_url, download_url, file_path, file_stem_norm, aliases_json,
        downloaded_at, playbook_id, playbook_path
      ) VALUES (
        @id, @query, @queryNorm, @title, @titleNorm, @author, @authorNorm,
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
    db.exec("DELETE FROM cache_aliases;");
    db.exec("DELETE FROM cache_entries;");
  });
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
    sourceUrl: String(entry.sourceUrl ?? "").trim(),
    downloadUrl: String(entry.downloadUrl ?? "").trim(),
    filePath: String(entry.filePath ?? "").trim(),
    aliases: compactUnique([entry.query, entry.title, ...(entry.aliases ?? [])]),
    downloadedAt: String(entry.downloadedAt ?? new Date().toISOString()),
    playbookId: String(entry.playbookId ?? "").trim(),
    playbookPath: String(entry.playbookPath ?? "").trim(),
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

function scoreCandidate(candidate, { query, titleHint, authorHint }) {
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

  let score = 0;
  if (titleHintNorm && title === titleHintNorm) score += 120;
  if (authorHintNorm && author.includes(authorHintNorm)) score += 45;
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
  if (PREFERRED_EXTENSIONS.has(extension)) return 10;
  return 0;
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
