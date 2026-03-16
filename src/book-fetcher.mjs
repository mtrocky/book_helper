import { randomUUID } from "node:crypto";
import { access, copyFile, mkdir, readdir, rename, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import {
  executeAgentBrowserPlaybook,
  scoreSearchResult,
  searchAgentBrowserPlaybook,
} from "./agent-browser-playbook.mjs";
import { openCacheStore } from "./cache-store.mjs";
import { debug } from "./debug.mjs";
import {
  describeStatus as describePlaybookStatus,
  parsePluginConfig,
  resolvePlaybook,
} from "./playbooks.mjs";

export const DEFAULT_TOOL_NAME = "library_book_fetch";
export const TOOL_GROUP = "group:plugins";

const DEFAULT_DOWNLOAD_TIMEOUT_MS = 120000;

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
      list.includes(DEFAULT_TOOL_NAME) ||
      list.includes("library-fetcher") ||
      list.includes(TOOL_GROUP)
    ) {
      return { nextConfig: current, changed: false, policyPath: pathName };
    }
  }

  if (Array.isArray(tools.allow)) {
    const allow = cloneStringList(tools.allow);
    allow.push(DEFAULT_TOOL_NAME);
    return {
      nextConfig: {
        ...current,
        tools: {
          ...tools,
          allow,
        },
      },
      changed: true,
      policyPath: `tools.allow (${DEFAULT_TOOL_NAME})`,
    };
  }

  const alsoAllow = cloneStringList(tools.alsoAllow);
  alsoAllow.push(DEFAULT_TOOL_NAME);
  return {
    nextConfig: {
      ...current,
      tools: {
        ...tools,
        alsoAllow,
      },
    },
    changed: true,
    policyPath: `tools.alsoAllow (${DEFAULT_TOOL_NAME})`,
  };
}

export async function describeStatus(pluginConfig, toolPolicySource = null) {
  return describePlaybookStatus(pluginConfig, toolPolicySource);
}

export async function auditLibraryCache(rawParams, rawPluginConfig) {
  const params =
    rawParams && typeof rawParams === "object" && !Array.isArray(rawParams) ? rawParams : {};
  const pluginConfig = parsePluginConfig(rawPluginConfig);
  const libraryRoot = path.resolve(
    params.libraryRoot ?? pluginConfig.defaultLibraryRoot ?? path.join(process.cwd(), "library"),
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
    return {
      libraryRoot,
      cacheDbPath: cacheStore.dbPath,
      migratedLegacyJson: cacheStore.migration.migrated,
      importedLegacyEntries: cacheStore.migration.imported,
      repair,
      ...audit,
    };
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
    params.libraryRoot ?? pluginConfig.defaultLibraryRoot ?? path.join(process.cwd(), "library"),
  );

  await mkdir(libraryRoot, { recursive: true });
  const entries = await readdir(libraryRoot, { withFileTypes: true });
  const removedPaths = [];
  for (const entry of entries) {
    const targetPath = path.join(libraryRoot, entry.name);
    await rm(targetPath, { recursive: true, force: true });
    removedPaths.push(targetPath);
  }

  return {
    libraryRoot,
    removedCount: removedPaths.length,
    removedPaths,
  };
}

export async function lookupCachedBook(rawParams, rawPluginConfig) {
  const startedAt = Date.now();
  const params = validateParams(rawParams);
  const pluginConfig = parsePluginConfig(rawPluginConfig);
  const libraryRoot = path.resolve(
    params.libraryRoot ?? pluginConfig.defaultLibraryRoot ?? path.join(process.cwd(), "library"),
  );

  await mkdir(libraryRoot, { recursive: true });
  const cacheStore = await openCacheStore({ libraryRoot });
  try {
    const cacheHit = await cacheStore.findCacheHit({
      query: params.query,
      titleHint: params.titleHint,
      authorHint: params.authorHint,
    });
    if (!cacheHit) {
      return {
        found: false,
        backend: "cache",
        libraryRoot,
        elapsedSeconds: roundElapsedSeconds(startedAt),
      };
    }

    return {
      found: true,
      backend: "cache",
      title: cacheHit.title,
      author: cacheHit.author,
      filePath: cacheHit.filePath,
      libraryRoot,
      sourceUrl: sanitizeExternalUrl(cacheHit.sourceUrl),
      downloadUrl: cacheHit.downloadUrl,
      playbookId: cacheHit.playbookId ?? "",
      elapsedSeconds: roundElapsedSeconds(startedAt),
    };
  } finally {
    cacheStore.close();
  }
}

export async function searchBooks(rawParams, rawPluginConfig, signal) {
  const startedAt = Date.now();
  const params = validateParams(rawParams);
  const pluginConfig = parsePluginConfig(rawPluginConfig);
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
  const tempRoot = path.join(os.tmpdir(), `openclaw-library-search-${randomUUID()}`);
  await mkdir(tempRoot, { recursive: true });

  try {
    const result = await searchAgentBrowserPlaybook({
      params,
      pluginConfig,
      playbook,
      tempRoot,
      downloadTimeoutMs,
      signal,
    });
    const results = (result.results ?? [])
      .map((entry, index) => formatSearchResult(entry, {
        params,
        playbookId: playbook.id,
        playbookPath,
        resultIndex: index + 1,
      }))
      .sort((left, right) => right.score - left.score);

    return {
      found: results.length > 0,
      backend: "agent-browser-search",
      query: params.query,
      titleHint: params.titleHint,
      authorHint: params.authorHint,
      playbookId: playbook.id,
      playbookPath,
      results,
      currentUrl: result.currentUrl,
      currentTitle: result.currentTitle,
      llmFallbackUsed: result.llmFallbackUsed,
      elapsedSeconds: roundElapsedSeconds(startedAt),
    };
  } finally {
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

export async function fetchBookToLibrary(rawParams, rawPluginConfig, signal) {
  const startedAt = Date.now();
  const params = validateParams(rawParams);
  const pluginConfig = parsePluginConfig(rawPluginConfig);
  debug("starting fetch", {
    query: params.query,
    titleHint: params.titleHint,
    authorHint: params.authorHint,
    siteId: params.siteId,
    playbookPath: params.playbookPath,
    libraryRoot: params.libraryRoot,
    timeoutMs: params.timeoutMs,
    forceRefresh: params.forceRefresh,
    cacheOnly: params.cacheOnly,
    keepSessionOnError: params.keepSessionOnError,
  });
  const libraryRoot = path.resolve(
    params.libraryRoot ?? pluginConfig.defaultLibraryRoot ?? path.join(process.cwd(), "library"),
  );
  const downloadTimeoutMs = params.timeoutMs ?? pluginConfig.downloadTimeoutMs ?? DEFAULT_DOWNLOAD_TIMEOUT_MS;
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
    });

    if (!params.forceRefresh) {
      const cacheHit = existingCacheEntry;
      if (cacheHit) {
        debug("cache hit", {
          title: cacheHit.title,
          filePath: cacheHit.filePath,
        });
        return {
          fromCache: true,
          backend: "cache",
          title: cacheHit.title,
          author: cacheHit.author,
          filePath: cacheHit.filePath,
          libraryRoot,
          sourceUrl: sanitizeExternalUrl(cacheHit.sourceUrl),
          downloadUrl: cacheHit.downloadUrl,
          playbookId: cacheHit.playbookId ?? "",
          elapsedSeconds: roundElapsedSeconds(startedAt),
        };
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

    const tempRoot = path.join(os.tmpdir(), `openclaw-library-fetch-${randomUUID()}`);
    await mkdir(tempRoot, { recursive: true });

    try {
      const download = await executeAgentBrowserPlaybook({
        params,
        pluginConfig,
        playbook,
        tempRoot,
        downloadTimeoutMs,
        signal,
      });
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
        sourceUrl: download.sourceUrl ?? "",
        downloadUrl: download.downloadUrl ?? "",
        filePath: finalPath,
        aliases: compactUnique([params.query, params.titleHint, download.title]),
        downloadedAt: new Date().toISOString(),
        playbookId: playbook.id,
        playbookPath,
      };
      cacheStore.upsert(entry);
      debug("updated cache store", {
        cacheDbPath: cacheStore.dbPath,
        entries: cacheStore.getEntryCount(),
      });

      return {
        fromCache: false,
        backend: "agent-browser",
        title: download.title,
        author: download.author,
        filePath: finalPath,
        libraryRoot,
        sourceUrl: sanitizeExternalUrl(download.sourceUrl),
        downloadUrl: download.downloadUrl,
        playbookId: playbook.id,
        llmFallbackUsed: download.llmFallbackUsed,
        elapsedSeconds: roundElapsedSeconds(startedAt),
      };
    } finally {
      debug("cleaning temp root", { tempRoot });
      await rm(tempRoot, { recursive: true, force: true });
    }
  } finally {
    cacheStore.close();
  }
}

function validateParams(rawParams) {
  if (!rawParams || typeof rawParams !== "object" || Array.isArray(rawParams)) {
    throw new Error("Tool parameters must be an object.");
  }

  const params = rawParams;
  const query = typeof params.query === "string" ? params.query.trim() : "";
  if (!query) {
    throw new Error("query is required.");
  }
  const explicitTitleHint =
    typeof params.titleHint === "string" ? params.titleHint.trim() : "";

  return {
    query,
    titleHint: explicitTitleHint || query,
    authorHint: typeof params.authorHint === "string" ? params.authorHint.trim() : "",
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

function roundElapsedSeconds(startedAt) {
  return Math.round(((Date.now() - startedAt) / 1000) * 100) / 100;
}

function formatSearchResult(entry, { params, playbookId, playbookPath, resultIndex }) {
  const score = scoreSearchResult(entry, params);
  return {
    title: entry.title,
    author: entry.author,
    publisher: entry.publisher ?? "",
    format: entry.format ?? "",
    resultIndex,
    score,
    selectionToken: encodeSelectionToken({
      query: params.query,
      titleHint: params.titleHint,
      authorHint: params.authorHint,
      playbookId,
      playbookPath,
      resultIndex,
      title: entry.title,
      author: entry.author ?? "",
      publisher: entry.publisher ?? "",
    }),
  };
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
