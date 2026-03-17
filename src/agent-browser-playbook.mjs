import { randomUUID } from "node:crypto";
import { spawn } from "node:child_process";
import { access, mkdir, readdir, stat } from "node:fs/promises";
import path from "node:path";
import { debug, info } from "./debug.mjs";
import { loadAgentBrowserSessionConfig } from "./playbooks.mjs";
import {
  buildSafeFileName,
  estimateDownloadTimeoutMs,
  guessExtension,
  parseSizeHintToBytes,
  pickResult,
  scoreResult,
} from "./playbook-runtime-utils.mjs";
import {
  executePlaywrightZLibraryPlaybook,
  searchPlaywrightZLibraryPlaybook,
  supportsPlaywrightZLibrary,
} from "./playwright-zlibrary.mjs";

const PROGRESS_LOG_INTERVAL_MS = 2000;

export async function searchAgentBrowserPlaybook({
  params,
  pluginConfig,
  playbook,
  tempRoot,
  downloadTimeoutMs,
  downloadTimingModel,
  signal,
}) {
  const browserOptions = await createBrowserOptions({
    pluginConfig,
    playbook,
    tempRoot,
    signal,
  });
  await validateBrowserBootstrap(browserOptions);

  if (supportsPlaywrightZLibrary(playbook)) {
    return searchPlaywrightZLibraryPlaybook({
      params,
      playbook,
      browserOptions,
      downloadTimeoutMs,
      signal,
      onBrowserError: (message) => buildAgentBrowserError(message, browserOptions),
    });
  }

  const state = {
    results: [],
    selectedResult: null,
    detail: {},
    currentUrl: "",
    currentTitle: "",
    llmFallbackUsed: false,
    fallback: {},
    failureDiagnostics: null,
    downloadMetrics: null,
  };

  try {
    for (const step of playbook.steps) {
      await executeStep({
        step,
        state,
        params,
        playbook,
        pluginConfig,
        browserOptions,
        tempRoot,
        downloadTimeoutMs,
      });

      if (step.type === "extract-results") {
        return {
          results: state.results,
          currentUrl: state.currentUrl,
          currentTitle: state.currentTitle,
          llmFallbackUsed: state.llmFallbackUsed,
        };
      }
    }
  } catch (error) {
    await collectFailureDiagnostics(state, browserOptions, downloadTimeoutMs);
    throw error;
  } finally {
    await runAgentBrowser(["close"], browserOptions, 15000).catch(() => {});
  }

  throw new Error(`Playbook "${playbook.id}" does not define an extract-results step.`);
}

export async function executeAgentBrowserPlaybook({
  params,
  pluginConfig,
  playbook,
  tempRoot,
  downloadTimeoutMs,
  downloadTimingModel,
  signal,
}) {
  const browserOptions = await createBrowserOptions({
    pluginConfig,
    playbook,
    tempRoot,
    signal,
  });
  await validateBrowserBootstrap(browserOptions);

  if (supportsPlaywrightZLibrary(playbook)) {
    return executePlaywrightZLibraryPlaybook({
      params,
      playbook,
      browserOptions,
      tempRoot,
      downloadTimeoutMs,
      downloadTimingModel,
      signal,
      onBrowserError: (message) => buildAgentBrowserError(message, browserOptions),
    });
  }

  const state = {
    results: [],
    selectedResult: null,
    detail: {},
    currentUrl: "",
    currentTitle: "",
    llmFallbackUsed: false,
    fallback: {},
    failureDiagnostics: null,
    downloadMetrics: null,
  };

  try {
    for (const step of playbook.steps) {
      await executeStep({
        step,
        state,
        params,
        playbook,
        pluginConfig,
        browserOptions,
        tempRoot,
        downloadTimeoutMs,
        downloadTimingModel,
      });
    }
  } catch (error) {
    await collectFailureDiagnostics(state, browserOptions, downloadTimeoutMs);
    throw error;
  } finally {
    if (!(params.keepSessionOnError && state.failureDiagnostics)) {
      await runAgentBrowser(["close"], browserOptions, 15000).catch(() => {});
    } else {
      debug("preserving browser session after failure", {
        sessionName: browserOptions.sessionName,
      });
    }
  }

  if (!state.downloadedFilePath) {
    throw new Error(`Playbook "${playbook.id}" finished without downloading a file.`);
  }

  return {
    filePath: state.downloadedFilePath,
    title:
      state.detail.title ??
      state.selectedResult?.title ??
      params.titleHint ??
      params.query,
    author: state.detail.author ?? state.selectedResult?.author ?? "",
    sourceUrl: normalizeResultSourceUrl(state.currentUrl, state.selectedResult?.href),
    downloadUrl: state.detail.downloadUrl ?? state.selectedResult?.downloadUrl ?? "",
    llmFallbackUsed: state.llmFallbackUsed,
    downloadMetrics: state.downloadMetrics ?? null,
  };
}

export function scoreSearchResult(result, params) {
  return scoreResult(result, params);
}

async function executeStep({
  step,
  state,
  params,
  playbook,
  pluginConfig,
  browserOptions,
  tempRoot,
  downloadTimeoutMs,
  downloadTimingModel,
}) {
  const context = buildTemplateContext(params, state);
  debug(`step ${step.name} (${step.type})`);

  switch (step.type) {
    case "open": {
      await runAgentBrowser(["open", renderString(step.url, context)], browserOptions, downloadTimeoutMs);
      state.currentUrl = await readCurrentUrl(browserOptions, downloadTimeoutMs);
      state.currentTitle = await readCurrentTitle(browserOptions, downloadTimeoutMs).catch(() => "");
      return;
    }
    case "wait": {
      const target = step.selector ? renderString(step.selector, context) : String(step.ms ?? 1000);
      await runAgentBrowser(["wait", target], browserOptions, downloadTimeoutMs);
      return;
    }
    case "fill": {
      const value = renderString(step.value ?? params.query, context);
      await runAgentBrowser(
        ["fill", renderString(step.selector, context), value],
        browserOptions,
        downloadTimeoutMs,
      );
      return;
    }
    case "click": {
      await runAgentBrowser(["click", renderString(step.selector, context)], browserOptions, downloadTimeoutMs);
      state.currentUrl = await readCurrentUrl(browserOptions, downloadTimeoutMs);
      return;
    }
    case "press": {
      await runAgentBrowser(["press", step.key ?? "Enter"], browserOptions, downloadTimeoutMs);
      return;
    }
    case "command": {
      const args = Array.isArray(step.args)
        ? step.args.map((entry) => renderString(String(entry), context))
        : [];
      await runAgentBrowser([step.command, ...args], browserOptions, downloadTimeoutMs);
      return;
    }
    case "extract-results": {
      try {
        state.results = await extractResults(step, browserOptions, downloadTimeoutMs);
        debug("extracted results", {
          count: Array.isArray(state.results) ? state.results.length : 0,
          preview: Array.isArray(state.results) ? state.results.slice(0, 5) : [],
        });
      } catch (error) {
        const fallback = await tryLlmFallback({
          kind: "extract-results",
          error,
          step,
          state,
          params,
          playbook,
          pluginConfig,
          browserOptions,
          downloadTimeoutMs,
        });
        applyFallback(state, fallback);
        if (!fallback?.selectedResultUrl && !fallback?.selectedResultIndex && !Array.isArray(fallback?.results)) {
          throw error;
        }
        if (Array.isArray(fallback.results)) {
          state.results = fallback.results;
        }
      }
      return;
    }
    case "open-result": {
      let selectedResult = state.selectedResult ?? pickResult(state.results, params);
      if (!selectedResult || step.useLlmSelection) {
        const fallback = await tryLlmFallback({
          kind: "select-result",
          error: selectedResult ? new Error("LLM selection requested by playbook.") : new Error("No matching result."),
          step,
          state,
          params,
          playbook,
          pluginConfig,
          browserOptions,
          downloadTimeoutMs,
        });
        applyFallback(state, fallback);
        selectedResult = resolveResultFromFallback(state.results, fallback) ?? selectedResult;
      }

      if (!selectedResult) {
        throw new Error(`No matching search result found for "${params.query}".`);
      }

      state.selectedResult = selectedResult;
      await openResultTarget(selectedResult.href, browserOptions, downloadTimeoutMs);
      state.currentUrl = await readCurrentUrl(browserOptions, downloadTimeoutMs);
      state.currentTitle = await readCurrentTitle(browserOptions, downloadTimeoutMs).catch(() => "");
      return;
    }
    case "download-result": {
      let selectedResult = state.selectedResult ?? pickResult(state.results, params);
      debug("selected result before fallback", selectedResult ?? null);
      if (
        !selectedResult ||
        (!selectedResult.downloadUrl && !selectedResult.downloadRef) ||
        step.useLlmSelection
      ) {
        const fallback = await tryLlmFallback({
          kind: "download-result",
          error: new Error(
            !selectedResult
              ? "No matching result."
              : !selectedResult.downloadUrl && !selectedResult.downloadRef
              ? "Selected result is missing a direct download target."
              : "LLM selection requested by playbook.",
          ),
          step,
          state,
          params,
          playbook,
          pluginConfig,
          browserOptions,
          downloadTimeoutMs,
        });
        applyFallback(state, fallback);
        selectedResult = resolveResultFromFallback(state.results, fallback) ?? selectedResult;
        if (fallback?.downloadUrl) {
          selectedResult = {
            ...(selectedResult ?? {
              title: params.titleHint || params.query,
              author: params.authorHint || "",
              href: "",
            }),
            downloadUrl: fallback.downloadUrl,
          };
        }
      }

      if (!selectedResult) {
        throw new Error(`No matching search result found for "${params.query}".`);
      }

      if (!selectedResult?.downloadUrl && !selectedResult?.downloadRef) {
        throw new Error(`No direct download target found for "${params.query}".`);
      }

      state.selectedResult = selectedResult;
      debug("selected result for direct download", selectedResult);
      info("download candidate selected", {
        title: selectedResult.title ?? params.titleHint ?? params.query,
        author: selectedResult.author ?? "",
        publisher: selectedResult.publisher ?? "",
        format: selectedResult.format ?? "",
      });
      const titleForPath = selectedResult.title ?? params.titleHint ?? params.query;
      const startedFiles = await listFiles(browserOptions.downloadDir);

      if (selectedResult.downloadRef) {
        await runDownloadCommand({
          selector: selectedResult.downloadRef,
          browserOptions,
          timeoutMs: downloadTimeoutMs,
        });
        state.currentUrl = await readCurrentUrl(browserOptions, downloadTimeoutMs).catch(() => "");
        state.currentTitle = await readCurrentTitle(browserOptions, downloadTimeoutMs).catch(() => "");
      } else {
        await runAgentBrowser(["open", selectedResult.downloadUrl], browserOptions, downloadTimeoutMs);
        state.currentUrl = await readCurrentUrl(browserOptions, downloadTimeoutMs).catch(() => "");
        state.currentTitle = await readCurrentTitle(browserOptions, downloadTimeoutMs).catch(() => "");
      }

      const timing = await selectAdaptiveDownloadTiming({
        baseTimeoutMs: downloadTimeoutMs,
        playbookId: playbook.id,
        backend: "agent-browser",
        format: selectedResult.format,
        downloadTimingModel,
      });
      const effectiveDownloadTimeoutMs = timing.effectiveTimeoutMs;
      info("download timeout selected", {
        title: titleForPath,
        baseTimeoutMs: downloadTimeoutMs,
        effectiveTimeoutMs: effectiveDownloadTimeoutMs,
        format: selectedResult.format ?? "",
        estimatedBytes: timing.expectedBytes,
        modelPredictedSeconds: timing.model?.predictedSeconds ?? null,
        modelRawPredictedSeconds: timing.model?.rawPredictedSeconds ?? null,
        modelErrorBias: timing.model?.errorBias ?? null,
        modelSampleCount: timing.model?.sampleCount ?? 0,
      });

      const extension = guessExtension({
        downloadUrl: selectedResult.downloadUrl,
        format: selectedResult.format,
      });
      const downloadTargetPath = path.join(tempRoot, buildSafeFileName(titleForPath, extension));
      const downloadStartedAt = Date.now();

      state.downloadedFilePath = await resolveDownloadedFile({
        targetPath: downloadTargetPath,
        reportedPath: null,
        downloadDir: browserOptions.downloadDir,
        beforeFiles: startedFiles,
        timeoutMs: effectiveDownloadTimeoutMs,
        expectedBytes: timing.expectedBytes,
      });
      state.downloadMetrics = {
        backend: "agent-browser",
        format: selectedResult.format ?? "",
        fileSizeBytes: await readFileSizeSafe(state.downloadedFilePath),
        expectedBytes: timing.expectedBytes,
        predictedSeconds: timing.model?.predictedSeconds ?? null,
        predictedTimeoutMs: effectiveDownloadTimeoutMs,
        durationSeconds: roundNumber((Date.now() - downloadStartedAt) / 1000, 2),
      };
      info("download completed via browser-managed path", {
        path: state.downloadedFilePath,
      });
      return;
    }
    case "extract-detail": {
      try {
        state.detail = await extractDetail(step, browserOptions, downloadTimeoutMs);
      } catch (error) {
        const fallback = await tryLlmFallback({
          kind: "extract-detail",
          error,
          step,
          state,
          params,
          playbook,
          pluginConfig,
          browserOptions,
          downloadTimeoutMs,
        });
        applyFallback(state, fallback);
        if (fallback?.metadata) {
          state.detail = { ...state.detail, ...fallback.metadata };
        } else {
          throw error;
        }
      }
      return;
    }
    case "download": {
      const titleForPath =
        state.detail.title ?? state.selectedResult?.title ?? params.titleHint ?? params.query;
      const extension = guessExtension(state.detail);
      const downloadTargetPath = path.join(tempRoot, buildSafeFileName(titleForPath, extension));
      const startedFiles = await listFiles(browserOptions.downloadDir);
      let reportedDownloadPath = null;
      const timing = await selectAdaptiveDownloadTiming({
        baseTimeoutMs: downloadTimeoutMs,
        playbookId: playbook.id,
        backend: "agent-browser",
        format: state.detail.format,
        downloadTimingModel,
      });
      const effectiveDownloadTimeoutMs = timing.effectiveTimeoutMs;
      info("download step timeout selected", {
        title: titleForPath,
        baseTimeoutMs: downloadTimeoutMs,
        effectiveTimeoutMs: effectiveDownloadTimeoutMs,
        format: state.detail.format ?? "",
        estimatedBytes: timing.expectedBytes,
        modelPredictedSeconds: timing.model?.predictedSeconds ?? null,
        modelRawPredictedSeconds: timing.model?.rawPredictedSeconds ?? null,
        modelErrorBias: timing.model?.errorBias ?? null,
        modelSampleCount: timing.model?.sampleCount ?? 0,
      });
      const downloadStartedAt = Date.now();

      try {
        const selector =
          state.fallback.downloadSelector ??
          step.selector ??
          state.detail.downloadSelector;
        reportedDownloadPath = await runDownloadCommand({
          selector,
          browserOptions,
          timeoutMs: effectiveDownloadTimeoutMs,
        });
      } catch (error) {
        const fallback = await tryLlmFallback({
          kind: "download",
          error,
          step,
          state,
          params,
          playbook,
          pluginConfig,
          browserOptions,
          downloadTimeoutMs: effectiveDownloadTimeoutMs,
          downloadTargetPath,
        });
        applyFallback(state, fallback);

        if (Array.isArray(fallback?.commands) && fallback.commands.length > 0) {
          await runFallbackCommands({
            commands: fallback.commands,
            browserOptions,
            downloadTimeoutMs: effectiveDownloadTimeoutMs,
            context: buildTemplateContext(params, state, { downloadTargetPath }),
          });
        }

        if (fallback?.downloadSelector) {
          reportedDownloadPath = await runDownloadCommand({
            selector: fallback.downloadSelector,
            browserOptions,
            timeoutMs: effectiveDownloadTimeoutMs,
          });
        } else if (!fallback?.commands?.length) {
          throw error;
        }
      }

      state.currentUrl = await readCurrentUrl(browserOptions, downloadTimeoutMs);
      state.downloadedFilePath = await resolveDownloadedFile({
        targetPath: downloadTargetPath,
        reportedPath: reportedDownloadPath,
        downloadDir: browserOptions.downloadDir,
        beforeFiles: startedFiles,
        timeoutMs: effectiveDownloadTimeoutMs,
        expectedBytes: timing.expectedBytes,
      });
      state.downloadMetrics = {
        backend: "agent-browser",
        format: state.detail.format ?? "",
        fileSizeBytes: await readFileSizeSafe(state.downloadedFilePath),
        expectedBytes: timing.expectedBytes,
        predictedSeconds: timing.model?.predictedSeconds ?? null,
        predictedTimeoutMs: effectiveDownloadTimeoutMs,
        durationSeconds: roundNumber((Date.now() - downloadStartedAt) / 1000, 2),
      };
      info("download completed via browser-managed path", {
        path: state.downloadedFilePath,
      });
      return;
    }
    default: {
      throw new Error(`Unsupported playbook step type: ${step.type}`);
    }
  }
}

async function createBrowserOptions({ pluginConfig, playbook, tempRoot, signal }) {
  const downloadDir = path.join(tempRoot, "downloads");
  await mkdir(downloadDir, { recursive: true });

  const sessionRuntime = await loadAgentBrowserSessionConfig(pluginConfig);
  const resolvedSessionName =
    sessionRuntime.config.sessionName ??
    playbook.browser.sessionName ??
    `library-fetch-${randomUUID()}`;
  const resolvedProfilePath =
    sessionRuntime.config.profilePath ??
    pluginConfig.browserProfilePath ??
    playbook.browser.profilePath;

  return {
    binary: pluginConfig.agentBrowserPath ?? "agent-browser",
    sessionName: resolvedSessionName,
    profilePath: resolvedProfilePath,
    downloadDir,
    headed: sessionRuntime.config.headed ?? playbook.browser.headed,
    ignoreHttpsErrors:
      sessionRuntime.config.ignoreHttpsErrors ?? playbook.browser.ignoreHttpsErrors,
    proxy: sessionRuntime.config.proxy ?? playbook.browser.proxy,
    sessionConfigPath: sessionRuntime.sessionConfigPath,
    sessionConfigExists: sessionRuntime.exists,
    initUrl: deriveInitUrl(playbook),
    requireAuthenticatedProfile: Boolean(playbook.browser.requireAuthenticatedProfile),
    signal,
  };
}

async function extractResults(step, browserOptions, timeoutMs) {
  if (step.snapshotStrategy === "download-list") {
    const snapshot = await readSnapshot(browserOptions, timeoutMs);
    return parseDownloadListSnapshot(snapshot);
  }
  const payload = await runAgentBrowser(
    ["eval", buildExtractResultsScript(step)],
    browserOptions,
    timeoutMs,
  );
  return JSON.parse(payload);
}

async function extractDetail(step, browserOptions, timeoutMs) {
  const payload = await runAgentBrowser(
    ["eval", buildExtractDetailScript(step)],
    browserOptions,
    timeoutMs,
  );
  return JSON.parse(payload);
}

async function openResultTarget(target, browserOptions, timeoutMs) {
  const value = String(target || "").trim();
  if (!value) {
    throw new Error("Missing result target.");
  }
  if (value.startsWith("@")) {
    await runAgentBrowser(["click", value], browserOptions, timeoutMs);
    return;
  }
  await runAgentBrowser(["open", value], browserOptions, timeoutMs);
}

async function runDownloadCommand({ selector, browserOptions, timeoutMs, targetPath = null }) {
  if (!selector || typeof selector !== "string") {
    throw new Error("No download selector is available for this step.");
  }

  if (targetPath) {
    await runAgentBrowser(["download", selector, targetPath], browserOptions, timeoutMs);
    return targetPath;
  }

  await runAgentBrowser(["click", selector], browserOptions, timeoutMs);
  return null;
}

async function runFallbackCommands({ commands, browserOptions, downloadTimeoutMs, context }) {
  for (const commandSpec of commands) {
    if (!commandSpec || typeof commandSpec !== "object" || typeof commandSpec.command !== "string") {
      continue;
    }
    const args = Array.isArray(commandSpec.args)
      ? commandSpec.args.map((entry) => renderString(String(entry), context))
      : [];
    await runAgentBrowser([commandSpec.command, ...args], browserOptions, downloadTimeoutMs);
  }
}

async function tryLlmFallback({
  kind,
  error,
  step,
  state,
  params,
  playbook,
  pluginConfig,
  browserOptions,
  downloadTimeoutMs,
  downloadTargetPath,
}) {
  const command = playbook.llmFallback.command ?? pluginConfig.llmFallbackCommand;
  const args = playbook.llmFallback.args ?? pluginConfig.llmFallbackArgs ?? [];
  if (!command || playbook.llmFallback.enabled === false) {
    return null;
  }

  const payload = {
    schemaVersion: 1,
    kind,
    goal:
      playbook.llmFallback.goal ??
      "Repair a failed browser step in a book download workflow.",
    notes: playbook.llmFallback.notes,
    query: params.query,
    titleHint: params.titleHint,
    authorHint: params.authorHint,
    step,
    error: error instanceof Error ? error.message : String(error),
    state: {
      results: state.results,
      selectedResult: state.selectedResult,
      detail: state.detail,
      currentUrl: await readCurrentUrl(browserOptions, downloadTimeoutMs).catch(() => ""),
      currentTitle: await readCurrentTitle(browserOptions, downloadTimeoutMs).catch(() => ""),
      snapshot: await readSnapshot(browserOptions, downloadTimeoutMs).catch(() => ""),
    },
    playbook: {
      id: playbook.id,
      name: playbook.name,
      path: playbook.path,
      notes: playbook.notes,
    },
    runtime: {
      downloadTargetPath,
    },
  };

  const { stdout, stderr, code } = await spawnCommand(command, args, {
    cwd: process.cwd(),
    timeoutMs: downloadTimeoutMs,
    input: `${JSON.stringify(payload, null, 2)}\n`,
    signal: browserOptions.signal,
  });

  if (code !== 0) {
    throw new Error(`LLM fallback command failed: ${stderr.trim() || stdout.trim() || `exit code ${code}`}`);
  }

  const response = JSON.parse(stdout);
  if (response && typeof response === "object") {
    state.llmFallbackUsed = true;
  }
  return response;
}

async function collectFailureDiagnostics(state, browserOptions, timeoutMs) {
  const diagnostics = {
    url: await readCurrentUrl(browserOptions, timeoutMs).catch(() => state.currentUrl || ""),
    title: await readCurrentTitle(browserOptions, timeoutMs).catch(() => state.currentTitle || ""),
    snapshot: await readSnapshot(browserOptions, timeoutMs).catch(() => ""),
  };
  state.failureDiagnostics = diagnostics;
  debug("failure diagnostics", diagnostics);
}

function applyFallback(state, response) {
  if (!response || typeof response !== "object") {
    return;
  }

  if (response.downloadSelector) {
    state.fallback.downloadSelector = response.downloadSelector;
  }
  if (response.metadata && typeof response.metadata === "object") {
    state.detail = { ...state.detail, ...response.metadata };
  }
}

function resolveResultFromFallback(results, response) {
  if (!response || typeof response !== "object") {
    return null;
  }

  if (Number.isInteger(response.selectedResultIndex) && response.selectedResultIndex > 0) {
    return results[response.selectedResultIndex - 1] ?? null;
  }

  if (typeof response.selectedResultUrl === "string" && response.selectedResultUrl) {
    return (
      results.find((entry) => entry.href === response.selectedResultUrl) ?? {
        title: response.metadata?.title ?? response.selectedResultUrl,
        author: response.metadata?.author ?? "",
        href: response.selectedResultUrl,
      }
    );
  }

  return null;
}

function buildExtractResultsScript(step) {
  return `(() => {
    const cfg = ${JSON.stringify({
      itemSelector: step.itemSelector,
      titleSelector: step.titleSelector,
      authorSelector: step.authorSelector,
      detailLinkSelector: step.detailLinkSelector,
      downloadSelector: step.downloadSelector,
      publisherSelector: step.publisherSelector,
      formatSelector: step.formatSelector,
    })};
    const resolveNode = (root, selector) => {
      if (!selector) return null;
      if (selector === "self") return root;
      const nextMatch = selector.match(/^__nextLink(?:\\((\\d+)\\))?__$/);
      if (nextMatch) {
        let remaining = Number(nextMatch[1] || "1");
        let current = root;
        while (current && remaining > 0) {
          current = current.nextElementSibling;
          while (current && current.tagName !== "A") current = current.nextElementSibling;
          if (current) remaining -= 1;
        }
        return current || null;
      }
      const prevMatch = selector.match(/^__prevLink(?:\\((\\d+)\\))?__$/);
      if (prevMatch) {
        let remaining = Number(prevMatch[1] || "1");
        let current = root;
        while (current && remaining > 0) {
          current = current.previousElementSibling;
          while (current && current.tagName !== "A") current = current.previousElementSibling;
          if (current) remaining -= 1;
        }
        return current || null;
      }
      return root.querySelector(selector);
    };
    const readText = (root, selector) => {
      if (!selector) return "";
      const node = resolveNode(root, selector);
      return node?.textContent?.trim() ?? "";
    };
    const readHref = (root, selector) => {
      if (!selector) return "";
      const node = resolveNode(root, selector);
      const value = node?.getAttribute?.("href");
      if (!value) return "";
      try {
        return new URL(value, window.location.href).toString();
      } catch {
        return value;
      }
    };
    const results = Array.from(document.querySelectorAll(cfg.itemSelector))
      .map((node) => ({
        title: readText(node, cfg.titleSelector),
        author: readText(node, cfg.authorSelector),
        href: readHref(node, cfg.detailLinkSelector),
        downloadUrl: readHref(node, cfg.downloadSelector),
        publisher: readText(node, cfg.publisherSelector),
        format: readText(node, cfg.formatSelector),
      }))
      .filter((item) => item.title && (item.href || item.downloadUrl));
    return JSON.stringify(results);
  })()`;
}

function buildExtractDetailScript(step) {
  return `(() => {
    const cfg = ${JSON.stringify({
      titleSelector: step.titleSelector,
      authorSelector: step.authorSelector,
      formatSelector: step.formatSelector,
      downloadSelector: step.downloadSelector,
    })};
    const readText = (selector) => {
      if (!selector) return "";
      return document.querySelector(selector)?.textContent?.trim() ?? "";
    };
    const node = cfg.downloadSelector ? document.querySelector(cfg.downloadSelector) : null;
    const rawHref = node?.getAttribute?.("href") ?? "";
    const downloadUrl = rawHref ? new URL(rawHref, window.location.href).toString() : "";
    return JSON.stringify({
      title: readText(cfg.titleSelector),
      author: readText(cfg.authorSelector),
      format: readText(cfg.formatSelector),
      downloadSelector: cfg.downloadSelector,
      downloadUrl,
    });
  })()`;
}

function parseDownloadListSnapshot(snapshot) {
  const lines = String(snapshot ?? "").split("\n");
  const results = [];

  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index];
    const headerMatch = line.match(/^(\s*)- link "(.+?)" \[ref=(e\d+)\]$/);
    if (!headerMatch) {
      continue;
    }

    const [, indent, headerText, headerRef] = headerMatch;
    if (!/\bDownloaded\b/i.test(headerText)) {
      continue;
    }

    const result = {
      title: "",
      author: "",
      href: "",
      downloadUrl: "",
      downloadRef: "",
      summaryRef: `@${headerRef}`,
      summaryText: headerText,
      publisher: "",
      format: "",
    };

    let cursor = index + 1;
    for (; cursor < lines.length; cursor += 1) {
      const childLine = lines[cursor];
      if (!childLine.startsWith(`${indent}  `)) {
        break;
      }
      const childMatch = childLine.match(/^\s*- link(?: "([^"]*)")? \[ref=(e\d+)\]$/);
      if (!childMatch) {
        continue;
      }
      const [, childText = "", childRef] = childMatch;
      if (!childText.trim()) {
        result.downloadRef = `@${childRef}`;
      }
    }

    const metadata = [];
    for (; cursor < lines.length; cursor += 1) {
      const siblingLine = lines[cursor];
      const siblingMatch = siblingLine.match(/^(\s*)- link "(.+?)" \[ref=(e\d+)\]$/);
      if (siblingMatch && siblingMatch[1] === indent && /\bDownloaded\b/i.test(siblingMatch[2])) {
        break;
      }

      if (!siblingMatch || siblingMatch[1] !== indent) {
        continue;
      }

      const [, , siblingText, siblingRef] = siblingMatch;
      if (!result.title) {
        result.title = siblingText.trim();
        result.href = `@${siblingRef}`;
        continue;
      }

      metadata.push(siblingText.trim());
      if (metadata.length >= 2) {
        result.publisher = metadata[0] ?? "";
        result.author = metadata[1] ?? "";
      }
    }

    if (!result.title) {
      result.title = headerText.replace(/\s+Downloaded\s*$/i, "").trim();
    }

    if (result.title && (result.downloadRef || result.href)) {
      results.push(result);
    }

    index = cursor - 1;
  }

  return results;
}

function buildTemplateContext(params, state, extra = {}) {
  return {
    query: params.query,
    queryEncoded: encodeURIComponent(params.query),
    titleHint: params.titleHint ?? "",
    titleHintEncoded: encodeURIComponent(params.titleHint ?? ""),
    authorHint: params.authorHint ?? "",
    authorHintEncoded: encodeURIComponent(params.authorHint ?? ""),
    selectedResultUrl: state.selectedResult?.href ?? "",
    selectedResultTitle: state.selectedResult?.title ?? "",
    selectedResultAuthor: state.selectedResult?.author ?? "",
    currentTitle: state.detail.title ?? "",
    currentAuthor: state.detail.author ?? "",
    downloadSelector: state.fallback.downloadSelector ?? state.detail.downloadSelector ?? "",
    ...extra,
  };
}

function renderString(value, context) {
  return String(value ?? "").replace(/\{\{\s*([a-zA-Z0-9_]+)\s*\}\}/g, (_, key) =>
    key in context ? String(context[key] ?? "") : "",
  );
}

async function readCurrentUrl(browserOptions, timeoutMs) {
  const data = await runAgentBrowser(["get", "url"], browserOptions, timeoutMs);
  return typeof data?.url === "string" ? data.url : "";
}

async function readCurrentTitle(browserOptions, timeoutMs) {
  const data = await runAgentBrowser(["get", "title"], browserOptions, timeoutMs);
  return typeof data?.title === "string" ? data.title : "";
}

async function readSnapshot(browserOptions, timeoutMs) {
  const data = await runAgentBrowser(["snapshot", "-i", "-c", "-d", "4"], browserOptions, timeoutMs);
  return typeof data?.snapshot === "string" ? data.snapshot : "";
}

async function runAgentBrowser(commandArgs, options, timeoutMs) {
  const args = ["--json", "--session", options.sessionName];
  if (options.profilePath) args.push("--profile", options.profilePath);
  if (options.downloadDir) args.push("--download-path", options.downloadDir);
  if (options.headed) args.push("--headed");
  if (options.ignoreHttpsErrors) args.push("--ignore-https-errors");
  if (options.proxy) args.push("--proxy", options.proxy);
  args.push(...commandArgs);
  debug(`agent-browser ${args.join(" ")}`);

  const { stdout, stderr, code } = await spawnCommand(options.binary, args, {
    cwd: process.cwd(),
    timeoutMs,
    signal: options.signal,
  });

  const output = stdout.trim();
  let parsed = null;
  try {
    parsed = output ? JSON.parse(output) : null;
  } catch {
    parsed = null;
  }

  if (code !== 0 || parsed?.success === false) {
    const errorMessage =
      parsed?.error || stderr.trim() || output || `agent-browser ${commandArgs[0]} failed with exit code ${code}`;
    throw buildAgentBrowserError(errorMessage, options);
  }

  if (!parsed || typeof parsed !== "object") {
    debug(`agent-browser raw output: ${output}`);
    return output;
  }

  if (parsed.data && typeof parsed.data === "object" && typeof parsed.data.result === "string") {
    debug(`agent-browser ok ${commandArgs[0]}`);
    return parsed.data.result;
  }

  debug(`agent-browser ok ${commandArgs[0]}`);
  return parsed.data ?? output;
}

async function validateBrowserBootstrap(options) {
  if (!options.profilePath) {
    return;
  }

  if (!(await exists(options.profilePath))) {
    throw buildAgentBrowserError(
      `Configured agent-browser profile path does not exist: ${options.profilePath}`,
      options,
    );
  }

  if (await isDirectoryEmpty(options.profilePath)) {
    throw buildAgentBrowserError(
      `Configured agent-browser profile path exists but appears empty or uninitialized: ${options.profilePath}`,
      options,
    );
  }

  if (
    options.requireAuthenticatedProfile &&
    !(await hasPersistedCookies(options.profilePath))
  ) {
    throw buildAgentBrowserError(
      `Configured agent-browser profile path exists but has no persisted cookies yet: ${options.profilePath}`,
      options,
    );
  }
}

function buildAgentBrowserError(message, options) {
  const normalized = String(message || "").trim();
  if (!shouldSuggestSessionBootstrap(normalized, options)) {
    return new Error(normalized);
  }

  const sessionName = options.sessionName || "clawbot";
  const profilePath =
    options.profilePath || path.join(process.cwd(), "Runtime", "profile");
  const initUrl = options.initUrl || "about:blank";
  const configPath =
    options.sessionConfigPath || path.join(process.cwd(), "Runtime", "agent-browser-session.json");

  return new Error(
    [
      colorize("Book fetch setup is incomplete.", "yellow"),
      "",
      colorize("Problem", "red"),
      colorize(normalized, "red"),
      "",
      colorize("Next Step", "green"),
      "1. If agent-browser says '--profile ignored: daemon already running', stop the daemon first:",
      "",
      colorize("  agent-browser close", "green"),
      "",
      "2. Create a persistent agent-browser session and log in if needed:",
      "",
      colorize(
        `  agent-browser close && agent-browser --headed --session ${sessionName} --profile ${profilePath} open ${initUrl}`,
        "green",
      ),
      "",
      "3. After the session is ready, write the session config:",
      "",
      colorize("Session Config", "cyan"),
      `Run this to write ${configPath}:`,
      "",
      colorize(
        indentBlock(
          [
            `mkdir -p ${path.dirname(configPath)}`,
            `printf '%s\\n' '{' '  "sessionName": ${escapeSingleQuotedJsonValue(sessionName)},' '  "profilePath": ${escapeSingleQuotedJsonValue(profilePath)}' '}' > ${configPath}`,
          ].join("\n"),
          2,
        ),
        "cyan",
      ),
    ].join("\n"),
  );
}

function shouldSuggestSessionBootstrap(message, options) {
  if (options.profilePath && /does not exist/i.test(message)) {
    return true;
  }

  return (
    /profile/i.test(message) ||
    /user data dir/i.test(message) ||
    /failed to launch/i.test(message) ||
    /browser.*closed/i.test(message) ||
    /target page, context or browser has been closed/i.test(message) ||
    /cannot find/i.test(message)
  );
}

function deriveInitUrl(playbook) {
  const openStep = Array.isArray(playbook?.steps)
    ? playbook.steps.find((step) => step?.type === "open" && typeof step?.url === "string")
    : null;
  if (!openStep?.url) {
    return "about:blank";
  }

  try {
    const sanitized = String(openStep.url)
      .replace(/\{\{[^}]+\}\}/g, "test")
      .replace(/\/s\/test(?:%20test)?/i, "/");
    const url = new URL(sanitized);
    return url.origin + "/";
  } catch {
    return openStep.url;
  }
}

function normalizeResultSourceUrl(currentUrl, fallbackHref) {
  const current = String(currentUrl ?? "").trim();
  if (current && !current.startsWith("@")) {
    return current;
  }

  const fallback = String(fallbackHref ?? "").trim();
  if (fallback && !fallback.startsWith("@")) {
    return fallback;
  }

  return "";
}

async function selectAdaptiveDownloadTiming({
  baseTimeoutMs,
  playbookId,
  backend,
  format,
  downloadTimingModel,
}) {
  const expectedBytes = parseSizeHintToBytes(format ?? "");
  const heuristicTimeoutMs = estimateDownloadTimeoutMs({
    baseTimeoutMs,
    format,
  });
  const model =
    typeof downloadTimingModel?.estimateTimeout === "function"
      ? await downloadTimingModel.estimateTimeout({
          playbookId,
          backend,
          expectedBytes,
        })
      : null;

  return {
    expectedBytes,
    heuristicTimeoutMs,
    model,
    effectiveTimeoutMs: Math.max(heuristicTimeoutMs, model?.timeoutMs ?? 0),
  };
}

function indentBlock(value, spaces) {
  const prefix = " ".repeat(spaces);
  return String(value)
    .split("\n")
    .map((line) => `${prefix}${line}`)
    .join("\n");
}

function colorize(value, color) {
  if (!supportsColor()) {
    return String(value);
  }

  const codes = {
    red: "\u001b[31m",
    green: "\u001b[32m",
    yellow: "\u001b[33m",
    cyan: "\u001b[36m",
  };
  const start = codes[color];
  if (!start) {
    return String(value);
  }
  return `${start}${value}\u001b[0m`;
}

function supportsColor() {
  return (
    Boolean(process.stderr?.isTTY) &&
    process.env.NO_COLOR !== "1" &&
    process.env.NO_COLOR !== "true"
  );
}

function escapeSingleQuotedJsonValue(value) {
  return JSON.stringify(String(value)).replace(/'/g, `'\"'\"'`);
}

async function resolveDownloadedFile({
  targetPath,
  reportedPath,
  downloadDir,
  beforeFiles,
  timeoutMs,
  expectedBytes = null,
}) {
  const deadline = Date.now() + timeoutMs;
  const reportProgress = createDownloadProgressReporter({
    channel: "browser-managed",
    targetPath,
    totalBytes: expectedBytes,
  });
  while (Date.now() < deadline) {
    if (reportedPath && (await exists(reportedPath))) {
      reportProgress(await readFileSizeSafe(reportedPath), { force: true });
      return reportedPath;
    }
    if (await exists(targetPath)) {
      reportProgress(await readFileSizeSafe(targetPath), { force: true });
      return targetPath;
    }
    const files = await listFiles(downloadDir);
    for (const file of files) {
      if (!beforeFiles.has(file)) {
        reportProgress(await readFileSizeSafe(file), { force: true });
        return file;
      }
    }
    const progressPath =
      (reportedPath && (await exists(reportedPath)) && reportedPath) ||
      ((await exists(targetPath)) && targetPath) ||
      null;
    if (progressPath) {
      reportProgress(await readFileSizeSafe(progressPath));
    }
    await sleep(750);
  }

  info("browser-managed download timed out", {
    targetPath,
    reportedPath,
    timeoutMs,
    expectedBytes,
  });
  throw new Error(`Timed out waiting for browser download after ${timeoutMs}ms.`);
}

function createDownloadProgressReporter({ channel, targetPath, totalBytes }) {
  const startedAt = Date.now();
  let transferredBytes = 0;
  let lastLogAt = 0;
  let lastLoggedBytes = 0;
  let lastLoggedPercentBucket = -1;

  return (deltaOrAbsoluteBytes, options = {}) => {
    const force = Boolean(options.force);
    if (force) {
      transferredBytes = Math.max(transferredBytes, Number(deltaOrAbsoluteBytes) || 0);
    } else {
      transferredBytes += Math.max(0, Number(deltaOrAbsoluteBytes) || 0);
    }

    const now = Date.now();
    const percent = totalBytes ? Math.min(100, (transferredBytes / totalBytes) * 100) : null;
    const percentBucket = percent === null ? null : Math.floor(percent / 5);
    const shouldLog =
      force ||
      lastLogAt === 0 ||
      (
        now - lastLogAt >= PROGRESS_LOG_INTERVAL_MS &&
        (
          transferredBytes - lastLoggedBytes >= 512 * 1024 ||
          (percentBucket !== null && percentBucket > lastLoggedPercentBucket)
        )
      );

    if (!shouldLog) {
      return;
    }

    const elapsedSeconds = Math.max((now - startedAt) / 1000, 0.001);
    const bytesPerSecond = transferredBytes / elapsedSeconds;
    const etaSeconds =
      totalBytes && bytesPerSecond > 0
        ? Math.max(0, (totalBytes - transferredBytes) / bytesPerSecond)
        : null;

    info("download progress", {
      channel,
      targetPath,
      receivedBytes: transferredBytes,
      totalBytes,
      percent: percent === null ? null : roundNumber(percent, 1),
      speedMbps: roundNumber((bytesPerSecond * 8) / 1_000_000, 2),
      etaSeconds: etaSeconds === null ? null : roundNumber(etaSeconds, 1),
    });

    lastLogAt = now;
    lastLoggedBytes = transferredBytes;
    lastLoggedPercentBucket = percentBucket ?? lastLoggedPercentBucket;
  };
}

async function readFileSizeSafe(targetPath) {
  try {
    const details = await stat(targetPath);
    return details.size;
  } catch {
    return 0;
  }
}

function roundNumber(value, digits) {
  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
}

async function listFiles(dir) {
  const items = await readdir(dir);
  return new Set(items.map((item) => path.join(dir, item)));
}

async function exists(targetPath) {
  try {
    await access(targetPath);
    return true;
  } catch {
    return false;
  }
}

async function isDirectoryEmpty(targetPath) {
  try {
    const entries = await readdir(targetPath);
    return entries.length === 0;
  } catch {
    return false;
  }
}

async function hasPersistedCookies(profilePath) {
  const candidates = [
    path.join(profilePath, "Default", "Cookies"),
    path.join(profilePath, "Default", "Network", "Cookies"),
    path.join(profilePath, "Profile 1", "Cookies"),
    path.join(profilePath, "Profile 1", "Network", "Cookies"),
  ];

  for (const candidate of candidates) {
    if (await exists(candidate)) {
      return true;
    }
  }

  return false;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}


async function spawnCommand(binary, args, { cwd, timeoutMs, signal, input }) {
  return new Promise((resolve, reject) => {
    const child = spawn(binary, args, {
      cwd,
      env: process.env,
      stdio: ["pipe", "pipe", "pipe"],
    });

    let stdout = "";
    let stderr = "";
    let settled = false;

    const timeout = setTimeout(() => {
      if (settled) return;
      settled = true;
      child.kill("SIGTERM");
      reject(new Error(`${binary} ${args[args.length - 1] ?? ""} timed out after ${timeoutMs}ms.`));
    }, timeoutMs);

    const onAbort = () => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      child.kill("SIGTERM");
      reject(new Error("Operation aborted."));
    };

    signal?.addEventListener("abort", onAbort, { once: true });
    child.stdout.on("data", (chunk) => {
      stdout += String(chunk);
    });
    child.stderr.on("data", (chunk) => {
      stderr += String(chunk);
    });
    child.on("error", (error) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      signal?.removeEventListener("abort", onAbort);
      reject(error);
    });
    child.on("close", (code) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      signal?.removeEventListener("abort", onAbort);
      resolve({ stdout, stderr, code: code ?? 0 });
    });

    if (input) {
      child.stdin.write(input);
    }
    child.stdin.end();
  });
}
