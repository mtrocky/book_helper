import { randomUUID } from "node:crypto";
import { spawn } from "node:child_process";
import { createWriteStream } from "node:fs";
import { access, mkdir, readdir } from "node:fs/promises";
import path from "node:path";
import { pipeline } from "node:stream/promises";
import { chromium } from "playwright";
import { debug } from "./debug.mjs";
import { loadAgentBrowserSessionConfig } from "./playbooks.mjs";

const DOWNLOAD_EXTENSIONS = [".pdf", ".epub", ".mobi", ".azw3", ".djvu", ".txt", ".zip"];
const MIN_RESULT_SCORE = 24;

export async function executeAgentBrowserPlaybook({
  params,
  pluginConfig,
  playbook,
  tempRoot,
  downloadTimeoutMs,
  signal,
}) {
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

  const browserOptions = {
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

  await validateBrowserBootstrap(browserOptions);

  const state = {
    results: [],
    selectedResult: null,
    detail: {},
    currentUrl: "",
    currentTitle: "",
    llmFallbackUsed: false,
    fallback: {},
    failureDiagnostics: null,
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
    sourceUrl: state.selectedResult?.href ?? state.currentUrl ?? "",
    downloadUrl: state.detail.downloadUrl ?? state.selectedResult?.downloadUrl ?? "",
    llmFallbackUsed: state.llmFallbackUsed,
  };
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
      await runAgentBrowser(["open", selectedResult.href], browserOptions, downloadTimeoutMs);
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
      const titleForPath =
        selectedResult.title ?? params.titleHint ?? params.query;
      const startedFiles = await listFiles(browserOptions.downloadDir);
      let detailDownload = null;

      if (selectedResult.downloadRef) {
        await runDownloadCommand({
          selector: selectedResult.downloadRef,
          browserOptions,
          timeoutMs: downloadTimeoutMs,
        });
        const immediateDownload = await waitForNewDownloadFile({
          downloadDir: browserOptions.downloadDir,
          beforeFiles: startedFiles,
          timeoutMs: Math.min(downloadTimeoutMs, 2500),
        });
        if (immediateDownload) {
          state.downloadedFilePath = immediateDownload;
          return;
        }

        const detailSnapshot = await readSnapshot(browserOptions, downloadTimeoutMs).catch(() => "");
        detailDownload = parseDetailDownloadRef(detailSnapshot);
        if (detailDownload) {
          debug("detail page download ref", detailDownload);
          const directTarget = await extractDirectDownloadTarget(browserOptions, downloadTimeoutMs).catch(
            () => null,
          );
          if (directTarget?.downloadUrl) {
            debug("detail page direct download target", directTarget);
            selectedResult.downloadUrl = directTarget.downloadUrl;
            selectedResult.format = selectedResult.format || directTarget.format;
          } else {
            await runDownloadCommand({
              selector: detailDownload.selector,
              browserOptions,
              timeoutMs: downloadTimeoutMs,
            });
            selectedResult.format = selectedResult.format || detailDownload.format;
          }
        }
      } else {
        try {
          await runAgentBrowser(["open", selectedResult.downloadUrl], browserOptions, downloadTimeoutMs);
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          if (!message.includes("net::ERR_ABORTED")) {
            throw error;
          }
        }
      }

      const extension = guessExtension({
        downloadUrl: selectedResult.downloadUrl,
        format: selectedResult.format,
      });
      const downloadTargetPath = path.join(tempRoot, buildSafeFileName(titleForPath, extension));

      if (selectedResult.downloadUrl) {
        const sessionDownload = await downloadViaSessionHttp({
          url: selectedResult.downloadUrl,
          referer: state.currentUrl || (await readCurrentUrl(browserOptions, downloadTimeoutMs).catch(() => "")),
          browserOptions,
          timeoutMs: downloadTimeoutMs,
          targetPath: downloadTargetPath,
        }).catch((error) => {
          debug("session http download failed", {
            message: error instanceof Error ? error.message : String(error),
            url: selectedResult.downloadUrl,
          });
          return null;
        });
        if (sessionDownload) {
          state.downloadedFilePath = sessionDownload;
          return;
        }

        if (selectedResult.downloadUrl) {
          const playwrightDownload = await downloadViaPlaywrightCdp({
            browserOptions,
            pageUrl: state.currentUrl || (await readCurrentUrl(browserOptions, downloadTimeoutMs).catch(() => "")),
            downloadUrl: selectedResult.downloadUrl,
            targetPath: downloadTargetPath,
            timeoutMs: downloadTimeoutMs,
          }).catch((error) => {
            debug("playwright cdp download failed", {
              message: error instanceof Error ? error.message : String(error),
              url: selectedResult.downloadUrl,
              targetPath: downloadTargetPath,
            });
            return null;
          });
          if (playwrightDownload) {
            state.downloadedFilePath = playwrightDownload;
            return;
          }
        }
      }

      state.downloadedFilePath = await resolveDownloadedFile({
        targetPath: downloadTargetPath,
        reportedPath: null,
        downloadDir: browserOptions.downloadDir,
        beforeFiles: startedFiles,
        timeoutMs: downloadTimeoutMs,
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

      try {
        const selector =
          state.fallback.downloadSelector ??
          step.selector ??
          state.detail.downloadSelector;
        reportedDownloadPath = await runDownloadCommand({
          selector,
          browserOptions,
          timeoutMs: downloadTimeoutMs,
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
          downloadTimeoutMs,
          downloadTargetPath,
        });
        applyFallback(state, fallback);

        if (Array.isArray(fallback?.commands) && fallback.commands.length > 0) {
          await runFallbackCommands({
            commands: fallback.commands,
            browserOptions,
            downloadTimeoutMs,
            context: buildTemplateContext(params, state, { downloadTargetPath }),
          });
        }

        if (fallback?.downloadSelector) {
          reportedDownloadPath = await runDownloadCommand({
            selector: fallback.downloadSelector,
            browserOptions,
            timeoutMs: downloadTimeoutMs,
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
        timeoutMs: downloadTimeoutMs,
      });
      return;
    }
    default: {
      throw new Error(`Unsupported playbook step type: ${step.type}`);
    }
  }
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

async function extractDirectDownloadTarget(browserOptions, timeoutMs) {
  const payload = await runAgentBrowser(
    ["eval", buildExtractDirectDownloadScript()],
    browserOptions,
    timeoutMs,
  );
  return JSON.parse(payload);
}

async function readUserAgent(browserOptions, timeoutMs) {
  const payload = await runAgentBrowser(["eval", "navigator.userAgent"], browserOptions, timeoutMs);
  return typeof payload === "string" ? payload : String(payload ?? "");
}

async function readCookies(browserOptions, timeoutMs) {
  const data = await runAgentBrowser(["cookies", "get"], browserOptions, timeoutMs);
  return Array.isArray(data?.cookies) ? data.cookies : [];
}

async function readCdpUrl(browserOptions, timeoutMs) {
  const data = await runAgentBrowser(["get", "cdp-url"], browserOptions, timeoutMs);
  return typeof data?.cdpUrl === "string"
    ? data.cdpUrl
    : typeof data?.url === "string"
    ? data.url
    : typeof data === "string"
    ? data
    : "";
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

function buildExtractDirectDownloadScript() {
  return `(() => {
    const anchors = Array.from(document.querySelectorAll("a"));
    const candidate = anchors.find((node) => {
      const text = (node.textContent || "").trim();
      const href = node.href || "";
      if (!/\\b(EPUB|PDF|MOBI|AZW3?|TXT|RTF|DJVU|FB2|CBZ)\\b/i.test(text)) return false;
      if (!href || /^javascript:/i.test(href)) return false;
      return true;
    });
    return JSON.stringify({
      text: candidate ? (candidate.textContent || "").trim() : "",
      downloadUrl: candidate?.href || "",
      format: candidate ? (candidate.textContent || "").trim() : "",
    });
  })()`;
}

async function downloadViaSessionHttp({ url, referer, browserOptions, timeoutMs, targetPath }) {
  const cookies = await readCookies(browserOptions, timeoutMs);
  const userAgent = await readUserAgent(browserOptions, timeoutMs).catch(() => "");
  const response = await fetchWithSession({
    url,
    referer,
    userAgent,
    cookies,
    timeoutMs,
  });

  if (!response.ok || !response.body) {
    throw new Error(`HTTP download failed with status ${response.status}.`);
  }

  await pipeline(response.body, createWriteStream(targetPath));
  return targetPath;
}

async function downloadViaPlaywrightCdp({ browserOptions, pageUrl, downloadUrl, targetPath, timeoutMs }) {
  const cdpUrl = await readCdpUrl(browserOptions, timeoutMs);
  if (!cdpUrl) {
    throw new Error("Missing CDP URL for current browser session.");
  }

  const browser = await chromium.connectOverCDP(cdpUrl, { timeout: timeoutMs });
  try {
    const page = findBestPage(browser, pageUrl);
    if (!page) {
      throw new Error("No active page found in browser CDP session.");
    }

    const target = new URL(downloadUrl);
    const hrefSelector = `a[href$="${target.pathname}"]`;
    const locator = page.locator(hrefSelector).first();
    const download = await Promise.all([
      page.waitForEvent("download", { timeout: timeoutMs }),
      locator.count().then((count) => {
        if (count > 0) {
          return locator.click();
        }
        return page.evaluate((url) => {
          window.location.assign(url);
        }, downloadUrl);
      }),
    ]).then(([result]) => result);

    await download.saveAs(targetPath);
    return targetPath;
  } finally {
    await browser.close().catch(() => {});
  }
}

async function fetchWithSession({ url, referer, userAgent, cookies, timeoutMs, redirectCount = 0 }) {
  if (redirectCount > 5) {
    throw new Error("Too many redirects while downloading.");
  }

  const headers = {
    Accept: "*/*",
  };
  if (referer) headers.Referer = referer;
  if (userAgent) headers["User-Agent"] = userAgent;

  const cookieHeader = buildCookieHeader(cookies, url);
  if (cookieHeader) headers.Cookie = cookieHeader;

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      method: "GET",
      headers,
      redirect: "manual",
      signal: controller.signal,
    });

    if ([301, 302, 303, 307, 308].includes(response.status)) {
      const location = response.headers.get("location");
      if (!location) {
        throw new Error(`Redirect response missing location header (${response.status}).`);
      }
      const nextUrl = new URL(location, url).toString();
      return fetchWithSession({
        url: nextUrl,
        referer: url,
        userAgent,
        cookies,
        timeoutMs,
        redirectCount: redirectCount + 1,
      });
    }

    return response;
  } finally {
    clearTimeout(timer);
  }
}

function buildCookieHeader(cookies, url) {
  const target = new URL(url);
  const pairs = cookies
    .filter((cookie) => shouldSendCookie(cookie, target))
    .map((cookie) => `${cookie.name}=${cookie.value}`);
  return pairs.join("; ");
}

function findBestPage(browser, pageUrl) {
  const pages = browser.contexts().flatMap((context) => context.pages());
  if (!pages.length) {
    return null;
  }
  if (pageUrl) {
    const exact = pages.find((page) => page.url() === pageUrl);
    if (exact) {
      return exact;
    }
  }
  return pages[0];
}

function shouldSendCookie(cookie, target) {
  if (!cookie || !cookie.name) {
    return false;
  }

  if (cookie.secure && target.protocol !== "https:") {
    return false;
  }

  const host = target.hostname.toLowerCase();
  const domain = String(cookie.domain ?? "").replace(/^\./, "").toLowerCase();
  if (domain && host !== domain && !host.endsWith(`.${domain}`)) {
    return false;
  }

  const cookiePath = cookie.path || "/";
  if (!target.pathname.startsWith(cookiePath)) {
    return false;
  }

  if (typeof cookie.expires === "number" && cookie.expires > 0 && cookie.expires * 1000 < Date.now()) {
    return false;
  }

  return true;
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

function parseDetailDownloadRef(snapshot) {
  const lines = String(snapshot ?? "").split("\n");
  for (const line of lines) {
    const match = line.match(/^\s*- link "(.+?)" \[ref=(e\d+)\]$/);
    if (!match) {
      continue;
    }
    const [, text, ref] = match;
    if (/read online/i.test(text)) {
      continue;
    }
    if (/\b(EPUB|PDF|MOBI|AZW3?|TXT|RTF|DJVU|FB2|CBZ)\b/i.test(text)) {
      return {
        selector: `@${ref}`,
        format: text.trim(),
      };
    }
  }
  return null;
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

async function resolveDownloadedFile({ targetPath, reportedPath, downloadDir, beforeFiles, timeoutMs }) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (reportedPath && (await exists(reportedPath))) {
      return reportedPath;
    }
    if (await exists(targetPath)) {
      return targetPath;
    }
    const files = await listFiles(downloadDir);
    for (const file of files) {
      if (!beforeFiles.has(file)) {
        return file;
      }
    }
    await sleep(750);
  }

  throw new Error(`Timed out waiting for browser download after ${timeoutMs}ms.`);
}

async function waitForNewDownloadFile({ downloadDir, beforeFiles, timeoutMs }) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const files = await listFiles(downloadDir);
    for (const file of files) {
      if (!beforeFiles.has(file)) {
        return file;
      }
    }
    await sleep(250);
  }

  return null;
}

function pickResult(results, params) {
  if (!Array.isArray(results) || results.length === 0) {
    return null;
  }

  if (params.resultIndex && results[params.resultIndex - 1]) {
    return results[params.resultIndex - 1];
  }

  const best = results
    .map((result) => ({ result, score: scoreResult(result, params) }))
    .sort((left, right) => right.score - left.score)[0];

  debug("best result candidate", best ?? null);
  if (!best || best.score < MIN_RESULT_SCORE) {
    return null;
  }

  return best.result;
}

function scoreResult(result, params) {
  const title = normalizeText(result.title);
  const author = normalizeText(result.author);
  const query = normalizeText(params.query);
  const titleHint = normalizeText(params.titleHint);
  const authorHint = normalizeText(params.authorHint);

  let score = 0;
  if (titleHint && title === titleHint) score += 140;
  if (title.includes(query)) score += 120;
  if (authorHint && author.includes(authorHint)) score += 40;

  const haystack = `${title} ${author}`;
  const tokens = tokenize(params.titleHint || params.query);
  score += tokens.filter((token) => haystack.includes(token)).length * 16;
  return score;
}

function guessExtension(detail) {
  const downloadUrl = detail.downloadUrl || "";
  try {
    const extension = path.extname(new URL(downloadUrl).pathname).toLowerCase();
    if (DOWNLOAD_EXTENSIONS.includes(extension)) {
      return extension;
    }
  } catch {}

  const format = String(detail.format ?? "").toLowerCase();
  for (const extension of DOWNLOAD_EXTENSIONS) {
    if (format.includes(extension.slice(1))) {
      return extension;
    }
  }
  return ".bin";
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
