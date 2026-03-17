import path from "node:path";
import { stat } from "node:fs/promises";
import { chromium } from "playwright";
import { info } from "./debug.mjs";
import {
  buildSafeFileName,
  estimateDownloadTimeoutMs,
  guessExtension,
  parseSizeHintToBytes,
  pickResult,
} from "./playbook-runtime-utils.mjs";

export function supportsPlaywrightZLibrary(playbook) {
  return playbook?.id === "c-library";
}

export async function searchPlaywrightZLibraryPlaybook({
  params,
  playbook,
  browserOptions,
  downloadTimeoutMs,
  signal,
  onBrowserError,
}) {
  return withPlaywrightPersistentContext(
    browserOptions,
    { acceptDownloads: false, timeoutMs: downloadTimeoutMs, signal, onBrowserError },
    async ({ page }) => {
      const result = await runPlaywrightZLibrarySearch({
        page,
        params,
        playbook,
        timeoutMs: downloadTimeoutMs,
      });
      return {
        ...result,
        backend: "playwright-search",
        llmFallbackUsed: false,
      };
    },
  );
}

export async function executePlaywrightZLibraryPlaybook({
  params,
  playbook,
  browserOptions,
  tempRoot,
  downloadTimeoutMs,
  downloadTimingModel,
  signal,
  onBrowserError,
}) {
  return withPlaywrightPersistentContext(
    browserOptions,
    { acceptDownloads: true, timeoutMs: downloadTimeoutMs, signal, onBrowserError },
    async ({ page }) => {
      const searchState = await runPlaywrightZLibrarySearch({
        page,
        params,
        playbook,
        timeoutMs: downloadTimeoutMs,
      });

      const selectedResult = pickResult(searchState.results, params);
      if (!selectedResult) {
        throw new Error(`No matching search result found for "${params.query}".`);
      }
      if (!selectedResult.downloadUrl) {
        throw new Error(`No direct download target found for "${params.query}".`);
      }

      info("download candidate selected", {
        title: selectedResult.title ?? params.titleHint ?? params.query,
        author: selectedResult.author ?? "",
        publisher: selectedResult.publisher ?? "",
        format: selectedResult.format ?? "",
      });

      const timing = await selectAdaptiveDownloadTiming({
        baseTimeoutMs: downloadTimeoutMs,
        playbookId: playbook.id,
        backend: "playwright",
        format: selectedResult.format,
        downloadTimingModel,
      });
      const effectiveDownloadTimeoutMs = timing.effectiveTimeoutMs;
      info("download timeout selected", {
        title: selectedResult.title ?? params.titleHint ?? params.query,
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
      const downloadTargetPath = path.join(
        tempRoot,
        buildSafeFileName(selectedResult.title ?? params.titleHint ?? params.query, extension),
      );

      const downloadStartedAt = Date.now();
      const download = await Promise.all([
        page.waitForEvent("download", { timeout: effectiveDownloadTimeoutMs }),
        page
          .goto(selectedResult.downloadUrl, {
            waitUntil: "domcontentloaded",
            timeout: effectiveDownloadTimeoutMs,
          })
          .catch(() => null),
      ])
        .then(([result]) => result)
        .catch((error) => {
          info("playwright browser download failed", {
            message: error instanceof Error ? error.message : String(error),
            url: selectedResult.downloadUrl,
            timeoutMs: effectiveDownloadTimeoutMs,
          });
          throw error;
        });

      await download.saveAs(downloadTargetPath);
      const fileSizeBytes = await download
        .path()
        .then((tmpPath) => (tmpPath ? stat(tmpPath).then((details) => details.size).catch(() => null) : null))
        .catch(() => null);
      info("download completed via playwright browser", {
        path: downloadTargetPath,
      });

      return {
        filePath: downloadTargetPath,
        title: selectedResult.title ?? params.titleHint ?? params.query,
        author: selectedResult.author ?? "",
        language: selectedResult.language ?? "",
        sourceUrl: selectedResult.href ?? searchState.currentUrl ?? "",
        downloadUrl: selectedResult.downloadUrl,
        llmFallbackUsed: false,
        backend: "playwright",
        downloadMetrics: {
          backend: "playwright",
          format: selectedResult.format ?? "",
          fileSizeBytes,
          expectedBytes: timing.expectedBytes,
          predictedSeconds: timing.model?.predictedSeconds ?? null,
          predictedTimeoutMs: effectiveDownloadTimeoutMs,
          durationSeconds: roundNumber((Date.now() - downloadStartedAt) / 1000, 2),
        },
      };
    },
  );
}

async function runPlaywrightZLibrarySearch({ page, params, playbook, timeoutMs }) {
  const openStep = Array.isArray(playbook?.steps)
    ? playbook.steps.find((step) => step?.type === "open" && typeof step?.url === "string")
    : null;
  if (!openStep?.url) {
    throw new Error(`Playbook "${playbook?.id ?? "unknown"}" is missing an open step.`);
  }

  const waitStep = Array.isArray(playbook?.steps)
    ? playbook.steps.find((step) => step?.type === "wait")
    : null;
  const searchUrl = renderTemplate(openStep.url, {
    query: params.query,
    queryEncoded: encodeURIComponent(params.query),
    titleHint: params.titleHint ?? "",
    titleHintEncoded: encodeURIComponent(params.titleHint ?? ""),
    authorHint: params.authorHint ?? "",
    authorHintEncoded: encodeURIComponent(params.authorHint ?? ""),
    languageHint: params.languageHint ?? "",
    languageHintEncoded: encodeURIComponent(params.languageHint ?? ""),
  });

  await page.goto(searchUrl, { waitUntil: "domcontentloaded", timeout: timeoutMs });
  await page.waitForTimeout(Number.isInteger(waitStep?.ms) ? waitStep.ms : 2000);
  await page
    .waitForSelector("div.book-item.resItemBoxBooks z-bookcard", {
      timeout: Math.min(timeoutMs, 15000),
    })
    .catch(() => null);

  const results = await page.evaluate(() => {
    const cards = Array.from(document.querySelectorAll("div.book-item.resItemBoxBooks"));
    return cards
      .map((node) => {
        const card = node.querySelector("z-bookcard");
        if (!card) return null;
        const title = card.querySelector('[slot="title"]')?.textContent?.trim() ?? "";
        const author = card.querySelector('[slot="author"]')?.textContent?.trim() ?? "";
        const rawHref = card.getAttribute("href") || "";
        const rawDownload = card.getAttribute("download") || "";
        const href = rawHref ? new URL(rawHref, window.location.href).toString() : "";
        const downloadUrl = rawDownload ? new URL(rawDownload, window.location.href).toString() : "";
        const publisher = card.getAttribute("publisher") || "";
        const language = (card.getAttribute("language") || "").trim();
        const extension = (card.getAttribute("extension") || "").trim();
        const filesize = (card.getAttribute("filesize") || "").trim();
        const format = [extension ? extension.toUpperCase() : "", filesize]
          .filter(Boolean)
          .join(", ");
        return {
          title,
          author,
          href,
          downloadUrl,
          publisher,
          language,
          format,
        };
      })
      .filter((entry) => entry && entry.title && (entry.href || entry.downloadUrl))
      .slice(0, 50);
  });

  return {
    results,
    currentUrl: page.url(),
    currentTitle: await page.title(),
  };
}

async function withPlaywrightPersistentContext(browserOptions, runtimeOptions, fn) {
  const launchOptions = {
    headless: !browserOptions.headed,
    acceptDownloads: runtimeOptions.acceptDownloads,
    downloadsPath: browserOptions.downloadDir,
    timeout: runtimeOptions.timeoutMs,
    ignoreHTTPSErrors: browserOptions.ignoreHttpsErrors,
  };
  if (browserOptions.proxy) {
    launchOptions.proxy = { server: browserOptions.proxy };
  }

  let context;
  try {
    context = await chromium.launchPersistentContext(browserOptions.profilePath, launchOptions);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (typeof runtimeOptions.onBrowserError === "function") {
      throw runtimeOptions.onBrowserError(message);
    }
    throw error;
  }

  try {
    const page = context.pages()[0] ?? (await context.newPage());
    return await fn({ context, page });
  } finally {
    await context.close().catch(() => {});
  }
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

function roundNumber(value, digits) {
  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
}

function renderTemplate(value, context) {
  return String(value ?? "").replace(/\{\{\s*([a-zA-Z0-9_]+)\s*\}\}/g, (_, key) =>
    key in context ? String(context[key] ?? "") : "",
  );
}
