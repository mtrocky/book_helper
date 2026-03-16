#!/usr/bin/env node

import { auditLibraryCache, fetchBookToLibrary, resetLibraryCache } from "../src/book-fetcher.mjs";

async function main() {
  const rawArgs = process.argv.slice(2);
  if (rawArgs.includes("--debug")) {
    process.env.DEBUG_BOOK_FETCH = "1";
  }
  const doctorMode = rawArgs.includes("--doctor");
  const repairMode = rawArgs.includes("--repair");
  const resetMode = rawArgs.includes("--reset");
  const confirmMode = rawArgs.includes("--confirm");
  const args = rawArgs.filter((arg) => !["--debug", "--doctor", "--repair", "--reset", "--confirm"].includes(arg));

  const raw = args[0];
  if (!raw) {
    process.stderr.write(
      'Usage: claw-library-fetch [--debug] [--doctor] [--repair] \'{"query":"book name","siteId":"site-id","playbookPath":"/abs/playbook.json","libraryRoot":"/abs/library"}\'\n',
    );
    process.exit(1);
  }

  const params = JSON.parse(raw);
  if (params && typeof params === "object" && params.debug) {
    process.env.DEBUG_BOOK_FETCH = "1";
  }
  if (process.env.DEBUG_BOOK_FETCH === "1" || process.env.DEBUG_BOOK_FETCH === "true") {
    process.stderr.write(`[book-fetch] cli params: ${JSON.stringify({
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
      doctorMode,
      repairMode,
      resetMode,
      confirmMode,
    })}\n`);
  }
  const result = doctorMode
    ? await auditLibraryCache(
        {
          libraryRoot: params.libraryRoot,
          repair: repairMode || Boolean(params.repair),
        },
        {},
      )
    : resetMode
    ? await resetLibraryCache(
        {
          libraryRoot: params.libraryRoot,
          confirm: confirmMode || Boolean(params.confirm),
        },
        {},
      )
    : await fetchBookToLibrary(params, {}, undefined);
  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
}

main().catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
  process.exit(1);
});
