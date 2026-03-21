# OpenClaw Library Fetcher

`library_book_fetch` is an OpenClaw plugin tool that:

- checks a local book cache first
- runs a site-specific Playwright-first provider when the cache misses
- clicks the download link and saves the file into the local library root
- records the download in a local SQLite cache for future reuse
- can fall back to `agent-browser` only for login/session recovery or unsupported providers

The plugin also exposes narrower tools for OpenClaw:

- `library_cache_lookup`: local SQLite cache only
- `library_book_search`: remote search only
- `library_book_download`: remote download only
- `library_job_submit`: submit a full fetch job and get a `jobId`
- `library_job_status`: poll current queue/running/completed state for a `jobId`
- `library_job_result`: retrieve the final file/result for a completed `jobId`

Recommended OpenClaw workflow:

1. `library_cache_lookup`
2. If not found, `library_book_search`
3. Choose one candidate result
4. `library_book_download(selectionToken=...)`

`library_book_search` returns a stable `selectionToken` for each result. `library_book_download` should prefer that token instead of browser-specific refs such as `@e41`.

When `selectionToken` is present, `library_book_download` ignores query/titleHint/authorHint/siteId/playbookPath/resultIndex and uses the token as the only selection input. It still checks the local cache first unless you explicitly pass `forceRefresh: true`.

Tools also accept explicit `title` and `author` fields. If `query` is omitted, `title` becomes the query; if `authorHint` is omitted, `author` is used as the author hint. They also accept optional `language` / `languageHint`; when present, result ranking and cache matching will prefer that language. Without a language hint, the default language preference is: Chinese, then English, then Traditional Chinese, then Spanish/French.

Remote library work is rate-limited through a shared FIFO queue. `library_book_search` and remote cache misses in `library_book_fetch` / `library_book_download` return a `queueStatus` object with the initial queue position and total wait time.

For chatbots that need user-visible queue progress, prefer the job tools:

1. `library_job_submit`
2. `library_job_status`
3. `library_job_result`

## Files

- `index.mjs`: OpenClaw plugin entry.
- `src/book-fetcher.mjs`: cache lookup, playbook orchestration, and library storage.
- `src/cache-store.mjs`: SQLite-backed cache store and cache consistency audit.
- `src/agent-browser-playbook.mjs`: compatibility executor and login/session recovery helpers for non-Playwright providers.
- `src/playbooks.mjs`: plugin config parsing and playbook loading.
- `playbooks/example-library.json`: sample playbook for a supported site.
- `bin/claw-library-fetch.mjs`: standalone CLI wrapper for local testing.

## Install

1. Install dependencies:

```bash
npm install
```

2. Point OpenClaw at this local plugin folder or publish/install it as a package.

3. Configure plugin defaults in `plugins.entries.library-fetcher.config`, for example:

```json
{
  "plugins": {
    "entries": {
      "library-fetcher": {
        "enabled": true,
        "config": {
          "playbooksDir": "/absolute/path/to/playbooks",
          "defaultSiteId": "zlib",
          "defaultLibraryRoot": "~/.openclaw/plugins/library-fetcher/library",
          "browserProfilePath": "~/.openclaw/plugins/library-fetcher/profile",
          "agentBrowserSessionConfigPath": "~/.openclaw/plugins/library-fetcher/agent-browser-session.json",
          "llmFallbackCommand": "/absolute/path/to/repair-agent"
        }
      }
    }
  }
}
```

4. If your global tool policy is restrictive, run `/bookfetch enable` once or add `library-fetcher` to `tools.allow` or `tools.alsoAllow`. That enables the whole plugin, including `library_cache_lookup`, `library_book_search`, `library_book_download`, and the compatibility wrapper `library_book_fetch`.

## Tool input

```json
{
  "query": "Clean Code",
  "titleHint": "Clean Code",
  "authorHint": "Robert C. Martin",
  "siteId": "example-library",
  "playbookPath": "/absolute/path/to/playbook.json",
  "libraryRoot": "/absolute/path/to/local-library",
  "forceRefresh": false,
  "cacheOnly": false,
  "resultIndex": 1
}
```

In normal use, `query` is the only field users need to provide. `titleHint` is optional and defaults to `query`; it only matters when you want to force a more exact title match than the raw search phrase.

If the user provides both a title and an author, prefer:

```json
{
  "title": "三体",
  "author": "刘慈欣"
}
```

The tool will derive the internal query/hints automatically.

All tool responses now use a consistent JSON shape with:

- `ok`
- `found`
- `reason`
- `elapsedSeconds`

Success responses then add tool-specific fields such as `results`, `selectionToken`, `filePath`, `sourceUrl`, and `downloadUrl`.

Failure responses also stay JSON. They return:

- `ok: false`
- `found: false`
- `reason`
- `errorMessage`

Current `reason` values include:

- `cache_hit`
- `cache_miss`
- `search_results_found`
- `no_matching_search_result`
- `downloaded`
- `login_session_started`
- `profile_missing`
- `profile_uninitialized`
- `profile_missing_cookies`
- `missing_query`
- `missing_selection_input`
- `invalid_selection_token`
- `cache_only_miss`
- `tool_error`

## Playbook notes

Each supported site gets one JSON playbook. The playbook stays deterministic and only covers the path you actually use on that site.

- Supported step types are `open`, `wait`, `fill`, `click`, `press`, `command`, `extract-results`, `open-result`, `download-result`, `extract-detail`, and `download`.
- `command` lets you encode site-specific recovery operations without changing core code.
- `extract-results` and `extract-detail` still use small selector sets, but only inside a site-local playbook.
- `download-result` is for sites where the search result row already contains a direct download link.
- If the site needs login, use `browserProfilePath` or `browser.profilePath` so the session can reuse saved cookies.
- You can also point the plugin at a local `agent-browser-session.json` file. When present, its `sessionName` and `profilePath` override the playbook defaults.
- URL templates can use `{{queryEncoded}}`, `{{titleHintEncoded}}`, and `{{authorHintEncoded}}` to avoid broken query strings.
- Result extractors also support adjacent-link helpers like `__prevLink__`, `__nextLink__`, and `__nextLink(2)__` when a result row has no stable wrapper selector.
- If a step fails, the plugin can send page title, URL, interactive snapshot, current results, and step metadata to an external LLM repair command.

## Standalone CLI

```bash
node ./bin/claw-library-fetch.mjs '{"query":"Clean Code","siteId":"example-library","playbookPath":"/abs/playbook.json","libraryRoot":"/abs/books"}'
```

To print execution progress and the current browser step:

```bash
node ./bin/claw-library-fetch.mjs --debug '{"query":"Clean Code","playbookPath":"/abs/playbook.json","libraryRoot":"/abs/books"}'
```

You can also set `DEBUG_BOOK_FETCH=1`.

Useful debug-only params:

```json
{
  "timeoutMs": 15000,
  "keepSessionOnError": true
}
```

`timeoutMs` overrides the default 120s timeout. `keepSessionOnError` leaves the browser session open after a failure so you can inspect the current page manually.

## Cache audit

The local cache now lives in `.openclaw-book-cache.sqlite`.

To audit cache entries against real files under the configured library root:

```bash
node ./bin/claw-library-fetch.mjs --doctor '{"libraryRoot":"/abs/books"}'
```

To remove invalid rows whose files are missing or whose stored paths point outside the library root:

```bash
node ./bin/claw-library-fetch.mjs --doctor --repair '{"libraryRoot":"/abs/books"}'
```

OpenClaw command equivalents:

```text
/bookfetch status
/bookfetch login
/bookfetch doctor
/bookfetch doctor --repair
/bookfetch reset --confirm
```

Structured command output is also available:

```text
/bookfetch status --json
/bookfetch login --json
/bookfetch doctor --json
/bookfetch reset --confirm --json
```

`/bookfetch login` is meant for OpenClaw recovery flows. It automatically:

- closes any stale `agent-browser` session
- writes the persistent `agent-browser-session.json` file
- launches a headed browser window using the configured `profilePath`

After the user finishes logging in, close the browser session. Later search/download runs can reuse the saved profile in headless mode.

To fully clear the local library root and all cache files for debugging:

```bash
node ./bin/claw-library-fetch.mjs --reset --confirm '{"libraryRoot":"/abs/books"}'
```

## Persistent browser session

If the configured browser profile is missing or invalid, the tool now returns a recovery message with:

- a one-line `agent-browser` command to initialize a fresh persistent session
- the JSON snippet to store in `agent-browser-session.json`

Example session config:

```json
{
  "sessionName": "clawbot",
  "profilePath": "~/.openclaw/plugins/library-fetcher/profile"
}
```

## LLM repair command

The optional repair command receives a JSON payload on stdin and should return JSON on stdout. Useful fields in the response are:

```json
{
  "selectedResultIndex": 1,
  "selectedResultUrl": "https://example.com/book/123",
  "downloadSelector": "text=Download PDF",
  "metadata": {
    "title": "Clean Code",
    "author": "Robert C. Martin",
    "format": "PDF"
  },
  "commands": [
    { "command": "click", "args": ["text=Download"] }
  ]
}
```

This keeps the normal path deterministic while still giving you an escape hatch for anti-bot changes or small page redesigns.

## Current assumptions

- You only need to support a small number of library sites.
- Playwright is the preferred runtime path for bundled providers.
- LLM fallback is only for repairing failed or ambiguous steps, not for the normal path.
