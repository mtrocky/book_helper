# OpenClaw Library Fetcher

`library_book_fetch` is an OpenClaw plugin tool that:

- checks a local book cache first
- runs a site-specific `agent-browser` playbook when the cache misses
- clicks the download link and saves the file into the local library root
- records the download in a local SQLite cache for future reuse
- can call an external LLM repair command when a playbook step fails

## Files

- `index.mjs`: OpenClaw plugin entry.
- `src/book-fetcher.mjs`: cache lookup, playbook orchestration, and library storage.
- `src/cache-store.mjs`: SQLite-backed cache store and cache consistency audit.
- `src/agent-browser-playbook.mjs`: `agent-browser` step executor and LLM repair hook.
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
          "defaultLibraryRoot": "/absolute/path/to/local-library",
          "browserProfilePath": "/absolute/path/to/browser-profile",
          "agentBrowserSessionConfigPath": "/absolute/path/to/agent-browser-session.json",
          "llmFallbackCommand": "/absolute/path/to/repair-agent"
        }
      }
    }
  }
}
```

4. If your global tool policy is restrictive, run `/bookfetch enable` once or add `library_book_fetch` to `tools.allow` or `tools.alsoAllow`.

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

## Playbook notes

Each supported site gets one JSON playbook. The playbook stays deterministic and only covers the path you actually use on that site.

- Supported step types are `open`, `wait`, `fill`, `click`, `press`, `command`, `extract-results`, `open-result`, `download-result`, `extract-detail`, and `download`.
- `command` lets you encode site-specific `agent-browser` operations without changing core code.
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
/bookfetch doctor
/bookfetch doctor --repair
/bookfetch reset --confirm
```

To fully clear the local library root and all cache files for debugging:

```bash
node ./bin/claw-library-fetch.mjs --reset --confirm '{"libraryRoot":"/abs/books"}'
```

## Persistent browser session

If the configured `agent-browser` profile is missing or invalid, the tool now returns a recovery message with:

- a one-line `agent-browser` command to initialize a fresh persistent session
- the JSON snippet to store in `agent-browser-session.json`

Example session config:

```json
{
  "sessionName": "clawbot",
  "profilePath": "/absolute/path/to/Runtime/profile"
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
- `agent-browser` is the main runtime path.
- LLM fallback is only for repairing failed or ambiguous steps, not for the normal path.
