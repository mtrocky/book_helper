import {
  auditLibraryCache,
  DEFAULT_TOOL_NAME,
  downloadBookToLibrary,
  lookupCachedBook,
  resetLibraryCache,
  searchBooks,
  TOOL_GROUP,
  describeStatus,
  fetchBookToLibrary,
  parsePluginConfig,
  withToolEnabled,
} from "./src/book-fetcher.mjs";

const PLUGIN_ID = "library-fetcher";
const CACHE_LOOKUP_TOOL_NAME = "library_cache_lookup";
const SEARCH_TOOL_NAME = "library_book_search";
const DOWNLOAD_TOOL_NAME = "library_book_download";

function cloneObject(value) {
  return value && typeof value === "object" && !Array.isArray(value)
    ? { ...value }
    : {};
}

function cloneStringList(value) {
  return Array.isArray(value) ? value.filter((entry) => typeof entry === "string") : [];
}

function resolveToolPolicySource(config) {
  const tools = cloneObject(config?.tools);
  for (const [path, rawList] of [
    ["tools.allow", tools.allow],
    ["tools.alsoAllow", tools.alsoAllow],
  ]) {
    const list = cloneStringList(rawList);
    if (list.includes(DEFAULT_TOOL_NAME)) return `${path} (${DEFAULT_TOOL_NAME})`;
    if (list.includes(PLUGIN_ID)) return `${path} (${PLUGIN_ID})`;
    if (list.includes(TOOL_GROUP)) return `${path} (${TOOL_GROUP})`;
  }
  return null;
}

async function handleBookFetchCommand(api, args) {
  const configApi = api.runtime?.config;
  const config = configApi?.loadConfig?.() ?? {};
  const parts = args.split(/\s+/).filter(Boolean);
  const action = parts[0]?.toLowerCase() ?? "status";

  if (action === "status" || action === "help") {
    const statusText = await describeStatus(api.pluginConfig, resolveToolPolicySource(config));
    return { text: statusText };
  }

  if (action === "doctor") {
    const repair = parts.includes("--repair");
    const audit = await auditLibraryCache({ repair }, api.pluginConfig);
    const lines = [
      "Library cache audit:",
      `Library Root: ${audit.libraryRoot}`,
      `Cache DB: ${audit.cacheDbPath}`,
      `Entries Checked: ${audit.checkedEntries}`,
      `Valid Entries: ${audit.validEntries}`,
      `Invalid Entries: ${audit.invalidEntries}`,
      `Removed Entries: ${audit.removedEntries}`,
    ];
    if (audit.migratedLegacyJson) {
      lines.push(`Legacy JSON Imported: ${audit.importedLegacyEntries}`);
    }
    if (audit.issues.length > 0) {
      lines.push("", "Sample issues:");
      for (const issue of audit.issues) {
        const label = [issue.title, issue.author].filter(Boolean).join(" / ") || issue.id;
        lines.push(`- ${issue.reason}: ${label} -> ${issue.filePath}`);
      }
      if (audit.invalidEntries > audit.issues.length) {
        lines.push(`- ...and ${audit.invalidEntries - audit.issues.length} more`);
      }
    }
    return { text: lines.join("\n") };
  }

  if (action === "reset") {
    const confirm = parts.includes("--confirm");
    if (!confirm) {
      return {
        text: [
          "Library reset requires confirmation.",
          "",
          "Run:",
          "/bookfetch reset --confirm",
        ].join("\n"),
      };
    }
    const result = await resetLibraryCache({ confirm: true }, api.pluginConfig);
    return {
      text: [
        "Library cache reset complete.",
        `Library Root: ${result.libraryRoot}`,
        `Removed Paths: ${result.removedCount}`,
      ].join("\n"),
    };
  }

  if (action !== "enable") {
    return {
      text: [
        "Book fetcher commands:",
        "",
        "/bookfetch status",
        "/bookfetch doctor",
        "/bookfetch doctor --repair",
        "/bookfetch reset --confirm",
        "/bookfetch enable",
      ].join("\n"),
    };
  }

  if (!configApi?.loadConfig || !configApi?.writeConfigFile) {
    return {
      text: `This runtime cannot edit tool policy automatically. Add ${DEFAULT_TOOL_NAME}, ${PLUGIN_ID}, or ${TOOL_GROUP} to tools.allow/tools.alsoAllow manually.`,
    };
  }

  const currentConfig = configApi.loadConfig();
  const { nextConfig, changed, policyPath } = withToolEnabled(currentConfig);
  if (changed) {
    await configApi.writeConfigFile(nextConfig);
  }

  const statusText = await describeStatus(api.pluginConfig, policyPath);
  return {
    text: [
      changed
        ? `Enabled ${DEFAULT_TOOL_NAME} in ${policyPath}.`
        : `${DEFAULT_TOOL_NAME} is already enabled via ${policyPath}.`,
      "Start a new session or restart the gateway if the agent still does not see the tool.",
      "",
      statusText,
    ].join("\n"),
  };
}

const libraryFetcherPlugin = {
  id: PLUGIN_ID,
  name: "Library Fetcher",
  description: "Search a configured library site and download books into a local cache.",
  configSchema: {
    parse: parsePluginConfig,
  },
  register(api) {
    api.registerCommand({
      name: "bookfetch",
      description: "Show book fetcher status or enable the tool policy.",
      acceptsArgs: true,
      handler: async (ctx) => {
        const args = ctx.args?.trim() ?? "";
        return handleBookFetchCommand(api, args);
      },
    });

    api.registerTool({
      name: CACHE_LOOKUP_TOOL_NAME,
      label: "Library Cache Lookup",
      description: "Look up a book in the local SQLite cache only. Does not access the remote site.",
      parameters: {
        type: "object",
        properties: {
          query: {
            type: "string",
            description: "Keyword query used to search the local cache.",
          },
          titleHint: {
            type: "string",
            description: "Optional exact title override. When omitted, the tool uses query as the title hint.",
          },
          authorHint: {
            type: "string",
            description: "Optional author hint for better cache matching.",
          },
          libraryRoot: {
            type: "string",
            description: "Absolute path to the local book cache directory. Falls back to plugin config when omitted.",
          },
        },
        required: ["query"],
      },
      execute: async (_toolCallId, params) => {
        const result = await lookupCachedBook(params, api.pluginConfig);
        return {
          content: [
            {
              type: "text",
              text: JSON.stringify(result, null, 2),
            },
          ],
        };
      },
    });

    api.registerTool({
      name: SEARCH_TOOL_NAME,
      label: "Library Book Search",
      description: "Search the remote library site and return candidate results without downloading. Results include stable selectionToken values instead of transient browser refs.",
      parameters: {
        type: "object",
        properties: {
          query: {
            type: "string",
            description: "Keyword query used to search the remote library site.",
          },
          titleHint: {
            type: "string",
            description: "Optional exact title override. When omitted, the tool uses query as the title hint.",
          },
          authorHint: {
            type: "string",
            description: "Optional author hint for better result ranking.",
          },
          siteId: {
            type: "string",
            description: "Site playbook id resolved against playbooksDir. Falls back to plugin config when omitted.",
          },
          playbookPath: {
            type: "string",
            description: "Absolute path to a site playbook JSON. Overrides siteId when provided.",
          },
          timeoutMs: {
            type: "integer",
            minimum: 1000,
            description: "Optional search timeout override in milliseconds.",
          },
        },
        required: ["query"],
      },
      execute: async (_toolCallId, params, signal) => {
        const result = await searchBooks(params, api.pluginConfig, signal);
        return {
          content: [
            {
              type: "text",
              text: JSON.stringify(result, null, 2),
            },
          ],
        };
      },
    });

    api.registerTool({
      name: DOWNLOAD_TOOL_NAME,
      label: "Library Book Download",
      description:
        "Download a book from the remote library site and write it into the local cache. By default it reuses a cache hit first; pass selectionToken from library_book_search whenever possible, and use forceRefresh only when you explicitly want a fresh remote run.",
      parameters: {
        type: "object",
        properties: {
          selectionToken: {
            type: "string",
            description: "Stable token returned by library_book_search. When provided, it becomes the only selection input and query/titleHint/authorHint/siteId/playbookPath/resultIndex are ignored.",
          },
          query: {
            type: "string",
            description: "Fallback search phrase used only when selectionToken is omitted.",
          },
          titleHint: {
            type: "string",
            description: "Fallback exact title override used only when selectionToken is omitted.",
          },
          authorHint: {
            type: "string",
            description: "Fallback author hint used only when selectionToken is omitted.",
          },
          siteId: {
            type: "string",
            description: "Fallback site playbook id used only when selectionToken is omitted.",
          },
          playbookPath: {
            type: "string",
            description: "Fallback playbook path used only when selectionToken is omitted.",
          },
          libraryRoot: {
            type: "string",
            description: "Absolute path to the local book cache directory. Falls back to plugin config when omitted.",
          },
          resultIndex: {
            type: "integer",
            minimum: 1,
            description: "Fallback 1-based result index used only when selectionToken is omitted.",
          },
          timeoutMs: {
            type: "integer",
            minimum: 1000,
            description: "Optional timeout override in milliseconds.",
          },
          forceRefresh: {
            type: "boolean",
            description: "Skip cache reuse and force a fresh remote download.",
          },
        },
        required: [],
      },
      execute: async (_toolCallId, params, signal) => {
        const result = await downloadBookToLibrary(params, api.pluginConfig, signal);
        return {
          content: [
            {
              type: "text",
              text: JSON.stringify(result, null, 2),
            },
          ],
        };
      },
    });

    api.registerTool({
      name: DEFAULT_TOOL_NAME,
      label: "Library Book Fetch",
      description:
        "Compatibility wrapper that checks the local cache first, then searches and downloads from the remote site when needed.",
      parameters: {
        type: "object",
        properties: {
          query: {
            type: "string",
            description: "The main user-provided book name or search phrase.",
          },
          titleHint: {
            type: "string",
            description: "Optional exact title override. When omitted, the tool uses query as the title hint.",
          },
          authorHint: {
            type: "string",
            description: "Optional author hint for better result ranking.",
          },
          siteId: {
            type: "string",
            description: "Site playbook id resolved against playbooksDir. Falls back to plugin config when omitted.",
          },
          playbookPath: {
            type: "string",
            description: "Absolute path to a site playbook JSON. Overrides siteId when provided.",
          },
          libraryRoot: {
            type: "string",
            description: "Absolute path to the local book cache directory. Falls back to plugin config when omitted.",
          },
          forceRefresh: {
            type: "boolean",
            description: "Skip the local cache and force a new search/download from the site.",
          },
          cacheOnly: {
            type: "boolean",
            description: "Only search the local cache; do not access the website when the file is missing.",
          },
          resultIndex: {
            type: "integer",
            minimum: 1,
            description: "Optional 1-based result index override when multiple search results are similar.",
          }
        },
        required: ["query"],
      },
      execute: async (_toolCallId, params, signal) => {
        const result = await fetchBookToLibrary(params, api.pluginConfig, signal);
        const lines = [
          `${result.fromCache ? "Cache hit" : "Downloaded"}: ${result.title}`,
          `Path: ${result.filePath}`,
          `Backend: ${result.backend}`,
        ];
        if (result.author) lines.push(`Author: ${result.author}`);
        if (result.playbookId) lines.push(`Playbook: ${result.playbookId}`);
        if (result.sourceUrl) lines.push(`Source URL: ${result.sourceUrl}`);
        if (result.downloadUrl) lines.push(`Download URL: ${result.downloadUrl}`);
        if (result.llmFallbackUsed) lines.push("LLM Fallback: used");
        if (Number.isFinite(result.elapsedSeconds)) lines.push(`Elapsed: ${result.elapsedSeconds} s`);
        lines.push(`Library Root: ${result.libraryRoot}`);
        return {
          content: [
            {
              type: "text",
              text: lines.join("\n"),
            },
          ],
        };
      },
    });
  },
};

export default libraryFetcherPlugin;
