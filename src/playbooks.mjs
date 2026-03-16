import { access, readFile } from "node:fs/promises";
import path from "node:path";
import { debug } from "./debug.mjs";

const STEP_TYPES = new Set([
  "open",
  "wait",
  "fill",
  "click",
  "press",
  "command",
  "extract-results",
  "open-result",
  "download-result",
  "extract-detail",
  "download",
]);

export const DEFAULT_AGENT_BROWSER_SESSION_CONFIG_PATH = path.join(
  process.cwd(),
  "Runtime",
  "agent-browser-session.json",
);

export function parsePluginConfig(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {};
  }

  const config = value;
  return {
    defaultLibraryRoot:
      typeof config.defaultLibraryRoot === "string" ? config.defaultLibraryRoot : undefined,
    playbooksDir: typeof config.playbooksDir === "string" ? config.playbooksDir : undefined,
    defaultSiteId: typeof config.defaultSiteId === "string" ? config.defaultSiteId : undefined,
    defaultPlaybookPath:
      typeof config.defaultPlaybookPath === "string" ? config.defaultPlaybookPath : undefined,
    browserProfilePath:
      typeof config.browserProfilePath === "string" ? config.browserProfilePath : undefined,
    agentBrowserSessionConfigPath:
      typeof config.agentBrowserSessionConfigPath === "string"
        ? config.agentBrowserSessionConfigPath
        : undefined,
    agentBrowserPath:
      typeof config.agentBrowserPath === "string" ? config.agentBrowserPath : undefined,
    downloadTimeoutMs:
      Number.isInteger(config.downloadTimeoutMs) && config.downloadTimeoutMs > 0
        ? config.downloadTimeoutMs
        : undefined,
    llmFallbackCommand:
      typeof config.llmFallbackCommand === "string" ? config.llmFallbackCommand : undefined,
    llmFallbackArgs: Array.isArray(config.llmFallbackArgs)
      ? config.llmFallbackArgs.filter((entry) => typeof entry === "string")
      : undefined,
  };
}

export async function describeStatus(pluginConfig, toolPolicySource = null) {
  const config = parsePluginConfig(pluginConfig);
  const playbooksDir = path.resolve(config.playbooksDir ?? path.join(process.cwd(), "playbooks"));
  const defaultPlaybookPath = config.defaultPlaybookPath
    ? path.resolve(config.defaultPlaybookPath)
    : config.defaultSiteId
    ? path.join(playbooksDir, `${config.defaultSiteId}.json`)
    : "(unset)";
  const playbookExists =
    defaultPlaybookPath !== "(unset)" && (await exists(defaultPlaybookPath)) ? "present" : "missing";
  const libraryRoot = config.defaultLibraryRoot ?? "(unset)";
  const libraryExists =
    libraryRoot !== "(unset)" && (await exists(libraryRoot)) ? "present" : "missing";
  const sessionConfigPath = path.resolve(
    config.agentBrowserSessionConfigPath ?? DEFAULT_AGENT_BROWSER_SESSION_CONFIG_PATH,
  );
  const sessionConfigExists = (await exists(sessionConfigPath)) ? "present" : "missing";

  return [
    "Library fetcher status:",
    "- execution model: agent-browser playbook with optional LLM repair fallback",
    `- tool policy: ${toolPolicySource ?? "(not enabled)"}`,
    `- playbooks dir: ${playbooksDir}`,
    `- default site id: ${config.defaultSiteId ?? "(unset)"}`,
    `- default playbook: ${defaultPlaybookPath} [${playbookExists}]`,
    `- default library root: ${libraryRoot} [${libraryExists}]`,
    `- agent-browser session config: ${sessionConfigPath} [${sessionConfigExists}]`,
    `- agent-browser executable: ${config.agentBrowserPath ?? "agent-browser"}`,
    `- LLM fallback command: ${config.llmFallbackCommand ?? "(unset)"}`,
    "",
    "Playbook model:",
    "- each supported site lives in its own JSON playbook",
    "- playbook steps run deterministically through agent-browser",
    "- LLM fallback only runs when a step fails or a result is ambiguous",
    "",
    "Commands:",
    "/bookfetch status",
    "/bookfetch doctor",
    "/bookfetch enable",
  ].join("\n");
}

export async function loadAgentBrowserSessionConfig(pluginConfig) {
  const config = parsePluginConfig(pluginConfig);
  const sessionConfigPath = path.resolve(
    config.agentBrowserSessionConfigPath ?? DEFAULT_AGENT_BROWSER_SESSION_CONFIG_PATH,
  );
  if (!(await exists(sessionConfigPath))) {
    return {
      sessionConfigPath,
      exists: false,
      config: {},
    };
  }

  const raw = await readFile(sessionConfigPath, "utf8");
  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch (error) {
    throw new Error(`Invalid agent-browser session config JSON at ${sessionConfigPath}.`);
  }

  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error(`Invalid agent-browser session config at ${sessionConfigPath}: expected an object.`);
  }

  return {
    sessionConfigPath,
    exists: true,
    config: {
      sessionName: typeof parsed.sessionName === "string" ? parsed.sessionName.trim() : undefined,
      profilePath: typeof parsed.profilePath === "string" ? parsed.profilePath.trim() : undefined,
      headed: typeof parsed.headed === "boolean" ? parsed.headed : undefined,
      ignoreHttpsErrors:
        typeof parsed.ignoreHttpsErrors === "boolean" ? parsed.ignoreHttpsErrors : undefined,
      proxy: typeof parsed.proxy === "string" ? parsed.proxy.trim() : undefined,
    },
  };
}

export async function resolvePlaybook({ siteId, playbookPath, pluginConfig }) {
  const config = parsePluginConfig(pluginConfig);
  const resolvedPlaybookPath = resolvePlaybookPath({ siteId, playbookPath, pluginConfig: config });
  debug("resolving playbook path", {
    requestedSiteId: siteId ?? null,
    requestedPlaybookPath: playbookPath ?? null,
    resolvedPlaybookPath,
  });

  if (!resolvedPlaybookPath) {
    return { playbook: null, playbookPath: null };
  }

  const raw = await readFile(resolvedPlaybookPath, "utf8");
  const parsed = JSON.parse(raw);
  debug("loaded playbook file", {
    resolvedPlaybookPath,
    bytes: raw.length,
  });
  return {
    playbook: normalizePlaybook(parsed, resolvedPlaybookPath),
    playbookPath: resolvedPlaybookPath,
  };
}

function resolvePlaybookPath({ siteId, playbookPath, pluginConfig }) {
  if (playbookPath) {
    return path.resolve(playbookPath);
  }

  if (pluginConfig.defaultPlaybookPath) {
    return path.resolve(pluginConfig.defaultPlaybookPath);
  }

  const resolvedSiteId = siteId ?? pluginConfig.defaultSiteId;
  if (!resolvedSiteId) {
    return null;
  }

  const playbooksDir = path.resolve(pluginConfig.playbooksDir ?? path.join(process.cwd(), "playbooks"));
  return path.join(playbooksDir, `${resolvedSiteId}.json`);
}

function normalizePlaybook(parsed, playbookPath) {
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error(`Invalid playbook at ${playbookPath}: expected an object.`);
  }

  if (!Array.isArray(parsed.steps) || parsed.steps.length === 0) {
    throw new Error(`Invalid playbook at ${playbookPath}: steps[] is required.`);
  }

  const steps = parsed.steps.map((step, index) => normalizeStep(step, index, playbookPath));

  return {
    id:
      typeof parsed.id === "string"
        ? parsed.id
        : path.basename(playbookPath, path.extname(playbookPath)),
    name: typeof parsed.name === "string" ? parsed.name : "library-site",
    notes: Array.isArray(parsed.notes)
      ? parsed.notes.filter((entry) => typeof entry === "string")
      : [],
    allowedDomains: Array.isArray(parsed.allowedDomains)
      ? parsed.allowedDomains.filter((entry) => typeof entry === "string")
      : [],
    browser: normalizeBrowser(parsed.browser),
    llmFallback: normalizeLlmFallback(parsed.llmFallback),
    steps,
    path: playbookPath,
  };
}

function normalizeStep(rawStep, index, playbookPath) {
  if (!rawStep || typeof rawStep !== "object" || Array.isArray(rawStep)) {
    throw new Error(`Invalid playbook at ${playbookPath}: step ${index + 1} must be an object.`);
  }

  if (typeof rawStep.type !== "string" || !STEP_TYPES.has(rawStep.type)) {
    throw new Error(
      `Invalid playbook at ${playbookPath}: step ${index + 1} has unsupported type "${rawStep.type}".`,
    );
  }

  return {
    ...rawStep,
    name: typeof rawStep.name === "string" ? rawStep.name : `step-${index + 1}`,
  };
}

function normalizeBrowser(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {};
  }

  return {
    sessionName: typeof value.sessionName === "string" ? value.sessionName : undefined,
    profilePath: typeof value.profilePath === "string" ? value.profilePath : undefined,
    headed: Boolean(value.headed),
    ignoreHttpsErrors: Boolean(value.ignoreHttpsErrors),
    proxy: typeof value.proxy === "string" ? value.proxy : undefined,
    requireAuthenticatedProfile: Boolean(value.requireAuthenticatedProfile),
  };
}

function normalizeLlmFallback(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return { enabled: false, notes: [] };
  }

  return {
    enabled: value.enabled !== false,
    goal: typeof value.goal === "string" ? value.goal : undefined,
    notes: Array.isArray(value.notes)
      ? value.notes.filter((entry) => typeof entry === "string")
      : [],
    command: typeof value.command === "string" ? value.command : undefined,
    args: Array.isArray(value.args) ? value.args.filter((entry) => typeof entry === "string") : undefined,
  };
}

async function exists(targetPath) {
  try {
    await access(targetPath);
    return true;
  } catch {
    return false;
  }
}
