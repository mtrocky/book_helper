let pluginLogger = null;

export function configureLogger(logger) {
  pluginLogger = logger && typeof logger === "object" ? logger : null;
}

export function isDebugEnabled() {
  return (
    process.env.DEBUG_BOOK_FETCH === "1" ||
    process.env.DEBUG_BOOK_FETCH === "true"
  );
}

export function isInfoEnabled() {
  return (
    process.env.QUIET_BOOK_FETCH !== "1" &&
    process.env.QUIET_BOOK_FETCH !== "true"
  );
}

export function debug(message, details) {
  if (pluginLogger?.debug) {
    pluginLogger.debug(formatLogMessage(message, details));
    return;
  }

  if (!isDebugEnabled()) {
    return;
  }

  writeLog(message, details);
}

export function info(message, details) {
  if (pluginLogger?.info) {
    pluginLogger.info(formatLogMessage(message, details));
    return;
  }

  if (!isInfoEnabled()) {
    return;
  }

  writeLog(message, details);
}

function writeLog(message, details) {
  if (details === undefined) {
    process.stderr.write(`[book-fetch] ${message}\n`);
    return;
  }

  process.stderr.write(
    `[book-fetch] ${message}: ${safeStringify(details)}\n`,
  );
}

function formatLogMessage(message, details) {
  if (details === undefined) {
    return `[book-fetch] ${message}`;
  }
  return `[book-fetch] ${message}: ${safeStringify(details)}`;
}

function safeStringify(value) {
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}
