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
  if (!isDebugEnabled()) {
    return;
  }

  writeLog(message, details);
}

export function info(message, details) {
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

function safeStringify(value) {
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}
