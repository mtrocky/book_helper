import path from "node:path";

const DOWNLOAD_EXTENSIONS = [".pdf", ".epub", ".mobi", ".azw3", ".djvu", ".txt", ".zip"];
const MIN_RESULT_SCORE = 24;
const MIN_DYNAMIC_DOWNLOAD_TIMEOUT_MS = 15000;
const MAX_DYNAMIC_DOWNLOAD_TIMEOUT_MS = 300000;
const MIN_DOWNLOAD_TIMEOUT_WITHOUT_SIZE_MS = 60000;

export function pickResult(results, params) {
  if (!Array.isArray(results) || results.length === 0) {
    return null;
  }

  if (params.resultIndex && results[params.resultIndex - 1]) {
    return results[params.resultIndex - 1];
  }

  const best = results
    .map((result) => ({ result, score: scoreResult(result, params) }))
    .sort((left, right) => right.score - left.score)[0];

  if (!best || best.score < MIN_RESULT_SCORE) {
    return null;
  }

  return best.result;
}

export function scoreResult(result, params) {
  const title = normalizeText(result.title);
  const author = normalizeText(result.author);
  const language = normalizeLanguage(result.language);
  const query = normalizeText(params.query);
  const titleHint = normalizeText(params.titleHint);
  const authorHint = normalizeText(params.authorHint);
  const languageHint = normalizeLanguage(params.languageHint);

  let score = 0;
  if (titleHint && title === titleHint) score += 140;
  if (title.includes(query)) score += 120;
  if (authorHint && author.includes(authorHint)) score += 40;
  score += languagePreferenceScore(language, languageHint);

  const haystack = `${title} ${author}`;
  const tokens = tokenize(params.titleHint || params.query);
  score += tokens.filter((token) => haystack.includes(token)).length * 16;
  score += formatPreferenceScore(result.format, result.downloadUrl);
  return score;
}

export function languagePreferenceScore(language, languageHint = "") {
  const normalizedLanguage = normalizeLanguage(language);
  const normalizedHint = normalizeLanguage(languageHint);

  if (normalizedHint) {
    if (normalizedLanguage === normalizedHint) {
      return 55;
    }
    if (
      normalizedHint === "chinese" &&
      normalizedLanguage === "traditional_chinese"
    ) {
      return 8;
    }
    if (
      normalizedHint === "traditional_chinese" &&
      normalizedLanguage === "chinese"
    ) {
      return 2;
    }
    return normalizedLanguage ? -24 : 0;
  }

  switch (normalizedLanguage) {
    case "chinese":
      return 26;
    case "english":
      return 18;
    case "traditional_chinese":
      return 10;
    case "spanish":
    case "french":
      return 4;
    default:
      return 0;
  }
}

export function formatPreferenceScore(format, downloadUrl = "") {
  const extension = inferFormatExtension(format, downloadUrl);
  const sizeBytes = parseSizeHintToBytes(format ?? "");
  const sizeMb = Number.isFinite(sizeBytes) && sizeBytes > 0 ? sizeBytes / (1024 * 1024) : null;

  switch (extension) {
    case ".epub": {
      let score = 28;
      if (sizeMb && sizeMb <= 8) score += 6;
      else if (sizeMb && sizeMb <= 20) score += 3;
      return score;
    }
    case ".pdf": {
      let score = 24;
      if (sizeMb && sizeMb >= 80) score -= 18;
      else if (sizeMb && sizeMb >= 40) score -= 10;
      else if (sizeMb && sizeMb >= 20) score -= 5;
      return score;
    }
    case ".mobi":
      return 6;
    case ".txt":
      return 2;
    case ".djvu":
    case ".zip":
      return -4;
    case ".azw3":
      return -14;
    default:
      return 0;
  }
}

export function guessExtension(detail) {
  const downloadUrl = detail.downloadUrl || "";
  return inferFormatExtension(detail.format, downloadUrl) || ".bin";
}

export function estimateDownloadTimeoutMs({ baseTimeoutMs, format }) {
  const normalizedBase = Math.max(
    baseTimeoutMs || MIN_DYNAMIC_DOWNLOAD_TIMEOUT_MS,
    MIN_DYNAMIC_DOWNLOAD_TIMEOUT_MS,
  );
  const estimatedBytes = parseSizeHintToBytes(format ?? "");
  if (!estimatedBytes) {
    return Math.max(normalizedBase, MIN_DOWNLOAD_TIMEOUT_WITHOUT_SIZE_MS);
  }

  const estimatedMb = estimatedBytes / (1024 * 1024);
  const handshakeMs = 10000;
  const perMbMs = 1200;
  const dynamicTimeoutMs = Math.round(handshakeMs + estimatedMb * perMbMs);
  return Math.max(normalizedBase, Math.min(dynamicTimeoutMs, MAX_DYNAMIC_DOWNLOAD_TIMEOUT_MS));
}

export function parseSizeHintToBytes(value) {
  const text = String(value || "").trim();
  if (!text) return null;

  const match = text.match(/(\d+(?:\.\d+)?)\s*(KB|MB|GB|TB)\b/i);
  if (!match) return null;
  const amount = Number(match[1]);
  if (!Number.isFinite(amount) || amount <= 0) return null;

  const unit = match[2].toUpperCase();
  const multipliers = {
    KB: 1024,
    MB: 1024 ** 2,
    GB: 1024 ** 3,
    TB: 1024 ** 4,
  };
  return Math.round(amount * (multipliers[unit] ?? 1));
}

export function buildSafeFileName(title, extension) {
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

export function normalizeLanguage(value) {
  const normalized = String(value || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
  if (!normalized) return "";

  const compact = normalized.replace(/[-_]/g, " ");
  if (
    compact === "en" ||
    compact === "eng" ||
    compact === "english" ||
    compact === "英文" ||
    compact === "英语"
  ) {
    return "english";
  }
  if (
    compact === "zh" ||
    compact === "zh cn" ||
    compact === "zh hans" ||
    compact === "zh sg" ||
    compact === "simplified chinese" ||
    compact === "chinese" ||
    compact === "中文" ||
    compact === "汉语" ||
    compact === "简体中文"
  ) {
    return "chinese";
  }
  if (
    compact === "zh tw" ||
    compact === "zh hk" ||
    compact === "zh hant" ||
    compact === "traditional chinese" ||
    compact === "繁體中文" ||
    compact === "繁体中文"
  ) {
    return "traditional_chinese";
  }
  if (
    compact === "ja" ||
    compact === "jpn" ||
    compact === "japanese" ||
    compact === "日文" ||
    compact === "日语" ||
    compact === "日本語"
  ) {
    return "japanese";
  }
  if (
    compact === "fr" ||
    compact === "fra" ||
    compact === "fre" ||
    compact === "french" ||
    compact === "法文" ||
    compact === "法语"
  ) {
    return "french";
  }
  if (
    compact === "de" ||
    compact === "deu" ||
    compact === "ger" ||
    compact === "german" ||
    compact === "德文" ||
    compact === "德语"
  ) {
    return "german";
  }
  if (
    compact === "es" ||
    compact === "spa" ||
    compact === "spanish" ||
    compact === "西班牙文" ||
    compact === "西班牙语"
  ) {
    return "spanish";
  }
  return normalized;
}

function inferFormatExtension(format, downloadUrl) {
  try {
    const extension = path.extname(new URL(downloadUrl || "").pathname).toLowerCase();
    if (DOWNLOAD_EXTENSIONS.includes(extension)) {
      return extension;
    }
  } catch {}

  const normalizedFormat = String(format ?? "").toLowerCase();
  for (const extension of DOWNLOAD_EXTENSIONS) {
    if (normalizedFormat.includes(extension.slice(1))) {
      return extension;
    }
  }
  return "";
}
