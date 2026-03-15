import api from './api';

function safeFilename(value, fallback = 'export') {
  const cleaned = String(value || '').trim().replace(/[<>:"/\\|?*\x00-\x1F]+/g, '_').replace(/\s+/g, '_');
  return cleaned || fallback;
}

export const SMART_STREAM_THRESHOLD_DOCS = 20000;
const DB_EXPORT_TIMEOUT_DEFAULT_MS = 4 * 60 * 60 * 1000;

export function shouldUseSmartStreamExport({
  limitChoice = '',
  limitValue = null,
  estimate = null,
  threshold = SMART_STREAM_THRESHOLD_DOCS,
  force = false,
} = {}) {
  if (typeof window === 'undefined' || typeof window.showSaveFilePicker !== 'function') return false;
  if (force) return true;
  const mode = String(limitChoice || '').trim().toLowerCase();
  if (mode === 'exact' || mode === 'unlimited' || mode === 'all') return true;
  const numericLimit = Number(limitValue);
  if (Number.isFinite(numericLimit) && numericLimit >= Number(threshold || SMART_STREAM_THRESHOLD_DOCS)) return true;
  const numericEstimate = Number(estimate);
  if (Number.isFinite(numericEstimate) && numericEstimate >= Number(threshold || SMART_STREAM_THRESHOLD_DOCS)) return true;
  return false;
}

export function downloadTextFile(filename, text, mime = 'application/json') {
  const blob = new Blob([text], { type: mime });
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = filename;
  link.rel = 'noopener';
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
}

function docsToCsv(docs = []) {
  if (!Array.isArray(docs) || docs.length === 0) return '';
  const keys = [...new Set(docs.flatMap((doc) => Object.keys(doc || {})))];
  const header = keys.join(',');
  const rows = docs.map((doc) => keys.map((key) => {
    const value = doc?.[key];
    if (value === null || value === undefined) return '';
    if (typeof value === 'object') return `"${JSON.stringify(value).replace(/"/g, '""')}"`;
    const str = String(value);
    return (str.includes(',') || str.includes('"') || str.includes('\n'))
      ? `"${str.replace(/"/g, '""')}"`
      : str;
  }).join(','));
  return [header, ...rows].join('\n');
}

async function buildDbPackage(
  dbName,
  {
    includeIndexes = true,
    includeSchema = true,
    includeDocuments = true,
    includeOptions = true,
    limitPerCollection = 0,
    schemaSampleSize = 150,
    heavyTimeoutMs = DB_EXPORT_TIMEOUT_DEFAULT_MS,
    heavyConfirm = true,
    controller,
    onProgress,
  } = {},
) {
  return api.exportDatabase(
    dbName,
    {
      includeDocuments,
      includeIndexes,
      includeOptions,
      includeSchema,
      limitPerCollection,
      schemaSampleSize,
    },
    {
      heavyTimeoutMs,
      heavyConfirm,
      controller,
      onProgress,
    },
  );
}

async function buildFilesForDatabase(dbName, options = {}) {
  const mode = options.mode === 'collections' ? 'collections' : 'package';
  const collectionFormat = options.collectionFormat === 'csv' ? 'csv' : 'json';
  const pkg = await buildDbPackage(dbName, options);
  const packageFilename = pkg.filename || `${safeFilename(dbName)}.mongostudio-db.json`;
  const packageText = typeof pkg?.data === 'string'
    ? pkg.data
    : JSON.stringify(pkg?.data || {}, null, 2);

  if (mode === 'package') {
    return [{
      path: `${safeFilename(dbName)}/${packageFilename}`,
      filename: packageFilename,
      text: packageText,
      mime: 'application/json',
    }];
  }

  let parsed;
  if (pkg?.data && typeof pkg.data === 'object' && !Array.isArray(pkg.data)) {
    parsed = pkg.data;
  } else {
    try {
      parsed = JSON.parse(packageText);
    } catch {
      throw new Error(`Failed to parse export package for database "${dbName}".`);
    }
  }
  const collections = Array.isArray(parsed?.collections) ? parsed.collections : [];
  return collections.map((entry) => {
    const docs = Array.isArray(entry?.documents) ? entry.documents : [];
    const colName = safeFilename(entry?.name || 'collection');
    const ext = collectionFormat === 'csv' ? 'csv' : 'json';
    const filename = `${safeFilename(dbName)}.${colName}.${ext}`;
    const text = collectionFormat === 'csv' ? docsToCsv(docs) : JSON.stringify(docs, null, 2);
    return {
      path: `${safeFilename(dbName)}/${colName}.${ext}`,
      filename,
      text,
      mime: collectionFormat === 'csv' ? 'text/csv' : 'application/json',
    };
  });
}

async function downloadFilesAsZip(files, archiveName = 'mongostudio-export') {
  if (!files.length) throw new Error('No export files generated.');
  const JSZip = (await import('jszip')).default;
  const zip = new JSZip();
  files.forEach((file) => {
    zip.file(file.path || file.filename, file.text);
  });
  const blob = await zip.generateAsync({ type: 'blob', compression: 'DEFLATE', compressionOptions: { level: 6 } });
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = `${safeFilename(archiveName)}.zip`;
  link.rel = 'noopener';
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
}

async function downloadFilesSeparately(files) {
  if (!files.length) throw new Error('No export files generated.');
  for (let idx = 0; idx < files.length; idx += 1) {
    const file = files[idx];
    downloadTextFile(file.filename, file.text, file.mime);
    if (idx < files.length - 1) {
      // Small gap helps browsers queue multiple downloads reliably.
      // eslint-disable-next-line no-await-in-loop
      await new Promise((resolve) => setTimeout(resolve, 90));
    }
  }
}

export async function exportSingleDatabase(dbName, options = {}) {
  const archive = options.archive !== false;
  const mode = options.mode === 'collections' ? 'collections' : 'package';
  const canStreamToDisk = typeof window !== 'undefined' && typeof window.showSaveFilePicker === 'function';
  const canUseApiDirectStream = typeof api?.exportDatabaseToFile === 'function';
  if (mode === 'package' && canStreamToDisk && canUseApiDirectStream) {
    await api.exportDatabaseToFile(
      dbName,
      {
        includeDocuments: options.includeDocuments !== false,
        includeIndexes: options.includeIndexes !== false,
        includeOptions: options.includeOptions !== false,
        includeSchema: options.includeSchema !== false,
        limitPerCollection: Number(options.limitPerCollection) || 0,
        schemaSampleSize: Number(options.schemaSampleSize) || 150,
      },
      {
        heavyTimeoutMs: options.heavyTimeoutMs,
        heavyConfirm: options.heavyConfirm,
        controller: options.controller,
        onProgress: options.onProgress,
        budget: options.budget,
        filename: archive
          ? `${safeFilename(dbName)}.mongostudio-db.zip`
          : `${safeFilename(dbName)}.mongostudio-db.json`,
        archive,
      },
    );
    return { files: 1, archive, streamed: true };
  }
  const files = await buildFilesForDatabase(dbName, options);
  if (archive) {
    try {
      await downloadFilesAsZip(files, `${dbName}-export`);
      return { files: files.length, archive: true };
    } catch {
      // Fallback keeps export working even if zip chunk loading fails in current runtime.
      await downloadFilesSeparately(files);
      return { files: files.length, archive: false, fallback: 'separate' };
    }
  }
  await downloadFilesSeparately(files);
  return { files: files.length, archive: false };
}

export async function exportMultipleDatabases(dbNames = [], options = {}) {
  const names = [...new Set((dbNames || []).map((name) => String(name || '').trim()).filter(Boolean))];
  if (!names.length) throw new Error('No databases to export.');
  const onProgress = typeof options?.onProgress === 'function' ? options.onProgress : null;
  const files = [];
  for (let index = 0; index < names.length; index += 1) {
    const dbName = names[index];
    // eslint-disable-next-line no-await-in-loop
    const dbFiles = await buildFilesForDatabase(dbName, {
      ...options,
      onProgress: onProgress
        ? (progress) => onProgress({ ...(progress || {}), database: dbName, databaseIndex: index + 1, databaseTotal: names.length })
        : undefined,
    });
    files.push(...dbFiles);
  }
  const archive = options.archive !== false;
  if (archive) {
    try {
      await downloadFilesAsZip(files, options.archiveName || 'all-databases-export');
      return { databases: names.length, files: files.length, archive: true };
    } catch {
      await downloadFilesSeparately(files);
      return { databases: names.length, files: files.length, archive: false, fallback: 'separate' };
    }
  }
  await downloadFilesSeparately(files);
  return { databases: names.length, files: files.length, archive: false };
}

