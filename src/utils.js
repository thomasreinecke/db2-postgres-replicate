/**
 * Convert DB2 types to PostgreSQL types.
 */
export function mapDb2ToPostgresType(db2Type, length) {
  const typeMapping = {
      "INTEGER": "INTEGER",
      "SMALLINT": "SMALLINT",
      "BIGINT": "BIGINT",
      "DECIMAL": `DECIMAL(${length},2)`,
      "DOUBLE": "DOUBLE PRECISION",
      "REAL": "REAL",
      "CHAR": "TEXT",
      "VARCHAR": "TEXT",
      "CLOB": "TEXT",
      "DATE": "DATE",
      "TIMESTAMP": "TIMESTAMP",
  };
  return typeMapping[db2Type] || "TEXT";
}

/**
 * Extract the last primary key values from the fetched chunk.
 */
export function getLastPrimaryKeyFromChunk(chunk, primaryKeys) {
  if (!chunk.length || primaryKeys.length === 0) return null;

  const lastRow = chunk[chunk.length - 1];
  
  return primaryKeys.reduce((acc, key) => {
    acc[key] = lastRow[key];
    return acc;
  }, {});
}

/**
 * Format values properly for SQL insertion (handle NULLs, timestamps, and numbers correctly).
 */
export function formatValue(value, type) {
  if (value === null || value === '') return 'NULL';
  if (type.includes('TEXT') || type.includes('CHAR')) {
      return `'${String(value).trim().replace(/'/g, "''")}'`;
  }
  if (type.includes('TIMESTAMP') || type.includes('DATE')) {
      return `'${value}'`;
  }
  if (type.includes('INTEGER') || type.includes('SMALLINT') || type.includes('BIGINT') || type.includes('DECIMAL') || type.includes('NUMERIC') || type.includes('DOUBLE') || type.includes('REAL')) {
      return value;
  }
  return `'${value}'`;
}

/**
 * Splits an array into chunks of a given size.
 */
export function chunkArray(array, size) {
  const chunkedArr = [];
  for (let i = 0; i < array.length; i += size) {
      chunkedArr.push(array.slice(i, i + size));
  }
  return chunkedArr;
}

/**
 * Format a JavaScript Date object into DB2 TIMESTAMP format: "YYYY-MM-DD HH:MM:SS"
 */
export function formatTimestampForDB2(date) {
  const pad = (num) => num.toString().padStart(2, '0');
  const year = date.getFullYear();
  const month = pad(date.getMonth() + 1);
  const day = pad(date.getDate());
  const hours = pad(date.getHours());
  const minutes = pad(date.getMinutes());
  const seconds = pad(date.getSeconds());
  return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
}
