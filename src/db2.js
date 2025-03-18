// src/db2.js

import ibmdb from 'ibm_db';
import config from './config.js';
import { mapDb2ToPostgresType } from './utils.js';

/**
 * Construct the DB2 connection string if not explicitly provided in config.
 */
function getDB2ConnectionString() {
    if (config.db2.connectionString) {
        return config.db2.connectionString;
    }
    return `DATABASE=${config.db2.dbname};HOSTNAME=${config.db2.host};PORT=${config.db2.port};` +
        `UID=${config.db2.user};PWD=${config.db2.password};SECURITY=SSL;SSLConnection=true;`;
}

/**
 * Connect to DB2 and return a promise-based connection.
 */
export function connectDB2() {
    return new Promise((resolve, reject) => {
        const connectionString = getDB2ConnectionString();
        if (!connectionString || typeof connectionString !== 'string') {
            return reject(new Error(`Invalid DB2 connection string: ${connectionString}`));
        }
        console.log(`🔌 Connecting to DB2`);
        ibmdb.open(connectionString, (err, conn) => {
            if (err) {
                console.error(`❌ DB2 Connection Error: ${err.message}`);
                reject(err);
            } else {
                console.log(`✅ Connected to DB2`);
                resolve(conn);
            }
        });
    });
}

/**
 * Fetch column metadata from DB2 for a given table.
 */
export async function getColumnMetadata(db2, schema, table) {
    try {
        const result = await new Promise((resolve, reject) => {
            db2.query(
                `SELECT COLNAME, TYPENAME, LENGTH FROM SYSCAT.COLUMNS WHERE TABSCHEMA = ? AND TABNAME = ?`,
                [schema.toUpperCase(), table.toUpperCase()],
                (err, res) => (err ? reject(err) : resolve(res))
            );
        });

        return result.map(col => ({
            name: col.COLNAME.trim(),
            type: mapDb2ToPostgresType(col.TYPENAME, col.LENGTH),
        }));
    } catch (error) {
        console.error(`⚠️ Failed to fetch column metadata for ${schema}.${table}: ${error.message}`);
        return [];
    }
}

/**
 * Fetch primary key columns for a given table.
 */
export async function getPrimaryKeys(db2, schema, table) {
    try {
        const result = await new Promise((resolve, reject) => {
            db2.query(
                `SELECT COLNAME FROM SYSCAT.KEYCOLUSE
         WHERE TABSCHEMA = ? AND TABNAME = ?
         ORDER BY COLSEQ`,
                [schema.toUpperCase(), table.toUpperCase()],
                (err, res) => (err ? reject(err) : resolve(res))
            );
        });

        return result.map(row => row.COLNAME.trim());
    } catch (error) {
        console.error(`⚠️ Failed to fetch primary keys for ${schema}.${table}: ${error.message}`);
        return [];
    }
}

/**
 * Fetch a chunk of data from DB2 for a given table.
 */
export async function fetchChunk(db2, schema, table, columns, chunkSize, offset) {
  // Get primary keys
  const primaryKeys = await getPrimaryKeys(db2, schema.toUpperCase(), table.toUpperCase());

  let orderByClause = '';
  if (primaryKeys && primaryKeys.length > 0) {
      // Create ORDER BY clause using primary keys
      const orderByColumns = primaryKeys.map(pk => `"${pk}" ASC`).join(', ');
      orderByClause = `ORDER BY ${orderByColumns}`;
  }

  const query = `SELECT * FROM ${schema}.${table} ${orderByClause} OFFSET ${offset} ROWS FETCH FIRST ${chunkSize} ROWS ONLY`;

  try {
      return await new Promise((resolve, reject) => {
          db2.query(query, (err, res) => {
              if (err) {
                  console.error(`⚠️ Error fetching chunk for ${schema}.${table}: ${err.message}`);
                  console.error(`❌ Query: ${query}`);
                  reject(err);
              } else {
                  resolve(res);
              }
          });
      });
  } catch (error) {
      console.error(`⚠️ Unexpected error fetching chunk for ${schema}.${table}: ${error.message}`);
      return [];
  }
}

/**
 * Get the exact total row count from DB2 using SELECT COUNT(*).
 */
export async function getDb2RowCount(db2, schema, table) {
  try {
      const result = await new Promise((resolve, reject) => {
          db2.query(
              `SELECT COUNT(*) AS COUNT FROM ${schema.toUpperCase()}.${table.toUpperCase()}`,
              (err, res) => (err ? reject(err) : resolve(res))
          );
      });
      if (result.length && result[0].COUNT) {
          return parseInt(result[0].COUNT, 10);
      }
      return 0;
  } catch (error) {
      console.error(`⚠️ Error getting exact row count for ${schema}.${table}: ${error.message}`);
      return 0;
  }
}


/**
 * Get the view definition from DB2.
 */
export async function getViewDefinition(db2, schema, viewName) {
  try {
      const query = `SELECT TEXT FROM SYSCAT.VIEWS WHERE VIEWSCHEMA = '${schema}' AND VIEWNAME = '${viewName}'`;
      console.log(`\nDB2 getViewDefinition Query:\n${query}\n`); // Log the query

      const result = await new Promise((resolve, reject) => {
          db2.query(
              query,
              (err, res) => (err ? reject(err) : resolve(res))
          );
      });
      if (result.length && result[0].TEXT) {
          const viewDefinition = result[0].TEXT;
          // Extract the part after "AS"
          const asIndex = viewDefinition.toUpperCase().indexOf('AS');
          if (asIndex !== -1) {
              let selectStatement = viewDefinition.substring(asIndex + 2).trim(); // +2 to skip "AS"
              // Wrap schema and table names in double quotes
              selectStatement = selectStatement.replace(/EDM_COMP\.LOCN_COMPANY_XREF/g, '"EDM_COMP"."LOCN_COMPANY_XREF"');
              return selectStatement;
          }
          return ''; // Return empty string if "AS" is not found
      }
      return '';
  } catch (error) {
      console.error(`⚠️ Error getting view definition for ${schema}.${viewName}: ${error.message}`);
      return '';
  }
}