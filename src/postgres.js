// src/postgres.js

import pkg from 'pg';
import config from './config.js';
import chalk from 'chalk';

const { Client } = pkg;

export const pgClient = new Client({
    host: config.postgres.host,
    port: config.postgres.port,
    user: config.postgres.user,
    password: config.postgres.password,
    database: config.postgres.database,
});

await pgClient.connect();
console.log(chalk.green('✅ Connected to PostgreSQL'));

/**
 * Drop a schema in PostgreSQL.
 */
export async function dropSchema(pgClient, schema) {
    try {
        await pgClient.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
        console.log(chalk.red(`🔥 Schema dropped: ${schema}`));
    } catch (error) {
        console.error(chalk.red(`❌ Failed to drop schema ${schema}: ${error.message}`));
        throw error;
    }
}

/**
 * Ensure a schema exists in PostgreSQL.
 */
export async function ensureSchemaExists(pgClient, schema) {
    try {
        await pgClient.query(`CREATE SCHEMA IF NOT EXISTS "${schema}"`);
        console.log(chalk.green(`✅ Schema ensured: ${schema}`));
    } catch (error) {
        console.error(chalk.red(`❌ Failed to ensure schema ${schema}: ${error.message}`));
        throw error;
    }
}

/**
 * Ensure a table exists in PostgreSQL.
 */
export async function ensureTableExists(pgClient, schema, table, columns, primaryKeys) {
    const columnDefinitions = columns.map(col => `"${col.name}" ${col.type}`).join(', ');
    try {
        await pgClient.query(`CREATE TABLE IF NOT EXISTS "${schema}"."${table}" (${columnDefinitions})`);
        console.log(chalk.green(`✅ Table ensured: ${schema}.${table}`));
    } catch (error) {
        console.error(chalk.red(`❌ Failed to ensure table ${schema}.${table}: ${error.message}`));
        throw error;
    }
}

/**
 * Get the last primary key values from the PostgreSQL table.
 */
export async function getLastPrimaryKey(pgClient, schema, table, primaryKeys, columns) {
    if (primaryKeys.length === 0) return null;

    try {
        const result = await pgClient.query(
            `SELECT ${primaryKeys.map(pk => `"${pk}"`).join(', ')} FROM "${schema}"."${table}" ORDER BY ${primaryKeys.map(pk => `"${pk}" DESC`).join(', ')} LIMIT 1`
        );

        if (result.rows.length > 0) {
            return result.rows[0];
        }
        return null;
    } catch (error) {
        console.error(chalk.red(`⚠️ Failed to get last primary key from ${schema}.${table}: ${error.message}`));
        return null;
    }
}

/**
 * Insert a chunk of data into the PostgreSQL table using batch inserts.
 */
export async function insertIntoPostgres(pgClient, schema, table, columns, chunk) {
    if (!chunk || chunk.length === 0) {
        console.warn(chalk.yellow(`⚠️ Skipping empty chunk for ${schema}.${table}`));
        return 0;
    }

    const batchSize = 1000; // Adjust batch size as needed
    let totalInserted = 0;

    for (let i = 0; i < chunk.length; i += batchSize) {
        const batch = chunk.slice(i, i + batchSize);

        const columnNames = columns.map((col) => `"${col.name}"`).join(',');
        const valuePlaceholders = batch
            .map((row) => `(${columns.map((col, index) => `$${index + 1 + columns.length * batch.indexOf(row)}`).join(',')})`)
            .join(',');
        const values = batch.flatMap((row) => columns.map((col) => row[col.name]));

        const query = `INSERT INTO "${schema}"."${table}" (${columnNames}) VALUES ${valuePlaceholders}`;

        if (values.length === 0) {
            console.warn(chalk.yellow(`⚠️ Skipping insert for ${schema}.${table} as values array is empty`));
            continue;
        }

        if (values.length !== columns.length * batch.length) {
            console.error(chalk.red(`❌ Error inserting data into ${schema}.${table}: values length does not match expected length`));
            console.error(chalk.red(`❗ Failing Query:\n${query}`));
            console.error(chalk.red(`❗ values length: ${values.length}, expected length: ${columns.length * batch.length}`));
            continue;
        }

        try {
            const result = await pgClient.query(query, values);
            totalInserted += result.rowCount;
        } catch (error) {
            console.error(chalk.red(`❌ Error inserting batch into ${schema}.${table}: ${error.message}`));
            console.error(chalk.red(`❗ Failing Query:\n${query}`));
            throw error;
        }
    }

    return totalInserted;
}

/**
 * Create a view in PostgreSQL.
 */
export async function createView(pgClient, schema, viewName, viewDefinition) {
    try {
        // Remove "WITH UR" clause
        const postgresViewDefinition = viewDefinition.replace(/WITH UR/i, '');
        const query = `CREATE OR REPLACE VIEW "${schema}"."${viewName}" AS ${postgresViewDefinition}`;

        console.log(chalk.yellow(`\nComposed Query:\n${query}\n`)); // Log the query

        await pgClient.query(query);
        console.log(chalk.green(`✅ View created: ${schema}.${viewName}`));
    } catch (error) {
        console.error(chalk.red(`❌ Failed to create view ${schema}.${viewName}: ${error.message}`));
        console.error(chalk.red(`❗ Failing Query:\n${query}`));
        throw error;
    }
}

/**
 * Get the row count of a table in PostgreSQL.
 */
export async function getPostgresRowCount(pgClient, schema, table) {
    try {
        const result = await pgClient.query(`SELECT COUNT(*) FROM "${schema}"."${table}"`);
        return parseInt(result.rows[0].count, 10);
    } catch (error) {
        console.error(chalk.red(`❌ Failed to get row count for ${schema}.${table}: ${error.message}`));
        return -1;
    }
}

/**
 * Truncate a table in PostgreSQL.
 */
export async function truncateTable(pgClient, schema, table) {
    try {
        await pgClient.query(`TRUNCATE TABLE "${schema}"."${table}"`);
        console.log(chalk.yellow(`✅ Table truncated: ${schema}.${table}`));
    } catch (error) {
        console.error(chalk.red(`❌ Failed to truncate table ${schema}.${table}: ${error.message}`));
        throw error;
    }
}