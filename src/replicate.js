// src/replicate.js

import {
    connectDB2,
    getColumnMetadata,
    getPrimaryKeys,
    fetchChunk,
    getDb2RowCount,
    getViewDefinition,
} from './db2.js';

import {
    dropSchema,
    ensureSchemaExists,
    ensureTableExists,
    insertIntoPostgres,
    createView, // Rename from createMaterializedView
    getPostgresRowCount,
    truncateTable,
} from './postgres.js';

import { pgClient } from './postgres.js';
import config from './config.js';
import ora from 'ora';
import chalk from 'chalk';

/**
 * Main function to replicate all specified tables and views.
 * Tables first, then views.
 */
export async function replicateAll() {
    console.log(chalk.blue(`\n🔍 Starting replication process...`));

    const db2 = await connectDB2();

    // Drop schemas if RESET is enabled
    if (config.replication.reset) {
        const uniqueSchemas = [...new Set([...config.replication.tables, ...config.replication.views].map((entry) => entry.schema))];
        for (const schema of uniqueSchemas) {
            console.log(chalk.red(`🔥 Dropping schema: ${schema} (RESET enabled)`));
            await dropSchema(pgClient, schema);
        }
    }

    // Ensure schemas exist
    const uniqueSchemas = [...new Set([...config.replication.tables, ...config.replication.views].map((entry) => entry.schema))];
    for (const schema of uniqueSchemas) {
        await ensureSchemaExists(pgClient, schema);
    }

    // Replicate tables
    if (config.replication.tables.length === 0) {
        console.log(chalk.yellow('⚠️ No tables specified in REPLICATED_TABLES. Skipping table replication.'));
    } else {
        console.log(chalk.green(`\n📂 Starting table replication...`));
        for (const { schema, table } of config.replication.tables) {
            console.log(chalk.blue(`🔍 Processing table: ${schema}.${table}`));
            await replicateTable(db2, pgClient, schema, table);
        }
    }

    // Replicate views
    if (config.replication.views.length === 0) {
        console.log(chalk.yellow('⚠️ No views specified in REPLICATED_VIEWS. Skipping view replication.'));
    } else {
        console.log(chalk.green(`\n📂 Starting view replication...`));
        for (const { schema, table: viewName } of config.replication.views) {
            console.log(chalk.blue(`🔍 Processing view: ${schema}.${viewName}`));
            await replicateView(db2, pgClient, schema, viewName);
        }
    }

    console.log(chalk.green(`🎉 Replication process complete!`));
    db2.close();
}

async function replicateTable(db2, pgClient, schema, table) {
    const db2Schema = schema.toUpperCase();
    const db2Table = table.toUpperCase();
    console.log(chalk.yellow(`🛠️  Replicating table: ${schema}.${table}`));

    // Fetch column metadata & primary keys
    const columns = await getColumnMetadata(db2, db2Schema, db2Table);
    const primaryKeys = await getPrimaryKeys(db2, db2Schema, db2Table);

    if (!columns.length) {
        console.log(chalk.red(`⚠️ Skipping ${schema}.${table} (no columns found)`));
        return;
    }

    await ensureTableExists(pgClient, schema, table, columns, primaryKeys);

    const db2Count = await getDb2RowCount(db2, db2Schema, db2Table);
    if (db2Count > 0) {
        console.log(chalk.blue(`🔍 Total rows for ${schema}.${table}: ${db2Count}`));
    } else {
        console.log(chalk.yellow(`⚠️ Could not determine total rows for ${schema}.${table}`));
    }

    if (!config.replication.reset) {
        const postgresCount = await getPostgresRowCount(pgClient, schema, table);
        if (postgresCount === db2Count) {
            console.log(chalk.yellow(`⚠️ Skipping ${schema}.${table} (record counts match)`));
            return;
        } else {
            console.log(chalk.yellow(`⚠️ Record counts differ for ${schema}.${table}. Truncating table.`));
            await truncateTable(pgClient, schema, table);
        }
    }

    let totalFetched = 0;
    const chunkSize = config.replication.chunkSize;
    let offset = 0;
    let hasMoreData = true;
    const spinner = ora(`📥 Fetching data from ${schema}.${table} in chunks...`).start();

    while (hasMoreData) {
        try {
            const chunk = await fetchChunk(db2, db2Schema, db2Table, columns, chunkSize, offset);

            if (!chunk.length) {
                hasMoreData = false;
                break;
            }

            totalFetched += chunk.length;

            const insertedCount = await insertIntoPostgres(pgClient, schema, table, columns, chunk);

            offset += chunkSize;

            if (db2Count > 0) {
                const percent = ((totalFetched / db2Count) * 100).toFixed(2);
                spinner.text = `📥 Fetched & inserted ${totalFetched}/${db2Count} records (${percent}%)`;
            } else {
                spinner.text = `📥 Fetched & inserted ${totalFetched} records (total unknown)`;
            }
        } catch (error) {
            spinner.fail(`⚠️ Error processing ${schema}.${table}: ${error.message}`);
            console.error(error);
            break;
        }
    }

    spinner.succeed(`✅ Finished replicating ${schema}.${table} (${totalFetched} records)`);
}

/**
 * Replicate a view by creating a regular view in PostgreSQL.
 */
async function replicateView(db2, pgClient, schema, viewName) {
    console.log(chalk.blue(`🔄 Creating view: ${schema}.${viewName}`));
    try {
        const viewDefinition = await getViewDefinition(db2, schema.toUpperCase(), viewName.toUpperCase());
        await createView(pgClient, schema, viewName, viewDefinition);
        console.log(chalk.green(`✅ View created: ${schema}.${viewName}`));
    } catch (error) {
        console.error(chalk.red(`❌ Failed to create view for ${schema}.${viewName}: ${error.message}`));
    }
}