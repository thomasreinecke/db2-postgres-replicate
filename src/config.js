import 'dotenv/config';

const parseEnvList = (envVar) =>
    (process.env[envVar] || '')
        .split(',')
        .map(entry => entry.trim())
        .filter(entry => entry.length > 0)
        .map(entry => {
            const [schema, table] = entry.split('.');
            if (!schema || !table) {
                console.error(`Invalid format for ${envVar}: ${entry}`);
                return null;
            }
            return { schema, table };
        })
        .filter(entry => entry !== null);

const config = {
    db2: {
        host: process.env.DB2_HOST,
        port: process.env.DB2_PORT,
        dbname: process.env.DB2_DB,
        user: process.env.DB2_USER,
        password: process.env.DB2_PASSWORD,
        sslTrustStore: process.env.DB2_SSL_TRUSTSTORE,
        sslPassword: process.env.DB2_SSL_PASSWORD,
    },
    postgres: {
        host: process.env.POSTGRES_HOST,
        port: process.env.POSTGRES_PORT,
        user: process.env.POSTGRES_USER,
        password: process.env.POSTGRES_PASSWORD,
        database: process.env.POSTGRES_DB,
    },
    replication: {
        tables: parseEnvList("REPLICATED_TABLES"),
        views: parseEnvList("REPLICATED_VIEWS"),
        chunkSize: parseInt(process.env.CHUNK_SIZE, 10) || 1000,
        reset: process.env.RESET === 'true'  // <-- parse RESET as boolean
    }
};

console.log("🔍 Loaded replication tables:", JSON.stringify(config.replication.tables, null, 2));
console.log("🔍 Loaded replication views:", JSON.stringify(config.replication.views, null, 2));
console.log("🔍 RESET =", config.replication.reset);

export default config;
