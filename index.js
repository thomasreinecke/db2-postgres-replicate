import { replicateAll } from './src/replicate.js';
import config from './src/config.js';
import chalk from 'chalk';

(async () => {
    await replicateAll();
    console.log(chalk.green('All schemas replicated successfully!'));
    process.exit(0);
})();
