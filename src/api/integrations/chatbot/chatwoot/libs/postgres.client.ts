import { Chatwoot, configService } from '@config/env.config';
import { Logger } from '@config/logger.config';
import postgresql from 'pg';

const { Pool } = postgresql;

class Postgres {
  private logger = new Logger('Postgres');
  private pool;
  private connected = false;

  getConnection(connectionString: string) {
    if (this.connected) {
      return this.pool;
    } else {
      this.pool = new Pool({
        connectionString,
        ssl: {
          rejectUnauthorized: false,
        },
      });

      this.pool.on('error', () => {
        this.logger.error('postgres disconnected');
        this.connected = false;
      });

      try {
        this.connected = true;
      } catch (e) {
        this.connected = false;
        this.logger.error('postgres connect exception caught: ' + e);
        return null;
      }

      return this.pool;
    }
  }

  getChatwootConnection() {
    const databaseConfig = configService.get<Chatwoot>('CHATWOOT').IMPORT.DATABASE;
    const uri = databaseConfig.CONNECTION.URI;

    if (!this.connected) {
      this.pool = new Pool({
        connectionString: uri,
        max: databaseConfig.POOL.MAX,
        idleTimeoutMillis: databaseConfig.POOL.IDLE_TIMEOUT_MS,
        connectionTimeoutMillis: databaseConfig.POOL.CONNECTION_TIMEOUT_MS,
        ssl: {
          rejectUnauthorized: false,
        },
      });

      this.pool.on('error', () => {
        this.logger.error('postgres disconnected');
        this.connected = false;
      });

      this.connected = true;
    }

    return this.pool;
  }
}

export const postgresClient = new Postgres();
