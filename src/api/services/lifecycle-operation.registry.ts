import { createId as cuid } from '@paralleldrive/cuid2';

export type LifecycleOperationSnapshot = {
  operationId: string;
  startedAt: string;
};

export type LifecycleOperationAcceptance = LifecycleOperationSnapshot & {
  accepted: true;
  deduplicated: boolean;
  inFlight: boolean;
};

type LifecycleOperationEntry = LifecycleOperationSnapshot & {
  completion: Promise<void>;
  active: boolean;
  expiresAt: number;
  retentionMs: number;
};

export class LifecycleOperationRegistry {
  private readonly operations = new Map<string, LifecycleOperationEntry>();

  public start(
    key: string,
    operation: (operationId: string) => Promise<unknown>,
    options: {
      operationId?: string;
      onFailure?: (error: unknown, operationId: string) => void | Promise<void>;
      retentionMs?: number;
    } = {},
  ): LifecycleOperationAcceptance {
    const requestedOperationId = options.operationId?.trim();
    const existing = this.currentEntry(key);
    if (existing && (existing.active || (requestedOperationId && existing.operationId === requestedOperationId))) {
      return {
        accepted: true,
        deduplicated: true,
        inFlight: existing.active,
        operationId: existing.operationId,
        startedAt: existing.startedAt,
      };
    }
    if (existing) {
      this.operations.delete(key);
    }

    const operationId = requestedOperationId || cuid();
    const startedAt = new Date().toISOString();
    const retentionMs = options.retentionMs ?? 10 * 60 * 1000;
    const execution = Promise.resolve().then(() => operation(operationId));
    const completion = execution.then(
      () => {
        const current = this.operations.get(key);
        if (current?.operationId === operationId) {
          current.active = false;
          current.expiresAt = Date.now() + current.retentionMs;
          this.scheduleRetainedDeletion(key, operationId, current.retentionMs);
        }
      },
      async (error) => {
        await Promise.resolve(options.onFailure?.(error, operationId)).catch(() => undefined);
        if (this.operations.get(key)?.operationId === operationId) {
          this.operations.delete(key);
        }
      },
    );

    this.operations.set(key, {
      operationId,
      startedAt,
      completion,
      active: true,
      expiresAt: Number.POSITIVE_INFINITY,
      retentionMs,
    });

    return {
      accepted: true,
      deduplicated: false,
      inFlight: true,
      operationId,
      startedAt,
    };
  }

  public get(key: string): LifecycleOperationSnapshot | null {
    const operation = this.currentEntry(key);
    if (!operation?.active) {
      return null;
    }

    return {
      operationId: operation.operationId,
      startedAt: operation.startedAt,
    };
  }

  private currentEntry(key: string): LifecycleOperationEntry | null {
    const operation = this.operations.get(key);
    if (!operation) {
      return null;
    }

    if (!operation.active && operation.expiresAt <= Date.now()) {
      this.operations.delete(key);
      return null;
    }

    return operation;
  }

  private scheduleRetainedDeletion(key: string, operationId: string, retentionMs: number) {
    const expirationTimer = setTimeout(() => {
      const current = this.operations.get(key);
      if (current?.operationId === operationId && !current.active) {
        this.operations.delete(key);
      }
    }, retentionMs);
    expirationTimer.unref?.();
  }
}
