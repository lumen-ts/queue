/**
 * In-memory job queue with retry, delay, and concurrency control.
 *
 * @example
 * ```ts
 * const queue = new MemoryQueue();
 *
 * // Register a handler
 * queue.process('send-email', async (job) => {
 *   await sendEmail(job.data.to, job.data.subject);
 * });
 *
 * // Add a job
 * await queue.add('send-email', { to: 'alice@example.com', subject: 'Hello' });
 *
 * // Add with delay
 * await queue.add('send-email', { to: 'bob@example.com', subject: 'Hi' }, { delay: 5000 });
 *
 * // Add with retry
 * await queue.add('send-email', { to: 'charlie@example.com' }, { attempts: 3 });
 * ```
 */

/** Job data payload. */
export type JobData = Record<string, unknown>;

/** Job status. */
export type JobStatus = 'pending' | 'active' | 'completed' | 'failed' | 'delayed';

/** A job in the queue. */
export interface Job<T extends JobData = JobData> {
  id: string;
  name: string;
  data: T;
  status: JobStatus;
  attempts: number;
  maxAttempts: number;
  createdAt: number;
  processedAt?: number | undefined;
  completedAt?: number | undefined;
  failedAt?: number | undefined;
  error?: string | undefined;
  delay?: number | undefined;
}

/** Job handler function. */
export type JobHandler<T extends JobData = JobData> = (job: Job<T>) => Promise<void>;

/** Job options. */
export interface JobOptions {
  /** Delay in milliseconds before processing. Default 0. */
  delay?: number;
  /** Maximum number of attempts. Default 1. */
  attempts?: number;
  /** Priority (higher = processed first). Default 0. */
  priority?: number;
}

interface QueueEntry {
  job: Job;
  priority: number;
}

/**
 * In-memory job queue with retry, delay, and concurrency control.
 */
export class MemoryQueue {
  private readonly handlers = new Map<string, JobHandler>();
  private readonly waiting: QueueEntry[] = [];
  private readonly active = new Map<string, Job>();
  private readonly completed = new Map<string, Job>();
  private readonly failed = new Map<string, Job>();
  private processing = false;
  private readonly concurrency: number;
  private jobIdCounter = 0;

  constructor(options: { concurrency?: number } = {}) {
    this.concurrency = options.concurrency ?? 1;
  }

  async add<T extends JobData>(name: string, data: T, options: JobOptions = {}): Promise<Job<T>> {
    const job: Job<T> = {
      id: `job_${++this.jobIdCounter}`,
      name,
      data,
      status: options.delay ? 'delayed' : 'pending',
      attempts: 0,
      maxAttempts: options.attempts ?? 1,
      createdAt: Date.now(),
      delay: options.delay,
    };

    this.waiting.push({ job, priority: options.priority ?? 0 });
    this.waiting.sort((a, b) => b.priority - a.priority);

    this.processNext();
    return job;
  }

  process<T extends JobData>(name: string, handler: JobHandler<T>): void {
    this.handlers.set(name, handler as JobHandler);
  }

  async getJob(id: string): Promise<Job | undefined> {
    return this.active.get(id) ?? this.completed.get(id) ?? this.failed.get(id);
  }

  async getWaiting(): Promise<Job[]> {
    return this.waiting.map((e) => e.job);
  }

  async getActive(): Promise<Job[]> {
    return [...this.active.values()];
  }

  async getCompleted(): Promise<Job[]> {
    return [...this.completed.values()];
  }

  async getFailed(): Promise<Job[]> {
    return [...this.failed.values()];
  }

  async removeJob(id: string): Promise<boolean> {
    const idx = this.waiting.findIndex((e) => e.job.id === id);
    if (idx !== -1) {
      this.waiting.splice(idx, 1);
      return true;
    }
    return this.active.delete(id) || this.completed.delete(id) || this.failed.delete(id);
  }

  async close(): Promise<void> {
    this.processing = false;
    this.waiting.length = 0;
    this.active.clear();
  }

  private async processNext(): Promise<void> {
    if (this.processing) return;
    if (this.active.size >= this.concurrency) return;
    if (this.waiting.length === 0) return;

    const entry = this.waiting.shift()!;
    const job = entry.job;

    // Handle delayed jobs
    if (job.delay && job.status === 'delayed') {
      job.status = 'pending';
      setTimeout(() => {
        this.waiting.unshift(entry);
        this.processNext();
      }, job.delay);
      return;
    }

    const handler = this.handlers.get(job.name);
    if (!handler) {
      job.status = 'failed';
      job.error = `No handler registered for "${job.name}"`;
      job.failedAt = Date.now();
      this.failed.set(job.id, job);
      return;
    }

    job.status = 'active';
    job.attempts++;
    job.processedAt = Date.now();
    this.active.set(job.id, job);

    try {
      await handler(job);
      job.status = 'completed';
      job.completedAt = Date.now();
      this.completed.set(job.id, job);
    } catch (error) {
      if (job.attempts < job.maxAttempts) {
        // Retry: put back in waiting
        job.status = 'pending';
        this.waiting.push(entry);
      } else {
        job.status = 'failed';
        job.error = error instanceof Error ? error.message : String(error);
        job.failedAt = Date.now();
        this.failed.set(job.id, job);
      }
    } finally {
      this.active.delete(job.id);
      this.processNext();
    }
  }
}
