import { describe, it, expect } from 'vitest';
import { MemoryQueue } from './memory-queue.js';

describe('MemoryQueue', () => {
  it('processes jobs', async () => {
    const queue = new MemoryQueue();
    const calls: string[] = [];

    queue.process('test', async (job) => {
      calls.push(job.data.task as string);
    });

    await queue.add('test', { task: 'hello' });
    await new Promise((r) => setTimeout(r, 50));

    expect(calls).toEqual(['hello']);
  });

  it('processes multiple jobs in order', async () => {
    const queue = new MemoryQueue();
    const calls: number[] = [];

    queue.process('test', async (job) => {
      calls.push(job.data.n as number);
    });

    await queue.add('test', { n: 1 });
    await queue.add('test', { n: 2 });
    await queue.add('test', { n: 3 });
    await new Promise((r) => setTimeout(r, 100));

    expect(calls).toEqual([1, 2, 3]);
  });

  it('retries failed jobs', async () => {
    const queue = new MemoryQueue();
    let attempts = 0;

    queue.process('test', async () => {
      attempts++;
      if (attempts < 3) throw new Error('fail');
    });

    await queue.add('test', {}, { attempts: 3 });
    await new Promise((r) => setTimeout(r, 100));

    expect(attempts).toBe(3);
  });

  it('marks job as failed after max attempts', async () => {
    const queue = new MemoryQueue();

    queue.process('test', async () => {
      throw new Error('always fails');
    });

    const job = await queue.add('test', {}, { attempts: 2 });
    await new Promise((r) => setTimeout(r, 100));

    const failed = await queue.getFailed();
    expect(failed.length).toBeGreaterThanOrEqual(1);
    expect(failed[0]!.error).toBe('always fails');
  });

  it('respects concurrency limit', async () => {
    const queue = new MemoryQueue({ concurrency: 2 });
    let running = 0;
    let maxRunning = 0;

    queue.process('test', async () => {
      running++;
      maxRunning = Math.max(maxRunning, running);
      await new Promise((r) => setTimeout(r, 20));
      running--;
    });

    for (let i = 0; i < 5; i++) {
      await queue.add('test', { i });
    }
    await new Promise((r) => setTimeout(r, 200));

    expect(maxRunning).toBeLessThanOrEqual(2);
  });

  it('returns job by id', async () => {
    const queue = new MemoryQueue();
    queue.process('test', async () => {});

    const job = await queue.add('test', { x: 1 });
    const found = await queue.getJob(job.id);
    expect(found).toBeDefined();
    expect(found!.id).toBe(job.id);
  });

  it('removes jobs', async () => {
    const queue = new MemoryQueue();
    queue.process('test', async () => {});

    const job = await queue.add('test', {});
    const removed = await queue.removeJob(job.id);
    expect(removed).toBe(true);

    const found = await queue.getJob(job.id);
    expect(found).toBeUndefined();
  });

  it('reports queue stats', async () => {
    const queue = new MemoryQueue();
    queue.process('test', async () => {});
    queue.process('other', async () => {});

    await queue.add('test', { n: 1 });
    await queue.add('other', { n: 2 });
    await new Promise((r) => setTimeout(r, 50));

    const completed = await queue.getCompleted();
    expect(completed.length).toBe(2);
  });
});
