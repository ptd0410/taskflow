type Task<T> = (ctx: { signal: AbortSignal }) => Promise<T> | T;

type QueueItem<T = any> = {
  task: Task<T>;
  run: () => Promise<void>;
  cancel: () => void;
  aborted: boolean;
};

type WorkerQueueOptions = {
  concurrency: number;
  onError?: (err: unknown, task: Task<any>) => void;
  onTaskStart?: (task: Task<any>) => void;
  onTaskFinish?: (task: Task<any>) => void;
};

export type Batch = { promise: Promise<any>; abort(): void } | null;

export class WorkerQueue {
  #concurrency: number;
  #running = 0;
  #queue: QueueItem[] = [];
  #paused = false;

  #idleResolvers: Array<() => void> = [];

  #onError?: WorkerQueueOptions["onError"];
  #onTaskStart?: WorkerQueueOptions["onTaskStart"];
  #onTaskFinish?: WorkerQueueOptions["onTaskFinish"];

  constructor(options: WorkerQueueOptions | number) {
    const opts = typeof options === "number" ? { concurrency: options } : options;

    if (opts.concurrency < 1) {
      throw new Error("concurrency must be >= 1");
    }

    this.#concurrency = opts.concurrency;
    this.#onError = opts.onError;
    this.#onTaskStart = opts.onTaskStart;
    this.#onTaskFinish = opts.onTaskFinish;
  }

  get running() {
    return this.#running;
  }

  get pending() {
    return this.#queue.length;
  }

  get size() {
    return this.#running + this.#queue.length;
  }

  get isPaused() {
    return this.#paused;
  }

  pause() {
    this.#paused = true;
  }

  resume() {
    this.#paused = false;
    this.#drain();
  }

  clear() {
    while (this.#queue.length) {
      this.#queue.shift()!.cancel();
    }
  }

  enqueue<T>(task: Task<T>) {
    let resolveFn!: (value: T) => void;
    let rejectFn!: (reason?: any) => void;

    const controller = new AbortController();

    const promise = new Promise<T>((resolve, reject) => {
      resolveFn = resolve;
      rejectFn = reject;
    });

    const item: QueueItem<T> = {
      task,
      aborted: false,

      cancel: () => {
        if (item.aborted) return;

        item.aborted = true;

        controller.abort();

        rejectFn(new Error("aborted"));
      },

      run: async () => {
        if (item.aborted) {
          return;
        }

        this.#running++;
        this.#onTaskStart?.(task);

        try {
          const result = await task({
            signal: controller.signal,
          });

          if (!item.aborted) {
            resolveFn(result);
          }
        } catch (err) {
          this.#onError?.(err, task);

          if (!item.aborted) {
            rejectFn(err);
          }
        } finally {
          this.#onTaskFinish?.(task);

          this.#running--;

          this.#drain();

          if (this.#running === 0 && this.#queue.length === 0) {
            this.#resolveIdle();
          }
        }
      },
    };

    this.#queue.push(item);

    this.#drain();

    return {
      promise,
      cancel: item.cancel,
      abort: item.cancel,
    };
  }

  async onIdle(timeout?: number): Promise<void> {
    if (this.#running === 0 && this.#queue.length === 0) {
      return;
    }

    return new Promise<void>((resolve, reject) => {
      this.#idleResolvers.push(resolve);

      if (timeout) {
        setTimeout(() => {
          const idx = this.#idleResolvers.indexOf(resolve);

          if (idx !== -1) {
            this.#idleResolvers.splice(idx, 1);
            reject(new Error("onIdle timeout"));
          }
        }, timeout);
      }
    });
  }

  all<T>(tasks: Task<T>[]) {
    const jobs = tasks.map((task) => this.enqueue(task));

    return {
      promise: Promise.all(jobs.map((j) => j.promise)),

      abort: () => {
        jobs.forEach((j) => j.abort());
      },
    };
  }

  #drain() {
    if (this.#paused) return;

    while (this.#running < this.#concurrency && this.#queue.length > 0) {
      const item = this.#queue.shift()!;
      item.run();
    }
  }

  #resolveIdle() {
    if (this.#idleResolvers.length === 0) return;

    const resolvers = this.#idleResolvers;
    this.#idleResolvers = [];

    for (const resolve of resolvers) {
      resolve();
    }
  }
}

export function withAbort<T>(fn: (signal: AbortSignal) => Promise<T>) {
  return ({ signal }: { signal: AbortSignal }) =>
    new Promise<T>((resolve, reject) => {
      if (signal.aborted) {
        reject(new Error("aborted"));
        return;
      }

      const onAbort = () => {
        reject(new Error("aborted"));
      };

      signal.addEventListener("abort", onAbort);

      fn(signal)
        .then(resolve)
        .catch(reject)
        .finally(() => {
          signal.removeEventListener("abort", onAbort);
        });
    });
}
