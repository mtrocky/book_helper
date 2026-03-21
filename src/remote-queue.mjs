import { randomUUID } from "node:crypto";
import { info } from "./debug.mjs";

const state = {
  concurrency: 1,
  maxQueued: 20,
  running: 0,
  pending: [],
  active: new Map(),
};

export function configureRemoteQueue({ concurrency, maxQueued } = {}) {
  if (Number.isInteger(concurrency) && concurrency > 0) {
    state.concurrency = concurrency;
  }
  if (Number.isInteger(maxQueued) && maxQueued >= 0) {
    state.maxQueued = maxQueued;
  }
}

export function getRemoteQueueSnapshot() {
  return {
    concurrency: state.concurrency,
    maxQueued: state.maxQueued,
    running: state.running,
    queued: state.pending.length,
    activeTasks: [...state.active.values()].map((task) => summarizeTask(task)),
    pendingTasks: state.pending.slice(0, 10).map((task) => summarizeTask(task)),
  };
}

export function getRemoteTaskState(taskId) {
  if (!taskId) {
    return null;
  }

  const active = state.active.get(taskId);
  if (active) {
    return {
      taskId: active.id,
      kind: active.kind,
      state: "running",
      currentPosition: 1,
      tasksAhead: 0,
      waitSeconds: roundSeconds(Math.max(0, active.startedAt - active.enqueuedAt) / 1000),
      runningSeconds: roundSeconds(Math.max(0, Date.now() - active.startedAt) / 1000),
      meta: active.meta,
    };
  }

  const queuedIndex = state.pending.findIndex((entry) => entry.id === taskId);
  if (queuedIndex >= 0) {
    const task = state.pending[queuedIndex];
    return {
      taskId: task.id,
      kind: task.kind,
      state: "queued",
      currentPosition: state.running + queuedIndex + 1,
      tasksAhead: state.running + queuedIndex,
      waitSeconds: roundSeconds(Math.max(0, Date.now() - task.enqueuedAt) / 1000),
      runningSeconds: 0,
      meta: task.meta,
    };
  }

  return null;
}

export async function enqueueRemoteTask(options, run) {
  const submitted = submitRemoteTask(options, run);
  return submitted.completion;
}

export function submitRemoteTask({ kind, meta = {}, signal, onEvent }, run) {
  const enqueuedAt = Date.now();
  const task = {
    id: randomUUID(),
    kind: String(kind || "remote"),
    meta,
    enqueuedAt,
    initialAhead: state.running + state.pending.length,
    initialPosition: state.running + state.pending.length + 1,
    startedAt: 0,
    settled: false,
    signal,
    abortHandler: null,
    run,
    onEvent,
    resolve: null,
    reject: null,
  };

  if (state.pending.length >= state.maxQueued) {
    throw new Error(`Remote queue is full (${state.maxQueued} queued). Try again later.`);
  }

  const promise = new Promise((resolve, reject) => {
    task.resolve = resolve;
    task.reject = reject;
  });

  if (signal?.aborted) {
    throw new Error("Remote queue wait aborted.");
  }

  if (signal && typeof signal.addEventListener === "function") {
    task.abortHandler = () => {
      if (task.settled || task.startedAt) {
        return;
      }
      const index = state.pending.findIndex((entry) => entry.id === task.id);
      if (index >= 0) {
        state.pending.splice(index, 1);
      }
      task.settled = true;
      task.reject(new Error("Remote queue wait aborted."));
      info("remote queue aborted", {
        taskId: task.id,
        kind: task.kind,
      });
    };
    signal.addEventListener("abort", task.abortHandler, { once: true });
  }

  if (state.running < state.concurrency) {
    startTask(task);
  } else {
    state.pending.push(task);
    task.onEvent?.({
      type: "queued",
      taskId: task.id,
      kind: task.kind,
      initialPosition: task.initialPosition,
      tasksAhead: task.initialAhead,
    });
    info("remote queue enqueued", {
      taskId: task.id,
      kind: task.kind,
      position: task.initialPosition,
      tasksAhead: task.initialAhead,
      queued: state.pending.length,
      running: state.running,
      meta: task.meta,
    });
  }

  return {
    taskId: task.id,
    initialPosition: task.initialPosition,
    tasksAheadAtEnqueue: task.initialAhead,
    completion: promise,
  };
}

function maybeStartNext() {
  while (state.running < state.concurrency && state.pending.length > 0) {
    const next = state.pending.shift();
    if (!next || next.settled) {
      continue;
    }
    startTask(next);
  }
}

async function startTask(task) {
  task.startedAt = Date.now();
  state.running += 1;
  state.active.set(task.id, task);
  const waitMs = Math.max(0, task.startedAt - task.enqueuedAt);
  task.onEvent?.({
    type: "started",
    taskId: task.id,
    kind: task.kind,
    initialPosition: task.initialPosition,
    tasksAhead: task.initialAhead,
    waitSeconds: roundSeconds(waitMs / 1000),
  });
  info("remote queue started", {
    taskId: task.id,
    kind: task.kind,
    position: task.initialPosition,
    tasksAhead: task.initialAhead,
    waitMs,
    running: state.running,
    queued: state.pending.length,
    meta: task.meta,
  });

  try {
    const value = await task.run();
    task.settled = true;
    task.resolve({
      value,
      queueStatus: {
        kind: task.kind,
        taskId: task.id,
        initialPosition: task.initialPosition,
        tasksAheadAtEnqueue: task.initialAhead,
        waitSeconds: roundSeconds(waitMs / 1000),
        serviceSeconds: roundSeconds((Date.now() - task.startedAt) / 1000),
      },
    });
  } catch (error) {
    task.settled = true;
    task.reject(error);
  } finally {
    if (task.abortHandler) {
      try {
        task.signal?.removeEventListener?.("abort", task.abortHandler);
      } catch {}
    }
    state.active.delete(task.id);
    state.running = Math.max(0, state.running - 1);
    task.onEvent?.({
      type: "finished",
      taskId: task.id,
      kind: task.kind,
    });
    info("remote queue finished", {
      taskId: task.id,
      kind: task.kind,
      running: state.running,
      queued: state.pending.length,
      meta: task.meta,
    });
    maybeStartNext();
  }
}

function summarizeTask(task) {
  return {
    id: task.id,
    kind: task.kind,
    meta: task.meta,
    initialPosition: task.initialPosition,
    enqueuedAt: new Date(task.enqueuedAt).toISOString(),
    startedAt: task.startedAt ? new Date(task.startedAt).toISOString() : "",
  };
}

function roundSeconds(value) {
  return Math.round(value * 100) / 100;
}
