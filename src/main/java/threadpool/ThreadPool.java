package threadpool;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.Nullable;

@SuppressWarnings("ALL")
public class ThreadPool implements Executor {

    private static final Worker[] EMPTY_WORKERS_ARRAY = new Worker[0];
    public static final Runnable SHUTDOWN_TASK = () -> {};

    private final int minNumWorkers;
    private final int maxNumWorkers;
    private final long idleTimeoutNanos;
    private final BlockingQueue<Runnable> queue = new LinkedTransferQueue<>();
    private final AtomicBoolean shutdown = new AtomicBoolean();
    private final Set<Worker> workers = new HashSet<>();
    private final Lock workersLock = new ReentrantLock();
    private final AtomicInteger numWorkers = new AtomicInteger();
    private final AtomicInteger numBusyWorkers = new AtomicInteger();

    public ThreadPool(int minNumWorkers, int maxNumWorkers, Duration idleTimeout) {
        this.minNumWorkers = minNumWorkers;
        this.maxNumWorkers = maxNumWorkers;
        idleTimeoutNanos = idleTimeout.toNanos();
    }

    @Override
    public void execute(Runnable command) {
        if (shutdown.get()) {
            throw new RejectedExecutionException();
        }
        queue.add(command);
        addWorkersIfNecessary();
        if (shutdown.get()) {
            queue.remove(command);
            throw new RejectedExecutionException();
        }
    }

    private void addWorkersIfNecessary() {
        if (needsMoreWorker() != null) {
            workersLock.lock();
            List<Worker> newWorkers = null;
            try {
                // Note that we check if the pool is shut down only *after* acquiring the lock,
                // because:
                // - shutting down a pool doesn't occur very often; and
                // - it's now worth checking whether the pool is shut down or not frequently.
                while (!shutdown.get()) {
                    final WorkerType workerType = needsMoreWorker();
                    if (workerType != null) {
                        if (newWorkers == null) {
                            newWorkers = new ArrayList<>();
                        }
                        newWorkers.add(newWorker(workerType));
                    } else {
                        break;
                    }
                }
            } finally {
                workersLock.unlock();
            }

            if (newWorkers != null) {
                newWorkers.forEach(Worker::start);
            }
        }

    }

    /**
     * Returns the type of the worker if more worker is needed to handle a newly submitted task.
     * {@code null} is returned if no new worker is needed.
     */
    @Nullable
    private WorkerType needsMoreWorker() {
        final int numBusyWorkers = this.numBusyWorkers.get();
        final int numWorkers = this.numWorkers.get();

        if (numWorkers < minNumWorkers) {
            return WorkerType.CORE;
        }

        if (numBusyWorkers >= numWorkers) {
            if (numBusyWorkers < maxNumWorkers) {
                return WorkerType.EXTRA;
            }
        }

        return null;
    }

    private Worker newWorker(WorkerType workerType) {
        numWorkers.incrementAndGet();
        numBusyWorkers.incrementAndGet();
        final Worker worker = new Worker(workerType);
        workers.add(worker);
        return worker;
    }


    public void shutdown() {
        if (shutdown.compareAndSet(false, true)) {
            for (int i = 0; i < maxNumWorkers; i++) {
                queue.add(SHUTDOWN_TASK);
            }
        }

        for (; ; ) {
            final Worker[] workers;
            workersLock.lock();
            try {
                workers = this.workers.toArray(EMPTY_WORKERS_ARRAY);
            } finally {
                workersLock.unlock();
            }

            if (workers.length == 0) {
                break;
            }

            for (Worker worker : workers) {
                worker.join();
            }
        }
    }

    private enum WorkerType {
        CORE,
        EXTRA
    }

    private class Worker {
        private final WorkerType type;
        private final Thread thread;

        Worker(WorkerType type) {
            this.type = type;
            thread = new Thread(this::work);
        }

        void start() {
            thread.start();
        }

        void join() {
            while (thread.isAlive()) {
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    // Do not propagate to prevent incomplete shutdown
                }
            }
        }

        private void work() {
            System.err.println("Started a new thread: " + Thread.currentThread().getName());
            boolean isBusy = true;
            long lastRunTimeNanos = System.nanoTime();
            try {
                loop:
                for (; ; ) {
                    try {
                        Runnable task = queue.poll();
                        if (task == null) {
                            if (isBusy) {
                                isBusy = false;
                                numBusyWorkers.decrementAndGet();
                                System.err.println(Thread.currentThread().getName() + " idle");
                            }

                            switch (type) {
                                case CORE:
                                    task = queue.take();
                                    break;
                                case EXTRA:
                                    final long waitTimeNanos =
                                            idleTimeoutNanos - (System.nanoTime() - lastRunTimeNanos);
                                    if (waitTimeNanos <= 0 ||
                                        (task = queue.poll(waitTimeNanos, TimeUnit.NANOSECONDS)) == null) {
                                        System.err.println(Thread.currentThread().getName() + " hit by idle timeout");
                                        break loop;
                                    }
                                    break;
                                default:
                                    throw new Error();
                            }
                            isBusy = true;
                            numBusyWorkers.incrementAndGet();
                            System.err.println(Thread.currentThread().getName() + " busy");
                        } else {
                            if (!isBusy) {
                                isBusy = true;
                                numBusyWorkers.incrementAndGet();
                                System.err.println(Thread.currentThread().getName() + " busy");
                            }
                        }

                        if (task == SHUTDOWN_TASK) {
                            System.err.println(Thread.currentThread().getName() + " received a poison pill");
                            break;
                        } else {
                            try {
                                task.run();
                            } finally {
                                lastRunTimeNanos = System.nanoTime();
                            }
                        }
                    } catch (Throwable t) {
                        if (!(t instanceof InterruptedException)) {
                            System.err.println("Unexpected exception: ");
                            t.printStackTrace();
                        }
                    }
                }
            } finally {
                workersLock.lock();
                try {
                    workers.remove(this);
                    numWorkers.decrementAndGet();
                    if (isBusy) {
                        numBusyWorkers.decrementAndGet();
                    }

                    if (workers.isEmpty() && !queue.isEmpty()) {
                        for (Runnable task : queue) {
                            if (task != SHUTDOWN_TASK) {
                                // We found the situation where:
                                // - there are no active workers available; and
                                // - there is a task in the queue.
                                // Start a new worker so that it's picked up.
                                addWorkersIfNecessary();
                                break;
                            }
                        }
                    }
                } finally {
                    workersLock.unlock();
                }

                System.err.println("Shutting down thread '" + Thread.currentThread().getName() + "' (" + type + ')');
            }
        }
    }
}
