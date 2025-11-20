package threadpool;

import static com.google.common.base.Preconditions.checkState;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.Nullable;

public final class ThreadPool implements Executor {

    public static ThreadPool of(int maxNumWorkers) {
        return builder(maxNumWorkers).build();
    }

    public static ThreadPool of(int minNumWorkers, int maxNumWorkers) {
        return builder(maxNumWorkers).minNumWorkers(minNumWorkers).build();
    }

    public static ThreadPoolBuilder builder(int maxNumWorkers) {
        return new ThreadPoolBuilder(maxNumWorkers);
    }

    private static final Worker[] EMPTY_WORKERS_ARRAY = new Worker[0];
    public static final Runnable SHUTDOWN_TASK = () -> {};

    private final int minNumWorkers;
    private final int maxNumWorkers;
    private final long idleTimeoutNanos;
    private final BlockingQueue<Runnable> queue;
    private final TaskSubmissionHandler taskSubmissionHandler;
    private final AtomicBoolean shutdown = new AtomicBoolean();
    private final Set<Worker> workers = new HashSet<>();
    private final Lock workersLock = new ReentrantLock();
    private final AtomicInteger numWorkers = new AtomicInteger();
    private final AtomicInteger numBusyWorkers = new AtomicInteger();

    ThreadPool(int minNumWorkers, int maxNumWorkers, long idleTimeoutNanos,
               BlockingQueue<Runnable> queue, TaskSubmissionHandler taskSubmissionHandler) {
        this.minNumWorkers = minNumWorkers;
        this.maxNumWorkers = maxNumWorkers;
        this.idleTimeoutNanos = idleTimeoutNanos;
        this.queue = queue;
        this.taskSubmissionHandler = taskSubmissionHandler;
    }

    @Override
    public void execute(Runnable task) {
        if (!handleLateSubmission(task)) {
            return;
        }

        if (!handleSubmission(task)) {
            return;
        }

        addWorkersIfNecessary();
        if (shutdown.get()) {
            //noinspection ResultOfMethodCallIgnored
            queue.remove(task);
            final boolean accepted = handleLateSubmission(task);
            assert !accepted;
        }
    }

    private boolean handleSubmission(Runnable task) {
        final TaskAction action = taskSubmissionHandler.handleSubmission(task, queue.size());
        if (action == TaskAction.accept()) {
            queue.add(task);
            return true;
        }

        action.doAction(task);
        return false;
    }

    private boolean handleLateSubmission(Runnable task) {
        if (!shutdown.get()) {
            return true;
        }

        final TaskAction action = taskSubmissionHandler.handleLateSubmission(task);
        checkState(action != TaskAction.accept(),
                   "TaskSubmissionHandler.handleLateSubmission() should never accept a task.");
        action.doAction(task);
        return false;
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
                    final ExpirationMode expirationMode = needsMoreWorker();
                    if (expirationMode != null) {
                        if (newWorkers == null) {
                            newWorkers = new ArrayList<>();
                        }
                        newWorkers.add(newWorker(expirationMode));
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
     * Returns the {@link ExpirationMode} of the worker if more worker is needed to handle a newly submitted
     * task. {@code null} is returned if no new worker is needed.
     */
    @Nullable
    private ExpirationMode needsMoreWorker() {
        final int numBusyWorkers = this.numBusyWorkers.get();
        final int numWorkers = this.numWorkers.get();

        if (numWorkers < minNumWorkers) {
            return ExpirationMode.NEVER;
        }

        if (numBusyWorkers >= numWorkers) {
            if (numBusyWorkers < maxNumWorkers) {
                return idleTimeoutNanos > 0 ? ExpirationMode.ON_IDLE : ExpirationMode.NEVER;
            }
        }

        return null;
    }

    private Worker newWorker(ExpirationMode expirationMode) {
        numWorkers.incrementAndGet();
        numBusyWorkers.incrementAndGet();
        final Worker worker = new Worker(expirationMode);
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

    private enum ExpirationMode {
        /**
         * The worker that never gets terminated.
         * It's only terminated by {@link #SHUTDOWN_TASK}, which is submitted when the pool is shut down.
         */
        NEVER,
        /**
         * The worker that can be terminated due to idle timeout.
         */
        ON_IDLE
    }

    private class Worker {
        private final ExpirationMode expirationMode;
        private final Thread thread;

        Worker(ExpirationMode expirationMode) {
            this.expirationMode = expirationMode;
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

                            switch (expirationMode) {
                                case NEVER:
                                    task = queue.take();
                                    break;
                                case ON_IDLE:
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

                System.err.println("Shutting down thread '" + Thread.currentThread().getName() + "' (" + expirationMode + ')');
            }
        }
    }
}
