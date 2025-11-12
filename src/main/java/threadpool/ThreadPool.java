package threadpool;

import java.time.Duration;
import java.util.HashSet;
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

public class ThreadPool implements Executor {

    private static final Thread[] EMPTY_THREADS_ARRAY = new Thread[0];
    public static final Runnable SHUTDOWN_TASK = () -> {};

    private final long idleTimeoutNanos;
    private final int maxNumThreads;
    private final BlockingQueue<Runnable> queue = new LinkedTransferQueue<>();
    private final AtomicBoolean shutdown = new AtomicBoolean();
    private final Set<Thread> threads = new HashSet<>();
    private final AtomicInteger numThreads = new AtomicInteger();
    private final AtomicInteger numBusyThreads = new AtomicInteger();
    private final Lock threadsLock = new ReentrantLock();

    public ThreadPool(int maxNumThreads, Duration idleTimeout) {
        this.maxNumThreads = maxNumThreads;
        idleTimeoutNanos = idleTimeout.toNanos();
    }

    @Override
    public void execute(Runnable command) {
        if (shutdown.get()) {
            throw new RejectedExecutionException();
        }
        queue.add(command);
        addThreadIfNecessary();
        if (shutdown.get()) {
            queue.remove(command);
            throw new RejectedExecutionException();
        }
    }

    private void addThreadIfNecessary() {
        if (needsMoreThreads()) {
            threadsLock.lock();
            Thread newThread = null;
            try {
                // Note that we check if the pool is shut down only *after* acquiring the lock,
                // because:
                // - shutting down a pool doesn't occur very often; and
                // - it's now worth checking whether the pool is shut down or not frequently.
                if (needsMoreThreads() && !shutdown.get()) {
                    newThread = newThread();
                }
            } finally {
                threadsLock.unlock();
            }

            if (newThread != null) {
                newThread.start();
            }
        }

    }

    private boolean needsMoreThreads() {
        final int numBusyThreads = this.numBusyThreads.get();
        final int numThreads = this.numThreads.get();
        return numBusyThreads >= numThreads && numBusyThreads < maxNumThreads;
    }

    private Thread newThread() {
        numThreads.incrementAndGet();
        numBusyThreads.incrementAndGet();
        final Thread thread = new Thread(() -> {
            System.err.println("Started a new thread: " + Thread.currentThread().getName());
            boolean isBusy = true;
            long lastRunTimeNanos = System.nanoTime();
            try {
                for (; ; ) {
                    try {
                        Runnable task = queue.poll();
                        if (task == null) {
                            if (isBusy) {
                                isBusy = false;
                                numBusyThreads.decrementAndGet();
                                System.err.println(Thread.currentThread().getName() + " idle");
                            }
                            final long waitTimeNanos = idleTimeoutNanos - (System.nanoTime() - lastRunTimeNanos);
                            if (waitTimeNanos <= 0 ||
                                (task = queue.poll(waitTimeNanos, TimeUnit.NANOSECONDS)) == null) {
                                break;
                            }
                            isBusy = true;
                            numBusyThreads.incrementAndGet();
                            System.err.println(Thread.currentThread().getName() + " busy");
                        } else {
                            if (!isBusy) {
                                isBusy = true;
                                numBusyThreads.incrementAndGet();
                            }
                        }

                        if (task == SHUTDOWN_TASK) {
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
                threadsLock.lock();
                try {
                    threads.remove(Thread.currentThread());
                    numThreads.decrementAndGet();
                    if (isBusy) {
                        numBusyThreads.decrementAndGet();
                        System.err.println(Thread.currentThread().getName() + " idle");
                    }

                    if (threads.isEmpty() && !queue.isEmpty()) {
                        for (Runnable task : queue) {
                            if (task != SHUTDOWN_TASK) {
                                // We found the situation where:
                                // - there are no threads available and
                                // - there is a task in the queue.
                                // Start a new thread so that it's picked up.
                                addThreadIfNecessary();
                                break;
                            }
                        }
                    }
                } finally {
                    threadsLock.unlock();
                }
                System.err.println("Shutting thread '" + Thread.currentThread().getName() + '\'');
            }
        });
        threads.add(thread);
        return thread;
    }


    public void shutdown() {
        if (shutdown.compareAndSet(false, true)) {
            for (int i = 0; i < maxNumThreads; i++) {
                queue.add(SHUTDOWN_TASK);
            }
        }

        for (; ; ) {
            final Thread[] threads;
            threadsLock.lock();
            try {
                threads = this.threads.toArray(EMPTY_THREADS_ARRAY);
            } finally {
                threadsLock.unlock();
            }

            if (threads.length == 0) {
                break;
            }

            for (Thread thread : threads) {
                do {
                    try {
                        thread.join();
                    } catch (InterruptedException e) {
                        // Do not propagate to prevent incomplete shutdown
                    }
                } while (thread.isAlive());
            }
        }
    }
}
