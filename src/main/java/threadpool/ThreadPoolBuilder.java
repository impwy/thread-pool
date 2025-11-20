package threadpool;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

public final class ThreadPoolBuilder {

    private final int maxNumWorkers;
    private int minNumWorkers;
    private long idleTimeoutNanos;
    @Nullable
    private BlockingQueue<Runnable> queue;
    private TaskSubmissionHandler submissionHandler = TaskSubmissionHandler.ofDefault();

    ThreadPoolBuilder(int maxNumWorkers) {
        checkArgument(maxNumWorkers > 0,
                      "maxNumWorkers: %s (expected: > 0)", maxNumWorkers);
        this.maxNumWorkers = maxNumWorkers;
    }

    public ThreadPoolBuilder minNumWorkers(int minNumWorkers) {
        checkArgument(minNumWorkers >= 0 && minNumWorkers <= maxNumWorkers,
                      "minNumWorkers: %s (expected: [0, maxNumWorkers (%s)]",
                      minNumWorkers, maxNumWorkers);
        this.minNumWorkers = minNumWorkers;
        return this;
    }

    public ThreadPoolBuilder idleTimeout(long idleTimeout, TimeUnit unit) {
        checkArgument(idleTimeout >= 0, "idleTimeout: %s (expected: >= 0");
        idleTimeoutNanos = requireNonNull(unit, "unit").toNanos(idleTimeout);
        return this;
    }

    public ThreadPoolBuilder idleTimeout(Duration idleTimeout) {
        requireNonNull(idleTimeout, "idleTimeout");
        return idleTimeout(idleTimeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    public ThreadPoolBuilder queue(BlockingQueue<Runnable> queue) {
        this.queue = requireNonNull(queue, "queue");
        return this;
    }

    public ThreadPoolBuilder submissionHandler(TaskSubmissionHandler submissionHandler) {
        this.submissionHandler = requireNonNull(submissionHandler, "submissionHandler");
        return this;
    }

    public ThreadPool build() {
        final BlockingQueue<Runnable> queue = this.queue != null ? this.queue
                                                                 : new LinkedBlockingQueue<>();
        return new ThreadPool(minNumWorkers, maxNumWorkers, idleTimeoutNanos, queue, submissionHandler);
    }
}
