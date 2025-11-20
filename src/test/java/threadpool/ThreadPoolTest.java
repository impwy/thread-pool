package threadpool;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

class ThreadPoolTest {

    @Test
    void submittedTasksAreExecuted() throws Exception {
        final ThreadPool executor = ThreadPool.builder(6)
                                              .minNumWorkers(3)
                                              .idleTimeout(1, TimeUnit.NANOSECONDS)
                                              .build();
        final int numTasks = 10000;
        final CountDownLatch latch = new CountDownLatch(numTasks);
        try {
            for (int i = 0; i < numTasks; i++) {
                final int finalI = i;
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        System.err.println(
                                "Thread '" + Thread.currentThread().getName() + "' executes task " + finalI);
//                    try {
//                        Thread.sleep(10);
//                    } catch (InterruptedException e) {
//                    }
                        latch.countDown();
                    }

                    @Override
                    public String toString() {
                        return "Task " + finalI;
                    }
                });
            }
            latch.await();
            Thread.sleep(1000);
        } finally {
            executor.shutdown();
        }
    }

    @Test
    void customTaskSubmissionHandler() throws InterruptedException {
        final Runnable taskToReject = () -> {};
        final ThreadPool pool
                = ThreadPool.builder(1)
                            .submissionHandler(new TaskSubmissionHandler() {
                                @Override
                                public TaskAction handleSubmission(Runnable task,
                                                                   int numPendingTasks) {
                                    return task == taskToReject ? TaskAction.reject()
                                                                : TaskAction.accept();
                                }

                                @Override
                                public TaskAction handleSubmission(Callable<?> task,
                                                                   int numPendingTasks) {
                                    throw new UnsupportedOperationException();
                                }

                                @Override
                                public TaskAction handleLateSubmission(Runnable task) {
                                    return TaskAction.reject();
                                }

                                @Override
                                public TaskAction handleLateSubmission(Callable<?> task) {
                                    return TaskAction.reject();
                                }
                            }).build();

        final CountDownLatch latch = new CountDownLatch(1);
        pool.execute(latch::countDown);
        latch.await();

//        pool.execute(taskToReject);

        pool.shutdown();
        pool.execute(() -> {});
    }

}
