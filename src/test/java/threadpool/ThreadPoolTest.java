package threadpool;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;

import org.junit.jupiter.api.Test;

class ThreadPoolTest {

    @Test
    void submittedTasksAreExecuted() throws Exception {
        final ThreadPool executor = new ThreadPool(1, Duration.ofNanos(1));
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
            System.err.println("-----------------");
            Thread.sleep(1000);
        } finally {
            executor.shutdown();
        }
    }
}
