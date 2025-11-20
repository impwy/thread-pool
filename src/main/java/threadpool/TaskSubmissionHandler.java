package threadpool;

import java.util.concurrent.Callable;

public interface TaskSubmissionHandler {

    static TaskSubmissionHandler ofDefault() {
        return DefaultTaskSubmissionHandler.INSTANCE;
    }
    TaskAction handleSubmission(Runnable task, int numPendingTasks);
    TaskAction handleSubmission(Callable<?> task, int numPendingTasks);
    TaskAction handleLateSubmission(Runnable task);
    TaskAction handleLateSubmission(Callable<?> task);
}
