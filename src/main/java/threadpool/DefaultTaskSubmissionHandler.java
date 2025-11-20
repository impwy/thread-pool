package threadpool;

import java.util.concurrent.Callable;

enum DefaultTaskSubmissionHandler implements TaskSubmissionHandler {
    INSTANCE;

    @Override
    public TaskAction handleSubmission(Runnable task, int numPendingTasks) {
        return TaskAction.accept();
    }

    @Override
    public TaskAction handleSubmission(Callable<?> task, int numPendingTasks) {
        return TaskAction.accept();
    }

    @Override
    public TaskAction handleLateSubmission(Runnable task) {
        return TaskAction.reject();
    }

    @Override
    public TaskAction handleLateSubmission(Callable<?> task) {
        return TaskAction.reject();
    }
}
