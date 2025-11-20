package threadpool;

import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;

final class TaskActions {

    static final TaskAction ACCEPT = new TaskAction() {
        @Override
        public void doAction(Runnable task) {}
        @Override
        public void doAction(Callable<?> task) {}
    };

    static final TaskAction REJECT = new TaskAction() {
        @Override
        public void doAction(Runnable task) {
            reject();
        }

        @Override
        public void doAction(Callable<?> task) {
            reject();
        }

        private static void reject() {
            throw new RejectedExecutionException();
        }
    };

    static final TaskAction LOG = new TaskAction() {
        @Override
        public void doAction(Runnable task) {
            log(task);
        }

        @Override
        public void doAction(Callable<?> task) {
            log(task);
        }
    };

    private static void log(Object task) {
        System.out.println("Rejected a task: " + task);
    }

    private TaskActions() {}
}
