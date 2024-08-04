package taskExecution;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

import taskExecution.Main.Task;
import taskExecution.Main.TaskExecutor;
import taskExecution.Main.TaskGroup;


public class TaskExecutorImpl implements TaskExecutor {

	
    /**
     * Executor Service is used to execute the tasks asynchronously 
     * and return the the future tasks associated with the task
     */
    private final ExecutorService executorService;
    
    
    /**
     * A map of semaphore which stores the task group and its respective semaphore.
     * A semaphore is used to execute the tasks having same task group sequentially
     */
    private final Map<TaskGroup, Semaphore> taskGroupSemaphores;

    /**
     * @param maxConcurrency - Max allowed concurrency for the tasks which DO NOT share the same task group
     */
    public TaskExecutorImpl(int maxConcurrency) {
        this.executorService = Executors.newFixedThreadPool(maxConcurrency);
        this.taskGroupSemaphores = new HashMap<>();
    }

    @Override
    public <T> Future<T> submitTask(Task<T> task) {
    	// Check if the semaphore for the task execution is already present, else if create a new semaphore
        Semaphore semaphore = taskGroupSemaphores.computeIfAbsent(task.taskGroup(), group -> new Semaphore(1));
        return executorService.submit(() -> {
            try {
            	// Waits until it acquires a lock on the semaphore, if other thread has acquired the lock
            	// it wait until the other thread releases the lock
                semaphore.acquire();
                return task.taskAction().call();
            } finally {
                semaphore.release();
            }
        });
    }
}