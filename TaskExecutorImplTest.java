package test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import taskExecution.Main.Task;
import taskExecution.Main.TaskGroup;
import taskExecution.Main.TaskType;
import taskExecution.TaskExecutorImpl;

public class TaskExecutorImplTest {

    @Test
    public void testSubmitTask() throws InterruptedException, ExecutionException {
        TaskExecutorImpl taskExecutor = new TaskExecutorImpl(5);
        Callable<String> task = () -> "result";
        Task<String> stringTask = new Task<>(UUID.randomUUID(), new TaskGroup(UUID.randomUUID()), TaskType.READ, task);
        Future<String> future = taskExecutor.submitTask(stringTask);
        assertEquals("result", future.get());
    }

    @Test
    public void testConcurrentTasks() throws InterruptedException, ExecutionException {
        TaskExecutorImpl taskExecutor = new TaskExecutorImpl(5);
        Task<String> task1 = new Task<>(UUID.randomUUID(), new TaskGroup(UUID.randomUUID()), TaskType.READ, () -> "result1");
        Task<String> task2 = new Task<>(UUID.randomUUID(), new TaskGroup(UUID.randomUUID()), TaskType.READ, () -> "result2");
        Future<String> future1 = taskExecutor.submitTask(task1);
        Future<String> future2 = taskExecutor.submitTask(task2);
        assertEquals("result1", future1.get());
        assertEquals("result2", future2.get());
    }

    @Test
    public void testTaskGroupConcurrency() throws InterruptedException, ExecutionException {
        TaskExecutorImpl taskExecutor = new TaskExecutorImpl(5);
        TaskGroup taskGroup = new TaskGroup(UUID.randomUUID());
        Task<String> task1 = new Task<>(UUID.randomUUID(), taskGroup, TaskType.READ, () -> {
            try {
                TimeUnit.SECONDS.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return "result1";
        });
        Task<String> task2 = new Task<>(UUID.randomUUID(), taskGroup, TaskType.READ, () -> "result2");
        Future<String> future1 = taskExecutor.submitTask(task1);
        Future<String> future2 = taskExecutor.submitTask(task2);
        
        // Assert that task2 started after task1 finished
        assertFalse(future1.isDone()); // task1 is not done as it takes 10 seconds
        assertFalse(future2.isDone()); // task2 is not done as it task1 was submitted first
        TimeUnit.SECONDS.sleep(11); // It takes 10 seconds for the task to complete
        assertTrue(future1.isDone()); 
        assertTrue(future2.isDone());
        assertEquals("result1", future1.get());
        assertEquals("result2", future2.get());
    }
    
    
    @Test
    public void taskExecutionOrderInSameTaskGroup() throws InterruptedException, ExecutionException {
        TaskExecutorImpl taskExecutor = new TaskExecutorImpl(5);
        TaskGroup taskGroup = new TaskGroup(UUID.randomUUID());
        List<String> taskExecutionResult = new ArrayList<String>();
        Task<String> task1 = new Task<>(UUID.randomUUID(), taskGroup, TaskType.WRITE, () -> {
            try {
            	
                TimeUnit.SECONDS.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            taskExecutionResult.add("result1");
            return "result1";
        });
        Task<String> task2 = new Task<>(UUID.randomUUID(), taskGroup, TaskType.WRITE, () -> {
            try {
            	
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            taskExecutionResult.add("result2");
            return "result2";
        });
        Future<String> future1 = taskExecutor.submitTask(task1);
        Future<String> future2 = taskExecutor.submitTask(task2);
        
        // Assert that task2 started after task1 finished
        assertFalse(future1.isDone()); // task1 is not done as it takes 10 seconds
        assertFalse(future2.isDone()); // task2 is not done as it task1 was submitted first
        TimeUnit.SECONDS.sleep(11); // It takes 10 seconds for the task to complete
        assertTrue(future1.isDone()); 
        TimeUnit.SECONDS.sleep(1); // task2 takes 1 seconds to complete
        assertTrue(future2.isDone());
        assertEquals("result1", taskExecutionResult.get(0)); // This should be the first task to complete
        assertEquals("result2", taskExecutionResult.get(1)); // This should be the second task to complete
    }
    
    @Test
    public void taskExecutionOrderInDifferenTaskGroup() throws InterruptedException, ExecutionException {
        TaskExecutorImpl taskExecutor = new TaskExecutorImpl(5);
        //TaskGroup taskGroup = new TaskGroup(UUID.randomUUID());
        List<String> taskExecutionResult = new ArrayList<String>();
        Task<String> task1 = new Task<>(UUID.randomUUID(), new TaskGroup(UUID.randomUUID()), TaskType.WRITE, () -> {
            try {
            	
                TimeUnit.SECONDS.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            taskExecutionResult.add("result1");
            return "result1";
        });
        Task<String> task2 = new Task<>(UUID.randomUUID(), new TaskGroup(UUID.randomUUID()), TaskType.WRITE, () -> {
            try {
            	
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            taskExecutionResult.add("result2");
            return "result2";
        });
        Future<String> future1 = taskExecutor.submitTask(task1);
        Future<String> future2 = taskExecutor.submitTask(task2);
        
        // Assert that task2 started after task1 finished
        assertFalse(future1.isDone()); // task1 is not done as it takes 10 seconds
        assertFalse(future2.isDone()); // task2 is not done as it takes 1 second
        TimeUnit.SECONDS.sleep(2); // task2 takes 1 seconds to complete
        assertTrue(future2.isDone());
        TimeUnit.SECONDS.sleep(11); // It takes 10 seconds for the task to complete
        assertTrue(future1.isDone()); 
        assertEquals("result2", taskExecutionResult.get(0)); // This should be the second task to complete as it takes 1 second
        assertEquals("result1", taskExecutionResult.get(1)); // This should be the first task to complete as it takes 10 seconds
    }

   
}
