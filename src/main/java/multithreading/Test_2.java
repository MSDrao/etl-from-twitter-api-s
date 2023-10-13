package multithreading;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

// Fixed thread pool
// Cached thread pool
// Scheduled thread pool
// Single thread executor

/**
 * Main thread ->
 * 1
 * 2
 * 3
 * 4
 * 5
 * 6
 * 7
 * 8
 * 9
 * 10
 */

public class Test_2 {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        System.out.println("Cores : " + Runtime.getRuntime().availableProcessors());
        List<Future> allFutures = new ArrayList<>();
        for (int i=0; i<100; i++) {
            Future<Integer> future = executorService.submit(new Task());
            allFutures.add(future);
        }
        for (int i=0; i<100; i++) {
            Future<Integer> future = allFutures.get(i);
            Integer result = future.get();
            System.out.println("Result of Thread #"+ i + "=" + result);
        }
    }

    public static class Task implements Callable<Integer> {
        @Override
        public Integer call() throws InterruptedException {
            Thread.sleep(3000);
            System.out.println("Thread Name : " + Thread.currentThread().getName());
            return new Random().nextInt();
        }
    }
}