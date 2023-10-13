package multithreading;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

// Fixed thread pool
// Cached thread pool
// Scheduled thread pool
// Single thread executor

public class Test {
    public static void main(String[] args) {
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        System.out.println("Cores : " + Runtime.getRuntime().availableProcessors());
        for (int i=0; i<=10; i++) executorService.execute(new Task());
    }

    public static class Task implements Runnable {
        @Override
        public void run() {
            System.out.println("Thread Name : " + Thread.currentThread().getName());
        }
    }
}
