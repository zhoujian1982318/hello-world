package com.examples.test;

import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ConcurrentTest {

    public static class  Task implements Callable<Boolean>{
        private static Task instance;
        private Task() {}
        public static synchronized Task getInstance(){
            if(instance ==null){
                instance = new Task();
            }
            return instance;
        }
        private volatile int count = 0;

        private Lock lock = new ReentrantLock();


        public int getCount(){
            return count;
        }

        @Override
        public Boolean call() throws Exception {
            lock.lock();
            try {
                TimeUnit.SECONDS.sleep(1);
            }catch (InterruptedException ex){
               System.out.println("interruppted");
            }
            System.out.println("count++");
            count++;
            lock.unlock();
            return true;
        }
    }

    public static void main(String[] args) {

        ExecutorService executor = Executors.newCachedThreadPool();
        Task task = Task.getInstance();

        for(int i=0; i<100; i++) {
            executor.submit(task);
        }

        executor.shutdown();

        try {
            boolean isTerminate = executor.awaitTermination(30L, TimeUnit.SECONDS);

            System.out.println("isTerminate:" + isTerminate);

            if(!isTerminate){
                executor.shutdownNow();
            }

            while(!executor.isTerminated()){
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }



        int count = task.getCount();

        System.out.println("the count is :" + count);


    }
}
