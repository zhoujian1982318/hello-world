package com.examples.test;

import java.util.NoSuchElementException;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ProductQueue<T> {

    private final T[] items;
    /** Main lock guarding all access */
    final ReentrantLock lock;

    /** Condition for waiting takes */
    private final Condition notEmpty;

    /** Condition for waiting puts */
    private final Condition notFull;

    private int head, tail, count;

    public ProductQueue(int maxSize) {

        this.items = (T []) new Object[maxSize];

        lock = new ReentrantLock();

        notEmpty = lock.newCondition();

        notFull = lock.newCondition();
    }

    public ProductQueue(){
        this(10);
    }

    public boolean offer(T t){
        lock.lock();
        try{

            if( count ==  getCapacity()){
                return false;

            }else{
                items[tail] = t;
                if (++tail == getCapacity()) {
                    tail = 0;
                }
                ++count;
                notEmpty.signalAll();
                return true;
            }

        }finally {
            lock.unlock();
        }

    }

    public T poll() {
        lock.lock();
        try {
             if (count == 0) {
                 return null;
             }else{
                 T ret = items[head];
                 items[head] = null;//GC
                 //
                 if (++head == getCapacity()) {
                     head = 0;
                 }
                 --count;
                 notFull.signalAll();
                 return ret;
             }
        } finally {
            lock.unlock();
        }
    }


    public boolean add(T e) {
        if (offer(e))
            return true;
        else
            throw new IllegalStateException("Queue full");
    }

    public T remove(){
        T x = poll();
        if (x != null)
            return x;
        else
            throw new NoSuchElementException();
    }

    public T peek() {

        lock.lock();
        try {
            return items[head]; // null when queue is empty
        } finally {
            lock.unlock();
        }
    }

    public T element(){
        T x = peek();
        if (x != null)
            return x;
        else
            throw new NoSuchElementException();
    }


    public void put(T t) throws InterruptedException {

        lock.lock();

        try {
            while ( count == getCapacity() ){
                notFull.await();
            }
            items[tail] = t;
            if (++tail == getCapacity()) {
                tail = 0;
            }
            ++count;
            notEmpty.signalAll();
        }finally {
            lock.unlock();
        }
    }

    public T take() throws InterruptedException {
        lock.lock();
        try {
            while (count == 0) {
                notEmpty.await();
            }
            T ret = items[head];
            items[head] = null;//GC
            //
            if (++head == getCapacity()) {
                head = 0;
            }
            --count;
            notFull.signalAll();
            return ret;
        } finally {
            lock.unlock();
        }
    }


    public int getCapacity() {
        return items.length;
    }



    public int size() {
        lock.lock();
        try {
            return count;
        } finally {
            lock.unlock();
        }
    }

     class PutQueueTask implements Callable<Boolean> {

        private ProductQueue<String> queue;

        private String data;

        public PutQueueTask(ProductQueue<String> queue,String data){
            this.queue = queue;
            this.data = data;
        }

        @Override
        public Boolean call() throws Exception {
            queue.put(data);
            System.out.println("put the data "+data+" to  the queue");
            return true;
        }
    }

      class TakeQueueTask implements Callable<String> {

        private ProductQueue<String> queue;


        public TakeQueueTask(ProductQueue<String> queue){
            this.queue = queue;
        }

        @Override
        public String call() throws Exception {
            String data = queue.take();
            System.out.println("take the data "+data+" from the queue");
            return data;
        }
    }

    public static void main(String[] args) throws InterruptedException {

        commonQueueOperation();

//        ProductQueue<String> queue = new ProductQueue<>();
//
//        ExecutorService executor = Executors.newCachedThreadPool();
//
//        for(int i=0; i<11; i++){
//            ProductQueue.PutQueueTask t = queue.new PutQueueTask(queue, "test"+i);
//            executor.submit(t);
//        }
//
//        System.out.println("the size of queue is :" + queue.size() );
//
//        TimeUnit.SECONDS.sleep(5);
//
//        ProductQueue.TakeQueueTask t1 = queue.new TakeQueueTask(queue);
//
//        executor.submit(t1);
//
//        executor.shutdown();
//
//        executor.awaitTermination(60, TimeUnit.SECONDS);
//
//        System.out.println("the size of queue is :" + queue.size() );

    }

    private static void commonQueueOperation() {

        ProductQueue<String> queue = new ProductQueue<>(2);

        String tmp = queue.poll();

        System.out.println("poll from   queue . the result is "+ tmp);


//         tmp = queue.remove();

        boolean rest = queue.offer("1");

        System.out.println("put 1 to queue . the result is "+ rest);


        rest = queue.add("2");
        System.out.println("add  2 to queue . the result is "+ rest);

        rest = queue.offer("3");
        System.out.println("offer  3 to queue . the result is "+ rest);

        rest = queue.add("3");
        System.out.println("add  3 to queue . the result is "+ rest);


        System.out.println("the size of queue is :" + queue.size() );



    }


}
