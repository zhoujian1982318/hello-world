package com.examples.test;

public class SynchQueue<T> {

    private final T[] items;
    /** Main lock guarding all access */

    private Object obj = new Object();

    private int head, tail, count;

    public SynchQueue(){
        this(10);
    }

    public SynchQueue(int maxSize) {

        this.items = (T []) new Object[maxSize];

    }

    public void put(T t) throws InterruptedException {

        synchronized (obj) {
            while (count == getCapacity()) {
                obj.wait();
            }
            boolean isEmpty = count==0 ? true : false;
            items[tail] = t;
            if (++tail == getCapacity()) {
                tail = 0;
            }
            ++count;
            if(isEmpty) {
                obj.notifyAll();
            }
        }
    }

    public T take() throws InterruptedException {

        synchronized (obj) {
            while (count == 0) {
                obj.wait();
            }
            boolean isFull  = count == getCapacity() ? true : false;
            T ret = items[head];
            items[head] = null;//GC
            //
            if (++head == getCapacity()) {
                head = 0;
            }
            --count;
            if(isFull) {
                obj.notifyAll();
            }
            return ret;
        }

    }

    public int getCapacity() {
        return items.length;
    }
}
