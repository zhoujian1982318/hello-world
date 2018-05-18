package com.examples.async;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.RejectedExecutionHandler;
import io.netty.util.concurrent.SingleThreadEventExecutor;


public class ListenableFuture {
	
	private static Logger LOG = LoggerFactory.getLogger(ListenableFuture.class);
	
	public static void main(String[] args) {
		
		//System.setProperty("io.netty.eventexecutor.maxPendingTasks", "8");
		//每个 EventExecutor 的队列 task queue 最小是 16， 即使你配置了 2 
		//SingleThreadEventExecutor
		//   this.addTaskWakesUp = addTaskWakesUp;
        //this.maxPendingTasks = Math.max(16, maxPendingTasks);
		EventExecutorGroup group = new DefaultEventExecutorGroup(1, null, 2, new RejectedExecutionHandler() {

			@Override
			public void rejected(Runnable task, SingleThreadEventExecutor executor) {
				System.out.println("reject the task "+ task.getClass().getName());
				
			}
			
		});
		
		//System.out.println(SingleThreadEventExecutor.DEFAULT_MAX_PENDING_EXECUTOR_TASKS);
		
		Runnable r  = ()->{
			try {
				TimeUnit.SECONDS.sleep(5);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			System.out.println("ssssss");
		};
		
		Future<String> f =  group.submit(r, "hello,world");
		
		FutureListener<String> listener = new FutureListener<String>() {

			@Override
			public void operationComplete(Future<String> future) throws Exception {
				System.out.println("the thread name is " + Thread.currentThread().getName());
				System.out.println("future  is success " + future.isSuccess());
				System.out.println("the result of  success is  " + future.get());
			}
		};
		f.addListener(listener);
		
		Future<String> f2 =  group.submit(r, "hello,world+2");
		f2.addListener(listener);
		
		Future<String> f3 =  group.submit(r, "hello,world+3");
		
		f3.addListener(listener);
		
		group.submit(r, "hello,world+4");
		group.submit(r, "hello,world+5");
		group.submit(r, "hello,world+6");
		group.submit(r, "hello,world+7");
		group.submit(r, "hello,world+8");
		group.submit(r, "hello,world+9");
		group.submit(r, "hello,world+10");
		group.submit(r, "hello,world+11");
		group.submit(r, "hello,world+12");
		group.submit(r, "hello,world+13");
		group.submit(r, "hello,world+14");
		group.submit(r, "hello,world+15");
		group.submit(r, "hello,world+16");
		group.submit(r, "hello,world+17");
		group.submit(r, "hello,world+18");
		group.submit(r, "hello,world+19");
		group.submit(r, "hello,world+21");
		group.submit(r, "hello,world+20");
		System.out.printf("run main thread, the main thread name is %s the future is Done %b \n", Thread.currentThread().getName(), f.isDone());
		System.out.println("continue run main thread, println .....");
		
		group.shutdownGracefully();
		
	}
}
