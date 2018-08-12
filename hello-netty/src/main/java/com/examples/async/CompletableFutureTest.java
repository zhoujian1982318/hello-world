package com.examples.async;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class CompletableFutureTest {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		
//		 Consumer<String> cosumer = (s-> {
//			 try {
//				Thread.sleep(5000);
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//			 System.out.println(s+" world");
//		 });
//
//
//
//		 CompletableFuture.supplyAsync(() -> {
//			 System.out.println("supply.....");
//			 try {
//				 Thread.sleep(5000);
//			 } catch (InterruptedException e) {
//				 e.printStackTrace();
//			 }
//		 	return "hello";
//		 }).thenApply((s)->{
//			 System.out.println("apply....");
//			 try {
//				 Thread.sleep(5000);
//			 } catch (InterruptedException e) {
//				 e.printStackTrace();
//			 }
//		 	String tmp = s + "world";
//		 	System.out.println(tmp);
//		 	return  tmp;
//		 });
		 String tmp = "hello init";
		CompletableFuture<String> cF = CompletableFuture.completedFuture(tmp);

		cF.thenRun(()->{
            System.out.printf("run..... \n");
		    try {
                TimeUnit.SECONDS.sleep(5);
            }catch (InterruptedException e){
		        e.printStackTrace();
            }
            System.out.printf("hello world \n");
		});

        String  result = cF.get();
        System.out.printf("the result is %s \n", result);



//		CompletableFuture.supplyAsync(() -> {
//			try {
//				Thread.sleep(2000);
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//			return "hello";
//		}).thenAccept(s -> {
//			System.out.println("xxxxx");
//			try {
//				Thread.sleep(2000);
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//			System.out.println(s+" world");
//		});


//		CompletableFuture.supplyAsync(() -> "hello")
//thenAccept(cosumer);           //.thenAcceptAsync(cosumer);
		 for(int i=0; i<10; i++) {
			 System.out.println("test...");
			 Thread.sleep(1000);
		 }
		
	}

}
