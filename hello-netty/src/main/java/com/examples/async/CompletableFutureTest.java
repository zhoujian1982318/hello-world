package com.examples.async;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class CompletableFutureTest {

	public static void main(String[] args) throws InterruptedException {
		
		 Consumer<String> cosumer = (s-> {
			 try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			 System.out.println(s+" world");
		 });
		 
		 CompletableFuture.supplyAsync(() -> "hello").thenAcceptAsync(cosumer);
		 
		 for(int i=0; i<10; i++) {
			 System.out.println("test...");
			 Thread.sleep(1000);
		 }
		
	}

}
