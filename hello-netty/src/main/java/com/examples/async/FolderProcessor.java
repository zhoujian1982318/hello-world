package com.examples.async;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;
import java.util.concurrent.TimeUnit;

public class FolderProcessor extends RecursiveTask<List<String>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -128863700409424755L;

	private String path;

	private String extension;

	private ForkJoinPool pool;

	public FolderProcessor(String path, String extension, ForkJoinPool pool) {
		super();
		this.path = path;
		this.extension = extension;
		this.pool = pool;
	}

	@Override
	protected List<String> compute() {
		List<String> list = new ArrayList<String>();
		List<FolderProcessor> tasks = new ArrayList<FolderProcessor>();
		File file = new File(path);
		File content[] = file.listFiles();
		if (content != null) {
			for (int i = 0; i < content.length; i++) {
				if (content[i].isDirectory()) {
					FolderProcessor task = new FolderProcessor(content[i].getAbsolutePath(), extension, pool);
					// 异步方式提交任务
					//task.fork();
					tasks.add(task);
				} else {
					if (checkFile(content[i].getName())) {
						list.add(content[i].getAbsolutePath());
					}
				}
			}
			invokeAll(tasks);
			System.out.println("task is blocked");
		}
		if (tasks.size() > 50) {
			System.out.printf("%s: %d tasks ran.\n", file.getAbsolutePath(), tasks.size());
		}

		addResultsFromTasks(list, tasks);
		return list;
	}

	private void addResultsFromTasks(List<String> list, List<FolderProcessor> tasks) {
		
		try {
			TimeUnit.SECONDS.sleep(30);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for (FolderProcessor item : tasks) {
			list.addAll(item.join());
		}
	}

	private boolean checkFile(String name) {
		return name.endsWith(extension);
	}

	public static void main(String[] args) {
		ForkJoinPool pool = new ForkJoinPool(4);
		//FolderProcessor system = new FolderProcessor("C:\\Windows", "log", pool);
		FolderProcessor apps = new FolderProcessor("C:\\Program Files", "log", pool);

		//pool.execute(system);
		pool.execute(apps);
		//pool.shutdown();

		//List<String> results = null;
		// results = system.join();
		// System.out.printf("System: %d files found.\n",results.size());

		// results = apps.join();
		// System.out.printf("Apps: %d files found. \n", results.size());
		// System.out.printf("found the result is %s " , results.toString());
		do {

//			System.out.printf("******************************************\n");
//
//			System.out.printf("Main: Parallelism: %d\n", pool.
//
//					getParallelism());
//
//			System.out.printf("Main: Active Threads: %d\n", pool.
//
//					getActiveThreadCount());
//
//			System.out.printf("Main: Task Count: %d\n", pool.
//
//					getQueuedTaskCount());
//
//			System.out.printf("Main: Steal Count: %d\n", pool.
//
//					getStealCount());
//
//			System.out.printf("******************************************\n");

			try {

				TimeUnit.SECONDS.sleep(1);

			} catch (InterruptedException e) {

				e.printStackTrace();

			}
			
			
		} while (!apps.isDone() );  //||! system.isDone());
		List<String> results = apps.join();
		 System.out.printf("Apps: %d files found. \n", results.size());
		 System.out.printf("found the result is %s " , results.toString());

	}

}
