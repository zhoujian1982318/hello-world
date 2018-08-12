package com.examples.echo;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.*;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class EchoServer {

	public static void main(String[] args) throws InterruptedException {
		
		EventLoopGroup bossEg = new NioEventLoopGroup(1);
		//EventLoopGroup workEg = new NioEventLoopGroup(1);

		//如果使用 EventExecutorGroup， 可以绑定多个 channel ctx. 每个 channel ctx 会绑定 group中的一个 event executor
		//还需要设置 SINGLE_EVENTEXECUTOR_PER_GROUP 为 false
		EventExecutorGroup longTskGroup = new DefaultEventExecutorGroup(4);

		//如果使用 thread pool 绑定 SingleThreadEventExecutor 是没有意义的,  因为 SingleThreadEventExecutor  submitted task in a single thread.

		//Executor threadPool = Executors.newFixedThreadPool(10);
		//final EventExecutor longTaskExe = new DefaultEventExecutor();
		//如果要并行执行，要用 UnorderedThreadPoolEventExecutor. 但是不能保证顺序
		final EventExecutor longTaskExe = new UnorderedThreadPoolEventExecutor(4);
		try {
			ServerBootstrap b = new ServerBootstrap();
			b.group(bossEg, bossEg);
			b.channel(NioServerSocketChannel.class);
			b.handler(new LoggingHandler(LogLevel.INFO));
			b.childOption(ChannelOption.SINGLE_EVENTEXECUTOR_PER_GROUP, false);
			b.childHandler(new ChannelInitializer<SocketChannel>() {
				@Override
				protected void initChannel(SocketChannel ch) throws Exception {
					ChannelPipeline pipeline = ch.pipeline();
					pipeline.addLast(new LoggingHandler(LogLevel.INFO));
					pipeline.addLast(new EchoServerHandler());
					pipeline.addLast(longTaskExe, new LongTaskHandler());
//					pipeline.addLast(longTskGroup, new LongTaskHandler());
//					pipeline.addLast(longTskGroup, new LongTask2Handler());
					pipeline.addLast(new ScheduleTaskHandler());
				}
			});
			ChannelFuture f = b.bind(8087).sync();
			f.channel().closeFuture().sync();
		} finally {
			bossEg.shutdownGracefully();
			//workEg.shutdownGracefully();
		}

	}
}
