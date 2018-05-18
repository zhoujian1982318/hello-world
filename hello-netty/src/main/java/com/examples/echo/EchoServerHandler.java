package com.examples.echo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.concurrent.TimeUnit;

public class EchoServerHandler extends ChannelInboundHandlerAdapter {
	private static Logger LOG = LoggerFactory.getLogger(EchoServerHandler.class);
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		LOG.info("channel Reade run in the Thread is {}",  Thread.currentThread().getName());
		//long periods task will block IO thread . so it can't put this .
		//this method will call SingleThreadEventExecutor 的 execute 方法
//		ctx.executor().execute(()->{
//			LOG.info("run the long  periods task");
//			LOG.info("long  periods task run int the thread is {}", Thread.currentThread().getName());
//			try {
//				TimeUnit.MINUTES.sleep(5);
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//			LOG.info("the long  task complete");
//		});
		LOG.info("receive the msg from channel {}", msg);
		ctx.write(msg);
	}

	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
	    LOG.info("receive the msg complete {}");
		ctx.flush();
		LOG.info("write flush to ctx");
		super.channelReadComplete(ctx);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		cause.printStackTrace();
		ctx.close();
	}
	
	
	
	
}
