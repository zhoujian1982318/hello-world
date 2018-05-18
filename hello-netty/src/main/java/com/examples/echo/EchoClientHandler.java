package com.examples.echo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;

public class EchoClientHandler extends ChannelDuplexHandler {
	
	private static Logger LOG = LoggerFactory.getLogger(EchoClientHandler.class);
	
	private final ByteBuf firstMessage;

	public EchoClientHandler() {
		firstMessage = Unpooled.buffer(EchoClient.SIZE);
//		for (int i = 0; i < firstMessage.capacity(); i++) {
//			firstMessage.writeByte((byte) i);
//		}
		firstMessage.writeByte(0x37);
	}

	@Override
	public void channelActive(final ChannelHandlerContext ctx) throws Exception {
		LOG.info("write first  msg to ctx");
//		ctx.write(firstMessage);
//		ctx.flush();
		//如果要把数据发送到远程， 必须调用 ctx.flush();
		ChannelFuture f =ctx.write(firstMessage);
		ctx.flush();
//		.addListener(new ChannelFutureListener() {
//			@Override
//			public void operationComplete(ChannelFuture future) throws Exception {
//				LOG.info("channel flush...");
//				ctx.flush();
//			}
//		});
		
		LOG.info("wait write complete");
		f.get();
		LOG.info("write msg completely");
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		// Close the connection when an exception is raised.
		cause.printStackTrace();
		ctx.close();
	}
    //不会进入这里， 除非调用 ctx.pipeline().writeAndFlush(msg)
	@Override
	public void flush(ChannelHandlerContext ctx) throws Exception {
		LOG.info("flush msg to ctx");
		super.flush(ctx);
	}
	
	

}
