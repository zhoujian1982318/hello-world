package com.examples.echo;

import com.examples.task.LongTask;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LongTaskHandler extends ChannelInboundHandlerAdapter {

    private static Logger LOG = LoggerFactory.getLogger(LongTaskHandler.class);

    private ChannelHandlerContext ctx;

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        LOG.info("long task handler, add handler context handler");
        this.ctx = ctx;
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        //不能适用ctx.channel().eventloop(). 要不然还是在 channel 的 eventloop 中运行，会阻塞IO 线程
        Future<String> f =  ctx.executor().submit(new LongTask());
        f.addListener(new FutureListener<String>(){

            @Override
            public void operationComplete(Future<String> future) throws Exception {
                LOG.info("long task has already completed in the thread {}",Thread.currentThread().getName());
                String result = future.get();
                LOG.info("long task return is {}", result);
                // call send Msg
                sendMsg();
            }
        });

        super.channelReadComplete(ctx);

    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }


    public void sendMsg(){
        LOG.info("long task handler.  send Msg  in the thread {}",Thread.currentThread().getName());
        byte []  bytes = new byte []{0x37};
        ByteBuf msg  = Unpooled.wrappedBuffer(bytes);
        ctx.writeAndFlush(msg);

    }
}
