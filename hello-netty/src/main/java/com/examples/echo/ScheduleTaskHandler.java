package com.examples.echo;


import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class ScheduleTaskHandler extends ChannelInboundHandlerAdapter {

    private static Logger LOG = LoggerFactory.getLogger(ScheduleTaskHandler.class);
    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {

        Runnable r = ()->{
            LOG.info("every 60 seconds write 0 to channel");
            byte[] bts = new byte[] {0x30};
            ByteBuf msg = Unpooled.wrappedBuffer(bts);
            ctx.writeAndFlush(msg);
        };

        ctx.executor().scheduleAtFixedRate(r,0, 60, TimeUnit.SECONDS);
    }
}