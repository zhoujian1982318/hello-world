package com.examples.buffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;

import java.nio.charset.Charset;

public class ByteBufferTest {

    public static void main(String[] args) {
        //经验表明，ByteBuf的最佳实践是在I/O通信线程的读写缓冲区使用DirectByteBuf，
        //后端业务消息的编解码模块使用HeapByteBuf，这样组合可以达到性能最优。
        Charset utf8 = Charset.forName("UTF-8");
        ByteBuf buf = Unpooled.copiedBuffer("Netty 学习", utf8);
        System.out.println(buf.readableBytes());
        System.out.println(buf.isReadable());
        System.out.println(buf.capacity());

        ByteBuf duplicateBuf = buf.duplicate();
        System.out.println(duplicateBuf.readableBytes());
        System.out.println(duplicateBuf.isReadable());
        System.out.println(duplicateBuf.capacity());

        ByteBuf sliceBuf = buf.slice(0, 5);
        System.out.println("the slice buffer readable Bytes is "+ sliceBuf.readableBytes());
        System.out.println("the slice buffer capacity  is "+ sliceBuf.capacity());
        System.out.printf("the slice buff is %s \n", sliceBuf.toString(utf8));

        ByteBufAllocator alloc = ByteBufAllocator.DEFAULT ;
        System.out.printf("the default ByteBuf allocator is  %s \n", alloc.getClass().getName());

        ByteBuf test =  alloc.heapBuffer(10, 20);

        ByteBufUtil.writeAscii(test, "abcdefghig");
        ByteBufUtil.writeAscii(test, "hijklmnopq");
        //exceed the max capacity
        ByteBufUtil.writeAscii(test, "rst");
        System.out.printf("the  test  hexdump  is  %s \n", ByteBufUtil.hexDump(test));

    }
}

