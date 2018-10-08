package com.examples.buffer;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class MappedByteBufferTest {

    public static void main(String[] args) throws IOException {

        RandomAccessFile file = new RandomAccessFile("C:\\Users\\Administrator\\git\\hello-world\\hello-netty\\src\\main\\resources\\mappedBuffer.txt", "rw");

        System.out.println("the length of file is " + file.length());

        FileChannel fileChannel = file.getChannel();

        MappedByteBuffer mappedByteBuffer  = fileChannel.map(FileChannel.MapMode.READ_WRITE,0, 10);


        System.out.println("the mapped buffer position is " + mappedByteBuffer.position());
        System.out.println("the mapped buffer cap is " + mappedByteBuffer.capacity());
        System.out.println("the mapped buffer limit is " + mappedByteBuffer.limit());

        byte [] bts = new byte [10];
        mappedByteBuffer.get(bts);
        System.out.println("get the file is :" + new String (bts));

        System.out.println("the mapped buffer position is " + mappedByteBuffer.position());
        System.out.println("the mapped buffer cap is " + mappedByteBuffer.capacity());
        System.out.println("the mapped buffer limit is " + mappedByteBuffer.limit());

        mappedByteBuffer.flip();

        System.out.println("the mapped buffer position is " + mappedByteBuffer.position());
        System.out.println("the mapped buffer cap is " + mappedByteBuffer.capacity());
        System.out.println("the mapped buffer limit is " + mappedByteBuffer.limit());
        //mappedByteBuffer.rewind();


        mappedByteBuffer.put("write test".getBytes());


        fileChannel.close();
        file.close();

    }
}
