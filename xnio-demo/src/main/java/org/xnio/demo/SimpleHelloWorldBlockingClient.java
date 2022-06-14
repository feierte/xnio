package org.xnio.demo;

import org.xnio.*;
import org.xnio.channels.Channels;
import org.xnio.channels.ConnectedStreamChannel;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;

/**
 * @author Jie Zhao
 * @date 2022/6/15 0:15
 */
public class SimpleHelloWorldBlockingClient {

    public static void main(String[] args) throws Exception {
        final Charset charset = Charset.forName("utf-8");
        //创建Xnio实例，并构造XnioWorker
        final Xnio xnio = Xnio.getInstance();
        final XnioWorker worker = xnio.createWorker(OptionMap.EMPTY);

        try {
            //连接服务器，本地12345端口，注意返回值是IoFuture类型，并不阻塞，返回后可以做些别的事情
            final IoFuture<ConnectedStreamChannel> futureConnection = worker.connectStream(
                    new InetSocketAddress("localhost", 12345), null, OptionMap.EMPTY);
            final ConnectedStreamChannel channel = futureConnection.get(); // get是阻塞调用
            try {
                // 发送消息
                Channels.writeBlocking(channel, ByteBuffer.wrap("Hello world!\n".getBytes(charset)));
                // 保证全部送出
                Channels.flushBlocking(channel);
                // 发送EOF
                channel.shutdownWrites();
                System.out.println("Sent greeting string! The response is...");
                ByteBuffer recvBuf = ByteBuffer.allocate(128);
                // 接收消息
                while (Channels.readBlocking(channel, recvBuf) != -1) {
                    recvBuf.flip();
                    final CharBuffer chars = charset.decode(recvBuf);
                    System.out.print(chars);
                    recvBuf.clear();
                }
            } finally {
                IoUtils.safeClose(channel);
            }
        } finally {
            worker.shutdown();
        }
    }
}
