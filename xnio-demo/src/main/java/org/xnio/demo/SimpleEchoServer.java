package org.xnio.demo;

import org.xnio.*;
import org.xnio.channels.AcceptingChannel;
import org.xnio.channels.Channels;
import org.xnio.channels.ConnectedStreamChannel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * @author Jie Zhao
 * @date 2022/6/15 0:14
 */
public class SimpleEchoServer {

    public static void main(String[] args) throws Exception {

        // 定义读数据listener
        final ChannelListener<ConnectedStreamChannel> readListener =
                new ChannelListener<ConnectedStreamChannel>() {
                    public void handleEvent(ConnectedStreamChannel channel) {
                        //分配缓冲
                        final ByteBuffer buffer = ByteBuffer.allocate(512);
                        int res;
                        try {
                            while ((res = channel.read(buffer)) > 0) {
                                //切换到写的状态并用阻塞的方式写回
                                buffer.flip();
                                Channels.writeBlocking(channel, buffer);
                            }
                            // 保证全部送出
                            Channels.flushBlocking(channel);
                            if (res == -1) {
                                channel.close();
                            } else {
                                channel.resumeReads();
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                            IoUtils.safeClose(channel);
                        }
                    }
                };
        // 创建接收 listener.
        final ChannelListener<AcceptingChannel<ConnectedStreamChannel>> acceptListener =
                new ChannelListener<AcceptingChannel<ConnectedStreamChannel>>() {
                    public void handleEvent(
                            final AcceptingChannel<ConnectedStreamChannel> channel) {
                        try {
                            ConnectedStreamChannel accepted;
                            // channel就绪，准备接收连接请求
                            while ((accepted = channel.accept()) != null) {
                                System.out.println("accepted " + accepted.getPeerAddress());
                                // 已经连接，设置读数据listener
                                accepted.getReadSetter().set(readListener);
                                // 恢复读的状态
                                accepted.resumeReads();
                            }
                        } catch (IOException ignored) {
                        }
                    }
                };

        //创建Xnio实例，并构造XnioWorker
        final XnioWorker worker = Xnio.getInstance().createWorker(OptionMap.EMPTY);
        // 创建server，在本地12345端口上侦听
        AcceptingChannel<? extends ConnectedStreamChannel> server = worker
                .createStreamServer(new InetSocketAddress(12345),
                        acceptListener, OptionMap.EMPTY);
        // 开始接受连接
        server.resumeAccepts();
        System.out.println("Listening on " + server.getLocalAddress());
    }
}
