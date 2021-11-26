/*
 * JBoss, Home of Professional Open Source
 *
 * Copyright 2020 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xnio.nio.test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URL;
import java.security.GeneralSecurityException;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.xnio.ChannelListener;
import org.xnio.IoFuture;
import org.xnio.OptionMap;
import org.xnio.Xnio;
import org.xnio.XnioWorker;
import org.xnio.channels.AcceptingChannel;
import org.xnio.channels.BoundChannel;
import org.xnio.conduits.ConduitStreamSinkChannel;
import org.xnio.conduits.ConduitStreamSourceChannel;
import org.xnio.ssl.SslConnection;
import org.xnio.ssl.XnioSsl;

/**
 * Test for {@code XnioSsl} connection with a
 * {@link javax.net.ssl.SSLEngineResult.Status#BUFFER_OVERFLOW} result
 * presented at a random time.
 * 
 * @author <a href="mailto:frainone@redhat.com">Flavia Rainone</a>
 *
 */
public class NioSslRandomlyTimedBufferExpansionTcpConnectionTestCase extends AbstractNioSslTcpTest<SslConnection, ConduitStreamSourceChannel, ConduitStreamSinkChannel>{
    private XnioSsl xnioSsl;
    private static final String KEY_STORE_PROPERTY = "javax.net.ssl.keyStore";
    private static final String KEY_STORE_PASSWORD_PROPERTY = "javax.net.ssl.keyStorePassword";
    private static final String TRUST_STORE_PROPERTY = "javax.net.ssl.trustStore";
    private static final String TRUST_STORE_PASSWORD_PROPERTY = "javax.net.ssl.trustStorePassword";
    private static final String DEFAULT_KEY_STORE = "keystore.jks";
    private static final String DEFAULT_KEY_STORE_PASSWORD = "jboss-remoting-test";

    @Before
    public void initXnioSsl() throws GeneralSecurityException {
        xnioSsl = Xnio.getInstance("nio", NioSslTcpChannelTestCase.class.getClassLoader()).getSslProvider(OptionMap.EMPTY);
    }

    @Override
    protected AcceptingChannel<? extends SslConnection> startServer(XnioWorker worker, final ChannelListener<? super SslConnection> serverHandler) throws
            IOException {
        AcceptingChannel<? extends SslConnection> server = super.startServer(worker, serverHandler);
        // server atack
        Thread thread = new Thread(() -> {
            final Socket socket;
            try {
                Thread.sleep((long) (Math.random()*300));
                socket = new Socket("127.0.0.1", SERVER_PORT);
                OutputStream outputStream = socket.getOutputStream();
                outputStream.write(new byte[]{0x16, 0x3, 0x3, 0x71, 0x41}, 0, 5);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }});
        thread.start();

        return server;
    }

    @BeforeClass
    public static void setKeyStoreAndTrustStore() {
        final URL storePath = NioSslTcpChannelTestCase.class.getClassLoader().getResource(DEFAULT_KEY_STORE);
        if (System.getProperty(KEY_STORE_PROPERTY) == null) {
            System.setProperty(KEY_STORE_PROPERTY, storePath.getFile());
        }
        if (System.getProperty(KEY_STORE_PASSWORD_PROPERTY) == null) {
            System.setProperty(KEY_STORE_PASSWORD_PROPERTY, DEFAULT_KEY_STORE_PASSWORD);
        }
        if (System.getProperty(TRUST_STORE_PROPERTY) == null) {
            System.setProperty(TRUST_STORE_PROPERTY, storePath.getFile());
        }
        if (System.getProperty(TRUST_STORE_PASSWORD_PROPERTY) == null) {
            System.setProperty(TRUST_STORE_PASSWORD_PROPERTY, DEFAULT_KEY_STORE_PASSWORD);
        }
    }

    @Override
    protected AcceptingChannel<? extends SslConnection> createServer(XnioWorker worker, InetSocketAddress address,
            ChannelListener<AcceptingChannel<SslConnection>> openListener, OptionMap optionMap) throws IOException {
        return xnioSsl.createSslConnectionServer(worker, address,  openListener,  optionMap);
    }

    @Override
    protected IoFuture<? extends SslConnection> connect(XnioWorker worker, InetSocketAddress address,
            ChannelListener<SslConnection> openListener, ChannelListener<? super BoundChannel> bindListener,
            OptionMap optionMap) {
        return xnioSsl.openSslConnection(worker, address,  openListener, bindListener, optionMap);
    }

    @Override
    protected void setReadListener(SslConnection connection, ChannelListener<ConduitStreamSourceChannel> readListener) {
        connection.getSourceChannel().setReadListener(readListener);
    }

    @Override
    protected void setWriteListener(SslConnection connection, ChannelListener<ConduitStreamSinkChannel> writeListener) {
        connection.getSinkChannel().setWriteListener(writeListener);
    }

    @Override
    protected void resumeReads(SslConnection connection) {
        connection.getSourceChannel().resumeReads();
    }

    @Override
    protected void resumeWrites(SslConnection connection) {
        connection.getSinkChannel().resumeWrites();
    }

    @Override
    protected void shutdownReads(SslConnection connection) throws IOException {
        connection.getSourceChannel().shutdownReads();
    }

    @Override
    protected void shutdownWrites(SslConnection connection) throws IOException {
        connection.getSinkChannel().shutdownWrites();
    }

    @Override @Ignore
    public void serverClose() {}
}
