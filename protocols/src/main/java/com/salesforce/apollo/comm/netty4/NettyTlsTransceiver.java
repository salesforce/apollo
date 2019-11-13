/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.salesforce.apollo.comm.netty4;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.avro.Protocol;
import org.apache.avro.ipc.CallFuture;
import org.apache.avro.ipc.Callback;
import org.apache.avro.ipc.Transceiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.comm.netty4.NettyTransportCodec.NettyDataPack;
import com.salesforce.apollo.comm.netty4.NettyTransportCodec.NettyFrameDecoder;
import com.salesforce.apollo.comm.netty4.NettyTransportCodec.NettyFrameEncoder;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.compression.FastLzFrameDecoder;
import io.netty.handler.codec.compression.FastLzFrameEncoder;
import io.netty.handler.ssl.SslContext;

/**
 * A Netty-based TLS {@link Transceiver} implementation.
 */
public class NettyTlsTransceiver extends Transceiver {
    /** If not specified, the default connection timeout will be used (60 sec). */
    public static final long    DEFAULT_CONNECTION_TIMEOUT_MILLIS = 2 * 1000L;
    public static final String  NETTY_CONNECT_TIMEOUT_OPTION      = "connectTimeoutMillis";
    public static final String  NETTY_TCP_NODELAY_OPTION          = "tcpNoDelay";
    public static final String  NETTY_KEEPALIVE_OPTION            = "keepAlive";
    public static final boolean DEFAULT_TCP_NODELAY_VALUE         = true;

    private static final Logger log = LoggerFactory.getLogger(NettyTlsTransceiver.class.getName());

    private final AtomicInteger                            serialGenerator = new AtomicInteger(0);
    private final Map<Integer, Callback<List<ByteBuffer>>> requests        = new ConcurrentHashMap<Integer, Callback<List<ByteBuffer>>>();

    private final long              connectTimeoutMillis;
    private final Bootstrap         bootstrap;
    private final InetSocketAddress remoteAddr;
    volatile ChannelFuture          channelFuture;
    volatile boolean                stopping;
    private final Object            channelFutureLock = new Object();

    /**
     * Read lock must be acquired whenever using non-final state. Write lock must be
     * acquired whenever modifying state.
     */
    private final ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();
    private Channel                      channel;                                 // Synchronized on stateLock
    private Protocol                     remote;                                  // Synchronized on stateLock

    NettyTlsTransceiver() {
        connectTimeoutMillis = 0L;
        bootstrap = null;
        remoteAddr = null;
        channelFuture = null;
    }

    /**
     * Creates a NettyTransceiver, and attempts to connect to the given address.
     * {@link #DEFAULT_CONNECTION_TIMEOUT_MILLIS} is used for the connection
     * timeout.
     * 
     * @param addr       the address to connect to.
     * @param eventGroup
     * @throws IOException if an error occurs connecting to the given address.
     */
    public NettyTlsTransceiver(InetSocketAddress addr, SslContext sslContext, EventLoopGroup eventGroup)
            throws IOException {
        this(addr, buildDefaultBootstrapOptions(null), sslContext, eventGroup);
    }

    public NettyTlsTransceiver(InetSocketAddress addr, Map<String, Object> nettyClientBootstrapOptions,
            SslContext sslCtx, EventLoopGroup group) throws IOException {
        this.connectTimeoutMillis = (Long) nettyClientBootstrapOptions.get(NETTY_CONNECT_TIMEOUT_OPTION);

        bootstrap = new Bootstrap();
        bootstrap.group(group)
                 .option(ChannelOption.TCP_NODELAY, true)
                 .option(ChannelOption.SO_KEEPALIVE, true)
                 .option(ChannelOption.TCP_NODELAY, true)
                 .option(ChannelOption.SO_REUSEADDR, true)
                 .option(ChannelOption.SO_KEEPALIVE, true)
                 .option(ChannelOption.SO_LINGER, 0)
                 .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 30_000)
                 .channel(NioSocketChannel.class)
                 .handler(new ChannelInitializer<SocketChannel>() {

                     @Override
                     protected void initChannel(SocketChannel ch) throws Exception {
                         ChannelPipeline pipeline = ch.pipeline()
                                                      .addLast(sslCtx.newHandler(ch.alloc(), addr.getHostName(),
                                                                                 addr.getPort()));
//                         pipeline.addLast(new LoggingHandler("client", LogLevel.INFO));
                         pipeline.addLast(new FastLzFrameDecoder())
                                 .addLast(new FastLzFrameEncoder())
                                 .addLast("frameDecoder", new NettyFrameDecoder())
                                 .addLast("frameEncoder", new NettyFrameEncoder())
                                 .addLast("handler", new NettyClientAvroHandler());
                     }
                 });

        remoteAddr = addr;

        // Make a new connection.
        stateLock.readLock().lock();
        try {
            getChannel();
        } finally {
            stateLock.readLock().unlock();
        }
    }

    /**
     * Creates the default options map for the Netty ClientBootstrap.
     * 
     * @param connectTimeoutMillis connection timeout in milliseconds, or null if no
     *                             timeout is desired.
     * @return the map of Netty bootstrap options.
     */
    protected static Map<String, Object> buildDefaultBootstrapOptions(Long connectTimeoutMillis) {
        Map<String, Object> options = new HashMap<String, Object>(3);
        options.put(NETTY_TCP_NODELAY_OPTION, DEFAULT_TCP_NODELAY_VALUE);
        options.put(NETTY_KEEPALIVE_OPTION, true);
        options.put(NETTY_CONNECT_TIMEOUT_OPTION,
                    connectTimeoutMillis == null ? DEFAULT_CONNECTION_TIMEOUT_MILLIS : connectTimeoutMillis);
        return options;
    }

    /**
     * Tests whether the given channel is ready for writing.
     * 
     * @return true if the channel is open and ready; false otherwise.
     */
    private static boolean isChannelReady(Channel channel) {
        return (channel != null) && channel.isOpen() && channel.isRegistered() && channel.isActive();
    }

    /**
     * Gets the Netty channel. If the channel is not connected, first attempts to
     * connect. NOTE: The stateLock read lock *must* be acquired before calling this
     * method.
     * 
     * @return the Netty channel
     * @throws IOException if an error occurs connecting the channel.
     */
    private Channel getChannel() throws IOException {
        if (!isChannelReady(channel)) {
            // Need to reconnect
            // Upgrade to write lock
            stateLock.readLock().unlock();
            stateLock.writeLock().lock();
            try {
                if (!isChannelReady(channel)) {
                    synchronized (channelFutureLock) {
                        if (!stopping) {
                            log.debug("Connecting to " + remoteAddr);
                            channelFuture = bootstrap.connect(remoteAddr);
                        }
                    }
                    if (channelFuture != null) {
                        try {
                            channelFuture.await(connectTimeoutMillis);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt(); // Reset interrupt flag
                            throw new IOException("Interrupted while connecting to " + remoteAddr);
                        }

                        synchronized (channelFutureLock) {
                            if (!channelFuture.isSuccess()) {
                                throw new IOException("Error connecting to " + remoteAddr, channelFuture.cause());
                            }
                            channel = channelFuture.channel();
                            channelFuture = null;
                        }
                    }
                }
            } finally {
                // Downgrade to read lock:
                stateLock.readLock().lock();
                stateLock.writeLock().unlock();
            }
        }
        return channel;
    }

    /**
     * Closes the connection to the remote peer if connected.
     * 
     * @param awaitCompletion       if true, will block until the close has
     *                              completed.
     * @param cancelPendingRequests if true, will drain the requests map and send an
     *                              IOException to all Callbacks.
     * @param cause                 if non-null and cancelPendingRequests is true,
     *                              this Throwable will be passed to all Callbacks.
     */
    private void disconnect(boolean awaitCompletion, boolean cancelPendingRequests, Throwable cause) {
        Channel channelToClose = null;
        Map<Integer, Callback<List<ByteBuffer>>> requestsToCancel = null;
        boolean stateReadLockHeld = stateLock.getReadHoldCount() != 0;

        ChannelFuture channelFutureToCancel = null;
        synchronized (channelFutureLock) {
            if (stopping && channelFuture != null) {
                channelFutureToCancel = channelFuture;
                channelFuture = null;
            }
        }
        if (channelFutureToCancel != null) {
            channelFutureToCancel.cancel(true);
        }

        if (stateReadLockHeld) {
            stateLock.readLock().unlock();
        }
        stateLock.writeLock().lock();
        try {
            if (channel != null) {
                if (cause != null) {
                    log.debug("Disconnecting from " + remoteAddr, cause);
                } else {
                    log.debug("Disconnecting from " + remoteAddr);
                }
                channelToClose = channel;
                channel = null;
                remote = null;
                if (cancelPendingRequests) {
                    // Remove all pending requests (will be canceled after relinquishing
                    // write lock).
                    requestsToCancel = new ConcurrentHashMap<Integer, Callback<List<ByteBuffer>>>(requests);
                    requests.clear();
                }
            }
        } finally {
            if (stateReadLockHeld) {
                stateLock.readLock().lock();
            }
            stateLock.writeLock().unlock();
        }

        // Cancel any pending requests by sending errors to the callbacks:
        if ((requestsToCancel != null) && !requestsToCancel.isEmpty()) {
            log.debug("Removing " + requestsToCancel.size() + " pending request(s).");
            for (Callback<List<ByteBuffer>> request : requestsToCancel.values()) {
                try {
                    request.handleError(cause != null ? cause
                            : new IOException(getClass().getSimpleName() + " closed"));
                } catch (Throwable e) {
                    log.info("Welp: " + e);
                }
            }
        }

        // Close the channel:
        if (channelToClose != null) {
            log.debug("Closing {}", channelToClose);
            ChannelFuture closeFuture = channelToClose.close();
            if (awaitCompletion && (closeFuture != null)) {
                try {
                    closeFuture.await(connectTimeoutMillis);
                    closeFuture.get();
                } catch (InterruptedException | ExecutionException e) {
                    Thread.currentThread().interrupt(); // Reset interrupt flag
                    log.warn("Interrupted while disconnecting", e);
                }
            }
            log.debug("Closed {}", channelToClose);
        } else {
            log.debug("no channel to close");
        }
    }

    /**
     * Netty channels are thread-safe, so there is no need to acquire locks. This
     * method is a no-op.
     */
    @Override
    public void lockChannel() {

    }

    /**
     * Netty channels are thread-safe, so there is no need to acquire locks. This
     * method is a no-op.
     */
    @Override
    public void unlockChannel() {

    }

    /**
     * Closes this transceiver and disconnects from the remote peer. Cancels all
     * pending RPCs, sends an IOException to all pending callbacks, and blocks until
     * the close has completed.
     */
    @Override
    public void close() {
        close(true);
    }

    /**
     * Closes this transceiver and disconnects from the remote peer. Cancels all
     * pending RPCs and sends an IOException to all pending callbacks.
     * 
     * @param awaitCompletion if true, will block until the close has completed.
     */
    public void close(boolean awaitCompletion) {
        // Close the connection:
        stopping = true;
        disconnect(awaitCompletion, true, null);
    }

    @Override
    public String getRemoteName() throws IOException {
        return remoteAddr.toString();
    }

    /**
     * Override as non-synchronized method because the method is thread safe.
     */
    @Override
    public List<ByteBuffer> transceive(List<ByteBuffer> request) throws IOException {
        try {
            CallFuture<List<ByteBuffer>> transceiverFuture = new CallFuture<List<ByteBuffer>>();
            transceive(request, transceiverFuture);
            return transceiverFuture.get();
        } catch (InterruptedException e) {
            log.debug("failed to get the response", e);
            return null;
        } catch (ExecutionException e) {
            log.debug("failed to get the response", e);
            return null;
        }
    }

    @Override
    public void transceive(List<ByteBuffer> request, Callback<List<ByteBuffer>> callback) throws IOException {
        stateLock.readLock().lock();
        try {
            int serial = serialGenerator.incrementAndGet();
            NettyDataPack dataPack = new NettyDataPack(serial, request);
            requests.put(serial, callback);
            try {
                writeDataPack(dataPack).get();
            } catch (InterruptedException | ExecutionException e) {
                throw new IOException(e);
            }
        } finally {
            stateLock.readLock().unlock();
        }
    }

    @Override
    public void writeBuffers(List<ByteBuffer> buffers) throws IOException {
        ChannelFuture writeFuture;
        stateLock.readLock().lock();
        try {
            writeFuture = writeDataPack(new NettyDataPack(serialGenerator.incrementAndGet(), buffers));
        } finally {
            stateLock.readLock().unlock();
        }

        if (!writeFuture.isDone()) {
            try {
                writeFuture.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // Reset interrupt flag
                throw new IOException("Interrupted while writing Netty data pack", e);
            }
        }
        if (!writeFuture.isSuccess()) {
            throw new IOException("Error writing buffers", writeFuture.cause());
        }
    }

    /**
     * Writes a NettyDataPack, reconnecting to the remote peer if necessary. NOTE:
     * The stateLock read lock *must* be acquired before calling this method.
     * 
     * @param dataPack the data pack to write.
     * @return the Netty ChannelFuture for the write operation.
     * @throws IOException if an error occurs connecting to the remote peer.
     */
    private ChannelFuture writeDataPack(NettyDataPack dataPack) throws IOException {
        Channel current = getChannel();
        if (current == null) {
            throw new ClosedChannelException();
        }
        return current.writeAndFlush(dataPack);
    }

    @Override
    public List<ByteBuffer> readBuffers() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Protocol getRemote() {
        stateLock.readLock().lock();
        try {
            return remote;
        } finally {
            stateLock.readLock().unlock();
        }
    }

    @Override
    public boolean isConnected() {
        stateLock.readLock().lock();
        try {
            return remote != null;
        } finally {
            stateLock.readLock().unlock();
        }
    }

    @Override
    public void setRemote(Protocol protocol) {
        stateLock.writeLock().lock();
        try {
            this.remote = protocol;
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    /**
     * A ChannelFutureListener for channel write operations that notifies a
     * {@link Callback} if an error occurs while writing to the channel.
     */
    protected class WriteFutureListener implements ChannelFutureListener {
        protected final Callback<List<ByteBuffer>> callback;

        /**
         * Creates a WriteFutureListener that notifies the given callback if an error
         * occurs writing data to the channel.
         * 
         * @param callback the callback to notify, or null to skip notification.
         */
        public WriteFutureListener(Callback<List<ByteBuffer>> callback) {
            this.callback = callback;
        }

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            if (!future.isSuccess() && (callback != null)) {
                callback.handleError(new IOException("Error writing buffers", future.cause()));
            }
        }
    }

    /**
     * Avro client handler for the Netty transport
     */
    protected class NettyClientAvroHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            NettyDataPack dataPack = (NettyDataPack) msg;
            Callback<List<ByteBuffer>> callback = requests.get(dataPack.getSerial());
            if (callback == null) {
                throw new RuntimeException("Missing previous call info");
            }
            try {
                callback.handleResult(dataPack.getDatas());
            } finally {
                requests.remove(dataPack.getSerial());
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            disconnect(false, true, cause.getCause());
            super.exceptionCaught(ctx, cause);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            log.debug("Remote peer " + remoteAddr + " closed connection.");
            disconnect(false, true, null);
            super.channelInactive(ctx);
        }

    }
}
