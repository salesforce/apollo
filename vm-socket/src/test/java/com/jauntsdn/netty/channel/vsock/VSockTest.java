/*
 * Copyright 2023 - present Maksym Ostroverkhov.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.jauntsdn.netty.channel.vsock;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.VSockAddress;
import io.netty.util.ReferenceCountUtil;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.CompletableFuture;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class VSockTest {

  Channel server;

  @AfterEach
  void tearDown() {
    Channel s = server;
    if (s != null) {
      s.close();
    }
  }

  @Test
  void remoteNonVSockAddress() throws Exception {
    InetSocketAddress unsupportedAddress = new InetSocketAddress("localhost", 8080);
    org.junit.jupiter.api.Assertions.assertThrows(
        Error.class, () -> server(unsupportedAddress, new ChannelInboundHandlerAdapter()));

    org.junit.jupiter.api.Assertions.assertThrows(
        Error.class, () -> client(unsupportedAddress, new ChannelInboundHandlerAdapter()));
  }

  @Test
  void localNonVSockAddress() {
    InetSocketAddress unsupportedAddress = new InetSocketAddress("localhost", 8080);

    org.junit.jupiter.api.Assertions.assertThrows(
        Error.class,
        () ->
            client(
                new VSockAddress(VSockAddress.VMADDR_CID_LOCAL, 8080),
                unsupportedAddress,
                new ChannelInboundHandlerAdapter()));
  }

  @Timeout(30)
  @Test
  void connectionOpen() throws Exception {
    VSockAddress serverAddress = new VSockAddress(VSockAddress.VMADDR_CID_LOCAL, 8080);
    VSockAddress clientAddress = new VSockAddress(VSockAddress.VMADDR_CID_LOCAL, 8081);
    LifecycleHandler serverHandler = new LifecycleHandler();
    LifecycleHandler clientHandler = new LifecycleHandler();

    server = server(serverAddress, serverHandler);
    Channel client = client(serverAddress, clientAddress, clientHandler);

    serverHandler.onConnected().join();
    clientHandler.onConnected().join();
    Assertions.assertThat(server.localAddress()).isExactlyInstanceOf(VSockAddress.class);
    VSockAddress actualServerAddress = (VSockAddress) server.localAddress();
    Assertions.assertThat(actualServerAddress.getCid()).isEqualTo(VSockAddress.VMADDR_CID_LOCAL);
    Assertions.assertThat(actualServerAddress.getPort()).isEqualTo(8080);

    Assertions.assertThat(clientHandler.localAddress).isExactlyInstanceOf(VSockAddress.class);
    Assertions.assertThat((VSockAddress) clientHandler.localAddress).isEqualTo(clientAddress);
    Assertions.assertThat((VSockAddress) clientHandler.remoteAddress).isEqualTo(serverAddress);

    Assertions.assertThat(serverHandler.localAddress).isExactlyInstanceOf(VSockAddress.class);
    Assertions.assertThat((VSockAddress) serverHandler.localAddress).isEqualTo(serverAddress);
    Assertions.assertThat((VSockAddress) serverHandler.remoteAddress).isEqualTo(clientAddress);
  }

  @Timeout(30)
  @Test
  void connectionCloseByClient() throws Exception {
    VSockAddress serverAddress = new VSockAddress(VSockAddress.VMADDR_CID_LOCAL, 8080);
    LifecycleHandler serverHandler = new LifecycleHandler();
    LifecycleHandler clientHandler = new LifecycleHandler();

    server = server(serverAddress, serverHandler);
    Channel client = client(serverAddress, clientHandler);
    clientHandler.onConnected().join();
    clientHandler.close();
    clientHandler.onClosed().join();
    serverHandler.onClosed().join();
  }

  @Timeout(30)
  @Test
  void connectionCloseByServer() throws Exception {
    VSockAddress serverAddress = new VSockAddress(VSockAddress.VMADDR_CID_LOCAL, 8080);
    LifecycleHandler serverHandler = new LifecycleHandler();
    LifecycleHandler clientHandler = new LifecycleHandler();

    server = server(serverAddress, serverHandler);
    Channel client = client(serverAddress, clientHandler);
    serverHandler.onConnected().join();
    serverHandler.close();
    serverHandler.onClosed().join();
    clientHandler.onClosed().join();
  }

  @Test
  void config() throws Exception {
    VSockAddress serverAddress = new VSockAddress(VSockAddress.VMADDR_CID_LOCAL, 8080);

    server = server(serverAddress, new ChannelInboundHandlerAdapter());
    Channel client = client(serverAddress, new ChannelInboundHandlerAdapter());

    Assertions.assertThat(client.config()).isExactlyInstanceOf(EpollVSockChannelConfig.class);
    EpollVSockChannelConfig clientConfig = (EpollVSockChannelConfig) client.config();
    Assertions.assertThat(clientConfig.getSendBufferSize()).isGreaterThan(0);
    Assertions.assertThat(clientConfig.getReceiveBufferSize()).isGreaterThan(0);
    Assertions.assertThat(server.config()).isExactlyInstanceOf(EpollServerVSockChannelConfig.class);
    EpollServerVSockChannelConfig serverConfig = (EpollServerVSockChannelConfig) server.config();
    Assertions.assertThat(serverConfig.getReceiveBufferSize()).isGreaterThan(0);

    /*verify setSockOpts does not throw*/
    serverConfig.setReceiveBufferSize(120_000);
  }

  @Timeout(30)
  @Test
  void exchange() throws Exception {
    VSockAddress serverAddress = new VSockAddress(VSockAddress.VMADDR_CID_LOCAL, 8088);

    server = server(serverAddress, new ServerExchangeHandler());
    ClientExchangeHandler handler = new ClientExchangeHandler();
    Channel client = client(serverAddress, handler);
    handler.onCompleted().join();
  }

  public Channel server(SocketAddress address, ChannelInboundHandler handler) throws Exception {
    ServerBootstrap bootstrap = new ServerBootstrap();
    Channel server =
        bootstrap
            .group(new EpollEventLoopGroup(4))
            .channel(EpollServerVSockChannel.class)
            .childHandler(
                new ChannelInitializer<Channel>() {

                  @Override
                  protected void initChannel(Channel ch) {
                    ch.pipeline().addLast(handler);
                  }
                })
            .bind(address)
            .sync()
            .channel();

    return server;
  }

  public Channel client(SocketAddress remote, ChannelInboundHandler handler) throws Exception {
    return client(remote, null, handler);
  }

  public Channel client(SocketAddress remote, SocketAddress local, ChannelInboundHandler handler)
      throws Exception {
    Bootstrap bootstrap =
        new Bootstrap()
            .group(new EpollEventLoopGroup(4))
            .channel(EpollVSockChannel.class)
            .handler(
                new ChannelInitializer<Channel>() {
                  @Override
                  protected void initChannel(Channel ch) {
                    ch.pipeline().addLast(handler);
                  }
                });
    if (local != null) {
      bootstrap.localAddress(local);
    }
    return bootstrap.connect(remote).sync().channel();
  }

  private static class LifecycleHandler extends ChannelInboundHandlerAdapter {
    final CompletableFuture<Void> onConnected = new CompletableFuture<>();
    final CompletableFuture<Void> onClosed = new CompletableFuture<>();

    volatile SocketAddress localAddress;
    volatile SocketAddress remoteAddress;
    volatile ChannelHandlerContext ctx;

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
      remoteAddress = ctx.channel().remoteAddress();
      localAddress = ctx.channel().localAddress();
      this.ctx = ctx;
      onConnected.complete(null);
      super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      onClosed.complete(null);
      super.channelInactive(ctx);
    }

    public void close() {
      ctx.close();
    }

    public CompletableFuture<Void> onConnected() {
      return onConnected;
    }

    public CompletableFuture<Void> onClosed() {
      return onClosed;
    }
  }

  private static class ServerExchangeHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      ctx.write(msg, ctx.voidPromise());
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
      ctx.flush();
      super.channelReadComplete(ctx);
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
      if (!ctx.channel().isWritable()) {
        ctx.flush();
      }
      super.channelWritabilityChanged(ctx);
    }
  }

  private static class ClientExchangeHandler extends ChannelInboundHandlerAdapter {
    final CompletableFuture<Void> onCompleted = new CompletableFuture<>();
    final int size = 77;
    int received;

    ClientExchangeHandler() {}

    public CompletableFuture<Void> onCompleted() {
      return onCompleted;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
      for (int i = 0; i < size; i++) {
        ctx.write(ctx.alloc().buffer(1).writeByte(i));
        if (!ctx.channel().isWritable()) {
          ctx.flush();
        }
      }
      ctx.flush();
      super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      CompletableFuture<Void> completed = onCompleted;
      if (!completed.isDone()) {
        completed.completeExceptionally(new ClosedChannelException());
      }
      super.channelInactive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      if (onCompleted.isDone()) {
        ReferenceCountUtil.release(msg);
        return;
      }
      ByteBuf byteBuf = (ByteBuf) msg;
      int readableBytes = byteBuf.readableBytes();
      for (int i = 0; i < readableBytes; i++) {
        int r = received++;
        byte b = byteBuf.readByte();
        if (b != r) {
          byteBuf.release();
          onCompleted.completeExceptionally(
              new IllegalStateException("unexpected value for index: " + r + " - " + b));
          ctx.close();
          return;
        }
      }
      byteBuf.release();
      if (received > size) {
        onCompleted.completeExceptionally(
            new IllegalStateException("Received more than requested: " + received));
        ctx.close();
      } else if (received == size) {
        onCompleted.complete(null);
        ctx.close();
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
      onCompleted.completeExceptionally(new IllegalStateException(cause));
      ctx.close();
    }
  }
}
