/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.comm.netty4;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.function.Function;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLPeerUnverifiedException;

import org.apache.avro.ipc.RPCPlugin;
import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.salesforce.apollo.comm.netty4.NettyTransportCodec.NettyDataPack;
import com.salesforce.apollo.comm.netty4.NettyTransportCodec.NettyFrameDecoder;
import com.salesforce.apollo.comm.netty4.NettyTransportCodec.NettyFrameEncoder;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.ChannelGroupFuture;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.compression.FastLzFrameDecoder;
import io.netty.handler.codec.compression.FastLzFrameEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.GlobalEventExecutor;

/**
 * @author hhildebrand
 */
public class MtlsServer implements Server {
	class AvroHandler extends ChannelInboundHandlerAdapter {

		private final NettyTlsTransceiver connectionMetadata = new NettyTlsTransceiver();
		private volatile Responder responder;
		private final SSLEngine sslEngine;

		public AvroHandler(SSLEngine sslEngine) {
			this.sslEngine = sslEngine;
		}

		@Override
		public void channelActive(final ChannelHandlerContext ctx) throws Exception {
			ctx.pipeline().get(SslHandler.class).handshakeFuture()
					.addListener(new GenericFutureListener<Future<Channel>>() {
						@Override
						public void operationComplete(Future<Channel> future) throws Exception {
							responder = getResponder(getSessionId());
							if (responder == null) {
								LOG.info("No responder, closing");
								ctx.close();
								return;
							}

							allChannels.add(ctx.channel());
						}
					});
			super.channelActive(ctx);
		}

		@Override
		public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
			try {
				NettyDataPack dataPack = (NettyDataPack) msg;
				List<ByteBuffer> req = dataPack.getDatas();
				Responder handler = getResponder(getSessionId());
				if (handler == null) {
					ctx.channel().close();
					return;
				}
				List<ByteBuffer> res = handler.respond(req, connectionMetadata);
				// response will be null for oneway messages.
				if (res != null) {
					dataPack.setDatas(res);
					ctx.channel().writeAndFlush(dataPack);
				}
			} catch (IOException ex) {
				LOG.warn("unexpected error", ex);
				ctx.close();
			}
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
			LOG.warn("Error caught", cause);
			ctx.close();
			super.exceptionCaught(ctx, cause);
		}

		private Responder getResponder(String sessionId) {
			Responder current = responder;
			if (current == null) {
				X509Certificate cert;
				try {
					cert = sslEngine.getNeedClientAuth()
							? (X509Certificate) sslEngine.getSession().getPeerCertificates()[0]
							: null;
				} catch (SSLPeerUnverifiedException e) {
					return null;
				}

				try {
					responder = responders.get(sessionId, () -> {
						Responder newResponder = responderProvider.apply(cert);
						if (stats != null) {
							responder.addRPCPlugin(stats);
						}
						return newResponder;
					});
				} catch (ExecutionException e) {
					throw new IllegalStateException("unable to create responder", e);
				}
			}
			return responder;
		}

		private String getSessionId() {
			byte[] bytes = sslEngine.getSession().getId();
			if (bytes == null) {
				return null;
			}

			StringBuilder sb = new StringBuilder();
			for (byte b : bytes) {
				String digit = Integer.toHexString(b);
				if (digit.length() < 2) {
					sb.append('0');
				}
				if (digit.length() > 2) {
					digit = digit.substring(digit.length() - 2);
				}
				sb.append(digit);
			}
			return sb.toString();
		}
	}

	private static final Logger LOG = LoggerFactory.getLogger(MtlsServer.class.getName());

	public static CacheBuilder<String, Responder> defaultBuiilder() {
		CacheBuilder<?, ?> builder = CacheBuilder.from("maximumSize=1000,expireAfterWrite=120s");
		@SuppressWarnings("unchecked")
		CacheBuilder<String, Responder> castBuilder = (CacheBuilder<String, Responder>) builder;
		return castBuilder;
	}

	private final ChannelGroup allChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
	private final EventLoopGroup bossGroup;
	private final Channel channel;
	private final CountDownLatch closed = new CountDownLatch(1);
	private final Function<X509Certificate, Responder> responderProvider;
	private final Cache<String, Responder> responders;
	private volatile RPCPlugin stats;
	private final EventLoopGroup workerGroup;

	public MtlsServer(InetSocketAddress address, SslContext sslCtx,
			Function<X509Certificate, Responder> responderProvider, CacheBuilder<String, Responder> builder,
			int bossThreads, String labelPrefix, int workerThreads) {
		LOG.debug("Server starting, binding to: {}", address);
		responders = builder.build();
		this.responderProvider = responderProvider;

		bossGroup = new NioEventLoopGroup(bossThreads, new ThreadFactory() {
			volatile int i = 0;

			@Override
			public Thread newThread(Runnable r) {
				int thread = i++;
				Thread t = new Thread(r, labelPrefix + " boss [" + thread + "]");
				t.setDaemon(true);
				return t;
			}
		});
		workerGroup = new NioEventLoopGroup(workerThreads, new ThreadFactory() {
			volatile int i = 0;

			@Override
			public Thread newThread(Runnable r) {
				int thread = i++;
				Thread t = new Thread(r, labelPrefix + " worker[" + thread + "]");
				t.setDaemon(true);
				return t;
			}
		});

		ChannelFuture future = new ServerBootstrap().option(ChannelOption.SO_BACKLOG, 128)
				.option(ChannelOption.SO_REUSEADDR, true).childOption(ChannelOption.TCP_NODELAY, true)
				.childOption(ChannelOption.SO_KEEPALIVE, true).group(bossGroup, workerGroup)
				.channel(NioServerSocketChannel.class)
				// .handler(new LoggingHandler(LogLevel.TRACE))
				.childHandler(new ChannelInitializer<SocketChannel>() {
					@Override
					protected void initChannel(SocketChannel ch) throws Exception {
						ChannelPipeline pipeline = ch.pipeline();
						SslHandler newHandler = sslCtx.newHandler(ch.alloc());
						pipeline.addLast(newHandler);
						SSLEngine engine = newHandler.engine();
						pipeline.addLast(new FastLzFrameDecoder());
						pipeline.addLast(new FastLzFrameEncoder());
						if (LOG.isTraceEnabled()) {
							pipeline.addLast(new LoggingHandler("server", LogLevel.TRACE));
						}
						pipeline.addLast(new NettyFrameDecoder());
						pipeline.addLast(new NettyFrameEncoder());
						pipeline.addLast(new AvroHandler(engine));
					}
				}).bind(address);
		try {
			future.sync();
		} catch (InterruptedException e) {
			throw new IllegalStateException("Binding sync interrupted", e);
		}
		if (!future.isSuccess()) {
			LOG.error("Server unable to bind to {}", future.cause());
			throw new IllegalStateException("Unable to bind server", future.cause());
		}
		channel = future.channel();
		LOG.debug("Server started, bound: {}", address);
	}

	@Override
	public void close() {
		ChannelGroupFuture future = allChannels.close();
		try {
			future.sync();
		} catch (InterruptedException e) {
			LOG.error("Failure closing all channels", e);
		}
		try {
			future.get();
		} catch (InterruptedException | ExecutionException e) {
			LOG.error("Failure closing all channels", e);
		}
		channel.close().awaitUninterruptibly();
		try {
			channel.close().get();
		} catch (InterruptedException | ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			bossGroup.shutdownGracefully().get();
		} catch (InterruptedException | ExecutionException e) {
			LOG.error("Failure shutting down boss group", e);
		}
		try {
			workerGroup.shutdownGracefully().get();
		} catch (InterruptedException | ExecutionException e) {
			LOG.error("Failure shutting down worker group", e);
		}
		closed.countDown();
	}

	public int getNumActiveConnections() {
		return allChannels.size();
	}

	@Override
	public int getPort() {
		return ((InetSocketAddress) channel.remoteAddress()).getPort();
	}

	@Override
	public void join() throws InterruptedException {
		closed.await();
	}

	public void setStats(RPCPlugin stats) {
		this.stats = stats;
	}

	@Override
	public void start() {
	}

}
