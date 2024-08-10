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

import static io.netty.channel.ChannelOption.SO_BACKLOG;
import static io.netty.channel.ChannelOption.SO_RCVBUF;
import static io.netty.util.internal.ObjectUtil.checkPositiveOrZero;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelException;
import io.netty.channel.ChannelOption;
import io.netty.channel.MessageSizeEstimator;
import io.netty.channel.RecvByteBufAllocator;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.epoll.EpollChannelConfig;
import io.netty.channel.epoll.EpollMode;
import io.netty.channel.socket.ServerSocketChannelConfig;
import io.netty.util.NetUtil;
import java.io.IOException;
import java.util.Map;

public class EpollServerVSockChannelConfig extends EpollChannelConfig
    implements ServerSocketChannelConfig {
  private volatile int backlog = NetUtil.SOMAXCONN;

  EpollServerVSockChannelConfig(EpollServerVSockChannel channel) {
    super(channel);
  }

  @Override
  public Map<ChannelOption<?>, Object> getOptions() {
    return getOptions(super.getOptions(), SO_RCVBUF, SO_BACKLOG);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T getOption(ChannelOption<T> option) {
    if (option == SO_RCVBUF) {
      return (T) Integer.valueOf(getReceiveBufferSize());
    }
    if (option == SO_BACKLOG) {
      return (T) Integer.valueOf(getBacklog());
    }
    return super.getOption(option);
  }

  @Override
  public <T> boolean setOption(ChannelOption<T> option, T value) {
    validate(option, value);
    return super.setOption(option, value);
  }

  @Override
  public EpollServerVSockChannelConfig setReuseAddress(boolean reuseAddress) {
    return this;
  }

  @Override
  public int getReceiveBufferSize() {
    try {
      return socket().getReceiveBufferSize();
    } catch (IOException e) {
      throw new ChannelException(e);
    }
  }

  @Override
  public EpollServerVSockChannelConfig setReceiveBufferSize(int receiveBufferSize) {
    return this;
  }

  @Override
  public EpollServerVSockChannelConfig setPerformancePreferences(
      int connectionTime, int latency, int bandwidth) {
    return this;
  }

  @Override
  public int getBacklog() {
    return backlog;
  }

  @Override
  public EpollServerVSockChannelConfig setBacklog(int backlog) {
    checkPositiveOrZero(backlog, "backlog");
    this.backlog = backlog;
    return this;
  }

  @Override
  public boolean isReuseAddress() {
    return false;
  }

  @Override
  public EpollServerVSockChannelConfig setConnectTimeoutMillis(int connectTimeoutMillis) {
    super.setConnectTimeoutMillis(connectTimeoutMillis);
    return this;
  }

  @Override
  @Deprecated
  public EpollServerVSockChannelConfig setMaxMessagesPerRead(int maxMessagesPerRead) {
    super.setMaxMessagesPerRead(maxMessagesPerRead);
    return this;
  }

  @Override
  public EpollServerVSockChannelConfig setWriteSpinCount(int writeSpinCount) {
    super.setWriteSpinCount(writeSpinCount);
    return this;
  }

  @Override
  public EpollServerVSockChannelConfig setAllocator(ByteBufAllocator allocator) {
    super.setAllocator(allocator);
    return this;
  }

  @Override
  public EpollServerVSockChannelConfig setRecvByteBufAllocator(RecvByteBufAllocator allocator) {
    super.setRecvByteBufAllocator(allocator);
    return this;
  }

  @Override
  public EpollServerVSockChannelConfig setAutoRead(boolean autoRead) {
    super.setAutoRead(autoRead);
    return this;
  }

  @Override
  @Deprecated
  public EpollServerVSockChannelConfig setWriteBufferHighWaterMark(int writeBufferHighWaterMark) {
    super.setWriteBufferHighWaterMark(writeBufferHighWaterMark);
    return this;
  }

  @Override
  @Deprecated
  public EpollServerVSockChannelConfig setWriteBufferLowWaterMark(int writeBufferLowWaterMark) {
    super.setWriteBufferLowWaterMark(writeBufferLowWaterMark);
    return this;
  }

  @Override
  public EpollServerVSockChannelConfig setWriteBufferWaterMark(
      WriteBufferWaterMark writeBufferWaterMark) {
    super.setWriteBufferWaterMark(writeBufferWaterMark);
    return this;
  }

  @Override
  public EpollServerVSockChannelConfig setMessageSizeEstimator(MessageSizeEstimator estimator) {
    super.setMessageSizeEstimator(estimator);
    return this;
  }

  @Override
  public EpollServerVSockChannelConfig setEpollMode(EpollMode mode) {
    super.setEpollMode(mode);
    return this;
  }
}
