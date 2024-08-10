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

import static io.netty.channel.ChannelOption.ALLOW_HALF_CLOSURE;
import static io.netty.channel.ChannelOption.SO_RCVBUF;
import static io.netty.channel.ChannelOption.SO_SNDBUF;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelException;
import io.netty.channel.ChannelOption;
import io.netty.channel.MessageSizeEstimator;
import io.netty.channel.RecvByteBufAllocator;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.epoll.EpollChannelConfig;
import io.netty.channel.epoll.EpollMode;
import io.netty.channel.socket.DuplexChannelConfig;
import java.io.IOException;
import java.util.Map;

public final class EpollVSockChannelConfig extends EpollChannelConfig
    implements VSockChannelConfig, DuplexChannelConfig {
  private volatile boolean allowHalfClosure;

  EpollVSockChannelConfig(EpollVSockChannel channel) {
    super(channel);

    calculateMaxBytesPerGatheringWrite();
  }

  @Override
  public Map<ChannelOption<?>, Object> getOptions() {
    return getOptions(super.getOptions(), SO_RCVBUF, SO_SNDBUF, ALLOW_HALF_CLOSURE);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T getOption(ChannelOption<T> option) {
    if (option == SO_RCVBUF) {
      return (T) Integer.valueOf(getReceiveBufferSize());
    }
    if (option == SO_SNDBUF) {
      return (T) Integer.valueOf(getSendBufferSize());
    }
    if (option == ALLOW_HALF_CLOSURE) {
      return (T) Boolean.valueOf(isAllowHalfClosure());
    }
    return super.getOption(option);
  }

  @Override
  public <T> boolean setOption(ChannelOption<T> option, T value) {
    validate(option, value);

    if (option == ALLOW_HALF_CLOSURE) {
      setAllowHalfClosure((Boolean) value);
    } else {
      return super.setOption(option, value);
    }
    return true;
  }

  public int getReceiveBufferSize() {
    try {
      return socket().getReceiveBufferSize();
    } catch (IOException e) {
      throw new ChannelException(e);
    }
  }

  public int getSendBufferSize() {
    try {
      return socket().getSendBufferSize();
    } catch (IOException e) {
      throw new ChannelException(e);
    }
  }

  @Override
  public boolean isAllowHalfClosure() {
    return allowHalfClosure;
  }

  @Override
  public EpollVSockChannelConfig setAllowHalfClosure(boolean allowHalfClosure) {
    this.allowHalfClosure = allowHalfClosure;
    return this;
  }

  @Override
  public EpollVSockChannelConfig setConnectTimeoutMillis(int connectTimeoutMillis) {
    super.setConnectTimeoutMillis(connectTimeoutMillis);
    return this;
  }

  @Override
  @Deprecated
  public EpollVSockChannelConfig setMaxMessagesPerRead(int maxMessagesPerRead) {
    super.setMaxMessagesPerRead(maxMessagesPerRead);
    return this;
  }

  @Override
  public EpollVSockChannelConfig setWriteSpinCount(int writeSpinCount) {
    super.setWriteSpinCount(writeSpinCount);
    return this;
  }

  @Override
  public EpollVSockChannelConfig setAllocator(ByteBufAllocator allocator) {
    super.setAllocator(allocator);
    return this;
  }

  @Override
  public EpollVSockChannelConfig setRecvByteBufAllocator(RecvByteBufAllocator allocator) {
    super.setRecvByteBufAllocator(allocator);
    return this;
  }

  @Override
  public EpollVSockChannelConfig setAutoRead(boolean autoRead) {
    super.setAutoRead(autoRead);
    return this;
  }

  @Override
  public EpollVSockChannelConfig setAutoClose(boolean autoClose) {
    super.setAutoClose(autoClose);
    return this;
  }

  @Override
  @Deprecated
  public EpollVSockChannelConfig setWriteBufferHighWaterMark(int writeBufferHighWaterMark) {
    super.setWriteBufferHighWaterMark(writeBufferHighWaterMark);
    return this;
  }

  @Override
  @Deprecated
  public EpollVSockChannelConfig setWriteBufferLowWaterMark(int writeBufferLowWaterMark) {
    super.setWriteBufferLowWaterMark(writeBufferLowWaterMark);
    return this;
  }

  @Override
  public EpollVSockChannelConfig setWriteBufferWaterMark(
      WriteBufferWaterMark writeBufferWaterMark) {
    super.setWriteBufferWaterMark(writeBufferWaterMark);
    return this;
  }

  @Override
  public EpollVSockChannelConfig setMessageSizeEstimator(MessageSizeEstimator estimator) {
    super.setMessageSizeEstimator(estimator);
    return this;
  }

  @Override
  public EpollVSockChannelConfig setEpollMode(EpollMode mode) {
    super.setEpollMode(mode);
    return this;
  }

  private void calculateMaxBytesPerGatheringWrite() {
    int newSendBufferSize = getSendBufferSize() << 1;
    if (newSendBufferSize > 0) {
      setMaxBytesPerGatheringWrite(newSendBufferSize);
    }
  }
}
