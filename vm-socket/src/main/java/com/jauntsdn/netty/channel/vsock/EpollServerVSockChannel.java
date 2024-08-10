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

import static io.netty.channel.epoll.LinuxSocket.newVSockStream;

import io.netty.channel.epoll.AbstractEpollServerChannel;
import io.netty.channel.epoll.LinuxSocket;
import io.netty.channel.epoll.VSockAddress;
import java.net.SocketAddress;

public final class EpollServerVSockChannel extends AbstractEpollServerChannel
    implements ServerVSockChannel {
  private final EpollServerVSockChannelConfig config;
  private volatile VSockAddress localAddr;

  public EpollServerVSockChannel() {
    super(newVSockStream(), false);
    config = new EpollServerVSockChannelConfig(this);
  }

  public EpollServerVSockChannel(int fd) {
    this(LinuxSocket.newSocket(fd));
  }

  EpollServerVSockChannel(LinuxSocket fd) {
    super(fd);
    config = new EpollServerVSockChannelConfig(this);
  }

  EpollServerVSockChannel(LinuxSocket fd, boolean active) {
    super(fd, active);
    config = new EpollServerVSockChannelConfig(this);
  }

  @Override
  protected void doBind(SocketAddress localAddress) throws Exception {
    if (!(localAddress instanceof VSockAddress)) {
      throw new Error("Unexpected local SocketAddress " + localAddress);
    }
    VSockAddress localVSock = (VSockAddress) localAddress;

    socket.bindVSock(localVSock);
    socket.listen(config.getBacklog());

    if (localVSock.getCid() != VSockAddress.VMADDR_CID_ANY
        && localVSock.getPort() != VSockAddress.VMADDR_PORT_ANY) {
      localAddr = localVSock;
    }
    active = true;
  }

  @Override
  public VSockAddress remoteAddress() {
    return null;
  }

  @Override
  public VSockAddress localAddress() {
    VSockAddress local = localAddr;
    if (local == null) {
      local = localAddr = socket.localVSockAddress();
    }
    return local;
  }

  @Override
  public EpollServerVSockChannelConfig config() {
    return config;
  }

  @Override
  public VSockChannel newChildChannel(int fd, byte[] address, int offset, int len) {
    LinuxSocket linuxSocket = LinuxSocket.newSocket(fd);
    return new EpollVSockChannel(this, linuxSocket, linuxSocket.remoteVSockAddress());
  }
}
