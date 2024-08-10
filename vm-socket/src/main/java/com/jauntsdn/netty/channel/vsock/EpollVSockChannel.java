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

import io.netty.channel.Channel;
import io.netty.channel.epoll.AbstractEpollStreamChannel;
import io.netty.channel.epoll.LinuxSocket;
import io.netty.channel.epoll.Native;
import io.netty.channel.epoll.VSockAddress;
import java.net.SocketAddress;

public final class EpollVSockChannel extends AbstractEpollStreamChannel implements VSockChannel {
  private final EpollVSockChannelConfig config;
  private volatile VSockAddress localAddr;
  private volatile VSockAddress remoteAddr;

  public EpollVSockChannel() {
    super(newVSockStream(), false);
    config = new EpollVSockChannelConfig(this);
  }

  public EpollVSockChannel(int fd) {
    super(fd);
    config = new EpollVSockChannelConfig(this);
  }

  EpollVSockChannel(LinuxSocket fd, boolean active) {
    super(fd, active);
    config = new EpollVSockChannelConfig(this);
  }

  EpollVSockChannel(Channel parent, LinuxSocket fd, VSockAddress remoteAddress) {
    super(parent, fd, remoteAddress);
    this.remoteAddr = remoteAddress;
    config = new EpollVSockChannelConfig(this);
  }

  @Override
  public VSockAddress remoteAddress() {
    return remoteAddr;
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
  public EpollVSockChannelConfig config() {
    return config;
  }

  @Override
  protected boolean doConnect(SocketAddress remoteAddress, SocketAddress localAddress)
      throws Exception {
    if (!(remoteAddress instanceof VSockAddress)) {
      throw new Error("Unexpected remote SocketAddress " + remoteAddress);
    }
    VSockAddress remoteVSock = (VSockAddress) remoteAddress;

    VSockAddress localVSock = null;
    if (localAddress != null) {
      if (!(localAddress instanceof VSockAddress)) {
        throw new Error("Unexpected local SocketAddress " + localAddress);
      }
      localVSock = (VSockAddress) localAddress;
      socket.bindVSock(localVSock);
    }
    boolean succeeded = false;
    try {
      boolean connected = socket.connectVSock(remoteVSock);
      if (!connected) {
        setFlag(Native.EPOLLOUT);
      }

      if (localVSock != null) {
        if (localVSock.getCid() != VSockAddress.VMADDR_CID_ANY
            && localVSock.getPort() != VSockAddress.VMADDR_PORT_ANY) {
          localAddr = localVSock;
        }
      }
      remoteAddr = remoteVSock;
      succeeded = true;
      return connected;
    } finally {
      if (!succeeded) {
        doClose();
      }
    }
  }
}
