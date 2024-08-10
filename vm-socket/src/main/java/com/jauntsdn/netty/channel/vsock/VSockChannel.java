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

import io.netty.channel.epoll.VSockAddress;
import io.netty.channel.socket.DuplexChannel;
import io.netty.channel.unix.UnixChannel;

public interface VSockChannel extends UnixChannel, DuplexChannel {

  @Override
  VSockAddress remoteAddress();

  @Override
  VSockAddress localAddress();

  @Override
  EpollVSockChannelConfig config();
}
