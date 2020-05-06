/*
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
package org.apache.hadoop.hbase.net;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility to generate a bound socket. Useful testing for BindException.
 * Use one of the Constructors to create an instance of this class. On creation it will have put
 * up a ServerSocket on a random port. Get the port it is bound to using {@link #getPort()}. In
 * your test, then try to start a Server using same port to generate a BindException. Call
 * {@link #close()} when done to shut down the Socket.
 */
public final class BoundSocketMaker implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(BoundSocketMaker.class);
  private final ServerSocket socket;

  private BoundSocketMaker() {
    this.socket = null;
  }

  public BoundSocketMaker(Supplier<Integer> randomPortMaker) {
    this(InetAddress.getLoopbackAddress().getHostName(), randomPortMaker);
  }

  public BoundSocketMaker(final String hostname, Supplier<Integer> randomPortMaker) {
    this.socket = get(hostname, randomPortMaker);
  }

  public int getPort() {
    return this.socket.getLocalPort();
  }

  /**
   * @return Returns a bound socket; be sure to close when done.
   */
  private ServerSocket get(String hostname, Supplier<Integer> randomPortMaker) {
    ServerSocket ss = null;
    int port = -1;
    while (true) {
      port = randomPortMaker.get();
      try {
        ss = new ServerSocket();
        ss.bind(new InetSocketAddress(hostname, port));
        break;
      } catch (IOException ioe) {
        LOG.warn("Failed bind", ioe);
        try {
          ss.close();
        } catch (IOException ioe2) {
          LOG.warn("FAILED CLOSE of failed bind socket", ioe2);
        }
      }
    }
    return ss;
  }

  @Override public void close() throws IOException {
    if (this.socket != null) {
      this.socket.close();
    }
  }
}
