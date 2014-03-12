/*
 * Copyright The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.ipc.thrift;

import com.facebook.nifty.client.FramedClientConnector;
import com.facebook.nifty.header.client.HeaderClientConnector;
import com.facebook.swift.service.ThriftClientManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ipc.ThriftClientInterface;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

/**
 * ThriftClient cache which keeps a client cache per thread in order to avoid
 * reusing the client in parallel threads at the same time.
 */
public class ThreadBasedThriftClientCache implements ThriftClientCache {
  private final static Log LOG =
      LogFactory.getLog(ThreadBasedThriftClientCache.class);

  private static final ThreadLocal<HashMap<Pair<InetSocketAddress,
    Class<? extends ThriftClientInterface>>, ThriftClientInterface>> clients
    = new ThreadLocal<HashMap<Pair<InetSocketAddress,
        Class<? extends ThriftClientInterface>>, ThriftClientInterface>>() {
      @Override
      public HashMap<Pair<InetSocketAddress,
        Class<? extends ThriftClientInterface>>, ThriftClientInterface>
      initialValue() {
        return new HashMap<Pair<InetSocketAddress,
        Class<? extends ThriftClientInterface>>, ThriftClientInterface>();
      }
    };

  private final ThriftClientManager clientManager;

  private boolean useHeaderProtocol;

  public ThreadBasedThriftClientCache(Configuration conf) {
    clientManager = new ThriftClientManager();
    useHeaderProtocol = conf.getBoolean(HConstants.USE_HEADER_PROTOCOL,
      HConstants.DEFAULT_USE_HEADER_PROTOCOL);
  }

  /**
   * It has a simple caching, such that when it is called for a same client
   * multiple times - the cached value will be returned. When called first time
   * the connection will be created
   *
   * @param address - the inet address
   * @param clazz - type of client
   * @return
   * @throws InterruptedException
   * @throws ExecutionException
   */
  public ThriftClientInterface getClient(
      InetSocketAddress address, Class<? extends ThriftClientInterface> clazz)
      throws InterruptedException, ExecutionException {
    Pair<InetSocketAddress, Class<? extends ThriftClientInterface>> key = new Pair<InetSocketAddress, Class<? extends ThriftClientInterface>>(
        address, clazz);
    ThriftClientInterface client = clients.get().get(key);
    if (client != null) {
      return client;
    }
    synchronized (clientManager) {
      ThriftClientInterface newClient;
      if (useHeaderProtocol) {
        newClient = clientManager.createClient(
          new HeaderClientConnector(address), clazz).get();
      } else {
        newClient = clientManager.createClient(
          new FramedClientConnector(address), clazz).get();
      }
      clients.get().put(key, newClient);
      return newClient;
    }
  }

  /**
   * Closes the client manager and clears the map of cached clients
   * @throws Exception
   */
  public synchronized void shutDownClientManager() throws Exception {
    for (ThriftClientInterface client : clients.get().values()) {
      client.close();
    }
    clients.get().clear();
    this.clientManager.close();
  }

  /**
   * Closes a connection of the specified client
   * @param address
   * @throws Exception
   */
  @Override
  public synchronized void close(InetSocketAddress address,
      Class<? extends ThriftClientInterface> clazz,
      ThriftClientInterface client) throws IOException {
    Pair<InetSocketAddress, Class<? extends ThriftClientInterface>> key =
        new Pair<InetSocketAddress, Class<? extends ThriftClientInterface>>(
        address, clazz);
    ThriftClientInterface cachedClient = clients.get().remove(key);
    try {
      if (cachedClient != null) {
        cachedClient.close();
      } else {
        client.close();
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void putBackClient(ThriftClientInterface server,
      InetSocketAddress addr, Class<? extends ThriftClientInterface> clazz) {
    // Do nothing for ThreadBasedThriftClientCache.
  }

  @Override
  public ThriftClientManager getThriftClientManager() {
    return this.clientManager;
  }
}
