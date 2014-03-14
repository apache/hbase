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

import com.facebook.swift.service.ThriftClientManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ipc.ThriftClientInterface;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ThriftClientCache with a backing connection pool for each server. This also
 * provides knobs to control the rate at which eviction of connections in the
 * pool happen.
 */
public class ThriftClientCacheWithConnectionPooling implements
    ThriftClientCache {

  private final Log LOG = LogFactory
      .getLog(ThriftClientCacheWithConnectionPooling.class);
  public static final String TIME_BETWEEN_EVICTION_RUNS_MILLIS =
      "hbase.client.cachepool.timeBetweenEvictionRunsMillis";
  public static final String MIN_EVICTABLE_IDLE_TIME_MILLIS =
      "hbase.client.cachepool.minEvictableIdleTimeMillis";
  public static final String WHEN_EXHAUSTED_MAX_WAITTIME =
      "hbase.client.cachepool.whenExhaustedMaxWaitTime";
  public static final String WHEN_EXHAUSTED =
      "hbase.client.cachepool.whenExhausted";
  public static final String MAX_ACTIVE = "hbase.client.cachepool.maxActive";
  public static final String MIN_IDLE = "hbase.client.cachepool.minIdle";

  private final ThriftClientManager clientManager;
  private final Map<Pair<InetSocketAddress,
    Class<? extends ThriftClientInterface>>,
      GenericObjectPool<ThriftClientInterface>>
    clientPools;
  private Configuration conf;

  public ThriftClientCacheWithConnectionPooling(Configuration conf) {
    this.conf = conf;
    clientManager = new ThriftClientManager();
    clientPools = new ConcurrentHashMap<>();
  }

  @Override
  public ThriftClientInterface getClient(InetSocketAddress address,
      Class<? extends ThriftClientInterface> clazz) throws Exception {
    Pair<InetSocketAddress, Class<? extends ThriftClientInterface>> key =
        new Pair<InetSocketAddress, Class<? extends ThriftClientInterface>>(
        address, clazz);
    GenericObjectPool<ThriftClientInterface> clientPool = clientPools.get(key);
    if (clientPool == null) {
      synchronized (clientPools) {
        clientPool = clientPools.get(key);
        if (clientPool == null) {
          clientPool = createGenericObjectPool(address, clazz);
          clientPools.put(key, clientPool);
        }
      }
    }
    return clientPool.borrowObject();
  }

  /**
   * The GenericObjectPool can be re-configured as per the documentation here:
   * http://commons.apache.org/proper/commons-pool/api-1.6/org/
   * apache/commons/pool/impl/GenericObjectPool.html
   *
   * @param address
   * @param clazz
   * @return
   */
  private GenericObjectPool<ThriftClientInterface> createGenericObjectPool(
      InetSocketAddress address, Class<? extends ThriftClientInterface> clazz) {
    ThriftClientObjectFactory factory = new ThriftClientObjectFactory(address,
        clazz, this.clientManager, this.conf);
    GenericObjectPool.Config config = new GenericObjectPool.Config();
    long DEFAULT_TIME_BETWEEN_EVICTION_RUNS_MILLIS = 1000;
    long DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS = 30000;
    long DEFAULT_WHEN_EXHAUSTED_MAX_WAITTIME = 1000;
    int DEFAULT_MAX_ACTIVE = 2000;

    // Keep some idle connections to prevent asynchronous client from contention on
    //   a single connection to each region server.
    // WARNING: Default maxIdle value is 8. If you want minIdle to be large than 8,
    //   you have to also modify config.maxIdle.
    int DEFAULT_MIN_IDLE = 5;

    config.maxActive = conf.getInt(MAX_ACTIVE, DEFAULT_MAX_ACTIVE);
    config.timeBetweenEvictionRunsMillis =
        conf.getLong(TIME_BETWEEN_EVICTION_RUNS_MILLIS,
            DEFAULT_TIME_BETWEEN_EVICTION_RUNS_MILLIS);
    config.minEvictableIdleTimeMillis =
        conf.getLong(MIN_EVICTABLE_IDLE_TIME_MILLIS,
            DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS);
    config.minIdle =
        conf.getInt(MIN_IDLE, DEFAULT_MIN_IDLE);

    // The idea is to fail fast if we run out of the maximum number of
    // connections for one region server, which by default is pretty large (500).
    // If we exhaust that many connections, we have bigger problems to fix.
    config.whenExhaustedAction = (byte)conf.getInt(WHEN_EXHAUSTED,
        GenericObjectPool.WHEN_EXHAUSTED_FAIL);
    config.maxWait = conf.getLong(WHEN_EXHAUSTED_MAX_WAITTIME,
        DEFAULT_WHEN_EXHAUSTED_MAX_WAITTIME);
    config.lifo = false;
    GenericObjectPool<ThriftClientInterface> pool = new GenericObjectPool<ThriftClientInterface>(
        factory, config);
    return pool;
  }

  /**
   * Puts back a ThriftClientInterface back to the client pool. Reuse of the
   * ThriftClientInterface after placing it back into the cache might result in
   * unpredicted behavior.
   *
   * @throws Exception
   */
  @Override
  public void putBackClient(ThriftClientInterface client,
      InetSocketAddress address, Class<? extends ThriftClientInterface> clazz)
      throws Exception {
    Pair<InetSocketAddress, Class<? extends ThriftClientInterface>> key = new Pair<InetSocketAddress, Class<? extends ThriftClientInterface>>(
        address, clazz);
    GenericObjectPool<ThriftClientInterface> clientPool = clientPools.get(key);
    if (clientPool == null) {
      client.close();
      return;
    }
    clientPool.returnObject(client);
  }

  @Override
  public ThriftClientManager getThriftClientManager() {
    return this.clientManager;
  }

  @Override
  public void close(InetSocketAddress address,
      Class<? extends ThriftClientInterface> clazz, ThriftClientInterface client)
      throws IOException {
    Pair<InetSocketAddress, Class<? extends ThriftClientInterface>> key = new Pair<InetSocketAddress, Class<? extends ThriftClientInterface>>(
        address, clazz);
    GenericObjectPool<ThriftClientInterface> clientPool = clientPools.get(key);
    try {
      if (clientPool == null) {
        client.close();
        return;
      }

      clientPool.invalidateObject(client);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public synchronized void shutDownClientManager() throws Exception {
    for (Pair<InetSocketAddress, Class<? extends ThriftClientInterface>> pair : this.clientPools
        .keySet()) {
      closeAll(pair.getFirst(), pair.getSecond());
    }
    clientPools.clear();
  }

  private void closeAll(InetSocketAddress server,
      Class<? extends ThriftClientInterface> clientInterface)
      throws IOException {
    Pair<InetSocketAddress, Class<? extends ThriftClientInterface>> key = new Pair<InetSocketAddress, Class<? extends ThriftClientInterface>>(
        server, clientInterface);
    GenericObjectPool<ThriftClientInterface> clientPool = clientPools.get(key);
    clientPool.clear();
  }
}
