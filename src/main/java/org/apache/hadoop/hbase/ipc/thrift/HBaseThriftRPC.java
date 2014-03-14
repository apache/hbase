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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ipc.HBaseRPCOptions;
import org.apache.hadoop.hbase.ipc.ThriftClientInterface;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Simple RPC mechanism for making client calls. It contains caching of the
 * client connection, such that we don't create connections multiple times for
 * the same client.
 *
 */
public class HBaseThriftRPC {
  private static final Log LOG = LogFactory.getLog(HBaseThriftRPC.class);
  private static final Map<Configuration, ThriftClientCache> CLIENT_CACHE =
      new ConcurrentHashMap<>();
  public static ThreadLocal<Stack<Boolean>> isMeta =
      new ThreadLocal<Stack<Boolean>>() {
    @Override
    public Stack<Boolean> initialValue() {
      return new Stack<>();
    }
  };

  private static Configuration metaConf = null;

  public static void clearAll() throws Exception {
    metaConf = null;
    isMeta.get().clear();
    synchronized (CLIENT_CACHE) {
      for (ThriftClientCache cache : CLIENT_CACHE.values()) {
        cache.shutDownClientManager();
      }
      CLIENT_CACHE.clear();
    }
  }

  /**
   * USE WITH CAUTION
   * Returns the ThriftClientInterface and THriftClientManager for the given parameters.
   * Creates a new Thrift client connection using the given parameters.
   * @param addr
   * @param conf
   * @param clazz
   * @return
   * @throws IOException
   */
  protected static Pair<ThriftClientInterface, ThriftClientManager> getClientWithoutWrapper(
      InetSocketAddress addr,
      Configuration conf,
      Class<? extends ThriftClientInterface> clazz) throws IOException {
    conf = checkIfInMeta(conf);
    ThriftClientCache clientsForConf = CLIENT_CACHE.get(conf);
    if (clientsForConf == null) {
      synchronized (CLIENT_CACHE) {
        clientsForConf = CLIENT_CACHE.get(conf);
        if (clientsForConf == null) {
          clientsForConf = (new ThriftClientCacheFactory(conf))
              .createClientCacheWithPooling();
          CLIENT_CACHE.put(conf, clientsForConf);
        }
      }
    }
    try {
      ThriftClientInterface client;
      try {
        client = clientsForConf.getClient(addr, clazz);
      } catch (Exception e) {
        LOG.warn("Failed getting Client without Wrapper for address: " +
          addr.toString(), e);
        throw new IOException(e);
      }
      return new Pair<> (client, clientsForConf.getThriftClientManager());
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Use this instead of getClientWrapper.
   * @param addr
   * @param conf
   * @param clazz
   * @return
   * @throws IOException
   */
  public static ThriftClientInterface getClient(InetSocketAddress addr,
      Configuration conf, Class<? extends ThriftClientInterface> clazz,
      HBaseRPCOptions options) throws IOException {
    Pair<ThriftClientInterface, ThriftClientManager> interfaceAndManager = getClientWithoutWrapper(
        addr, conf, clazz);
    return new HBaseToThriftAdapter(interfaceAndManager.getFirst(),
        interfaceAndManager.getSecond(), addr, conf, clazz, options);
  }

  /**
   * Refreshes the connection i.e., closes and returns a new connection and the
   * connection manager We use the auxiliary parameters in this function call to
   * uniquely identify the given ThriftClientInterface.
   *
   * @param inetSocketAddress
   * @param conf
   * @param thriftClient
   * @param clientInterface
   * @return
   * @throws IOException
   */
  protected static Pair<ThriftClientInterface, ThriftClientManager> refreshConnection(
      InetSocketAddress inetSocketAddress,
      Configuration conf, ThriftClientInterface thriftClient,
      Class<? extends ThriftClientInterface> clientInterface) throws IOException {
    conf = checkIfInMeta(conf);
    ThriftClientCache clientsForConf = CLIENT_CACHE.get(conf);
    try {
      if (clientsForConf == null) {
        LOG.error("Client cache pool for current configuration is null.");
        thriftClient.close();
      } else {
        clientsForConf.close(inetSocketAddress, clientInterface, thriftClient);
      }
      LOG.debug("Refreshing client connection due to an error in the channel. Remote address: " + inetSocketAddress);
      return getClientWithoutWrapper(inetSocketAddress, conf, clientInterface);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Clean up current connection.
   *
   * @param inetSocketAddress
   * @param conf
   * @param thriftClient
   * @param clientInterface
   * @throws IOException
   */
  protected static void cleanUpConnection(
      InetSocketAddress inetSocketAddress,
      Configuration conf,
      ThriftClientInterface thriftClient,
      Class<? extends ThriftClientInterface> clientInterface) throws IOException {
    conf = checkIfInMeta(conf);
    ThriftClientCache clientsForConf = CLIENT_CACHE.get(conf);
    try {
      if (clientsForConf == null) {
        LOG.error("Client cache pool for current configuration is null.");
        thriftClient.close();
      } else {
        clientsForConf.close(inetSocketAddress, clientInterface, thriftClient);
      }
      LOG.debug("Clean up connection due to an error in the channel");
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private static Configuration checkIfInMeta(Configuration inputConf) {
    Configuration conf = inputConf;
    if (!isMeta.get().isEmpty() && isMeta.get().peek()) {
      if (metaConf == null) {
        metaConf = (HBaseConfiguration.create(inputConf));
        // Make metaConf different from inputConf
        metaConf.setStrings("metaConf", "yes");
      }
      conf = metaConf;
    }
    return conf;
  }

  public static void putBackClient(ThriftClientInterface server,
      InetSocketAddress addr, Configuration conf,
      Class<? extends ThriftClientInterface> clazz) throws Exception {
    conf = checkIfInMeta(conf);
    ThriftClientCache clientsForConf = CLIENT_CACHE.get(conf);
    if (clientsForConf == null) {
      return;
    }
    clientsForConf.putBackClient(server, addr, clazz);
  }
}
