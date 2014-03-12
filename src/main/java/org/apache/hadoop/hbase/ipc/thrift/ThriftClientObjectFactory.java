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
import com.facebook.nifty.duplex.TDuplexProtocolFactory;
import com.facebook.nifty.header.client.HeaderClientConnector;
import com.facebook.nifty.header.protocol.TFacebookCompactProtocol;
import com.facebook.swift.service.ThriftClient;
import com.facebook.swift.service.ThriftClientConfig;
import com.facebook.swift.service.ThriftClientManager;
import io.airlift.units.Duration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ipc.ThriftClientInterface;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

public class ThriftClientObjectFactory extends
    BasePoolableObjectFactory<ThriftClientInterface> {
  private final Log LOG = LogFactory.getLog(ThriftClientObjectFactory.class);
  private final InetSocketAddress address;
  private final Class<? extends ThriftClientInterface> clazz;
  private final ThriftClientManager clientManager;
  private final Configuration conf;
  private final boolean useHeaderProtocol;
  private final ThriftClientConfig thriftClientConfig;

  public ThriftClientObjectFactory(final InetSocketAddress address,
      final Class<? extends ThriftClientInterface> clazz,
      ThriftClientManager clientManager,
      Configuration conf) {
    this.address = address;
    this.clazz = clazz;
    this.clientManager = clientManager;
    this.conf = conf;
    this.useHeaderProtocol = this.conf.getBoolean(HConstants.USE_HEADER_PROTOCOL,
      HConstants.DEFAULT_USE_HEADER_PROTOCOL);
    this.thriftClientConfig = new ThriftClientConfig()
      .setMaxFrameSize(HConstants.SWIFT_MAX_FRAME_SIZE_BYTES_DEFAULT)
      .setConnectTimeout(
        new Duration(conf.getInt(HConstants.HBASE_RPC_TIMEOUT_KEY,
          HConstants.DEFAULT_HBASE_RPC_TIMEOUT), TimeUnit.MILLISECONDS));
  }

  /**
   * TODO: create the connectors in async fashion and return ListenableFuture<ThriftClientInterface>
   */
  @Override
  public ThriftClientInterface makeObject() throws Exception {
    ThriftClient<? extends ThriftClientInterface> newClient = new ThriftClient<>(
      clientManager, clazz, thriftClientConfig,
        clazz.getName());

    if (useHeaderProtocol) {
      return newClient.open(new HeaderClientConnector(address)).get();
    } else {
      return newClient.open(
          new FramedClientConnector(address, TDuplexProtocolFactory
              .fromSingleFactory(new TFacebookCompactProtocol.Factory())))
          .get();
    }
  }

  @Override
  public void destroyObject(ThriftClientInterface obj) {
    try {
      obj.close();
    } catch (Exception e) {
      LOG.error("Connection could not be closed properly : " + this.address, e);
    }
  }
}
