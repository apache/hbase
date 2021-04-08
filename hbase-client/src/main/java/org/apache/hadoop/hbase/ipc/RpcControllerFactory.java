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
package org.apache.hadoop.hbase.ipc;

import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScannable;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory to create a {@link HBaseRpcController}
 */
@InterfaceAudience.LimitedPrivate({HBaseInterfaceAudience.COPROC, HBaseInterfaceAudience.PHOENIX})
@InterfaceStability.Evolving
public class RpcControllerFactory {
  private static final Logger LOG = LoggerFactory.getLogger(RpcControllerFactory.class);

  /**
   * Custom RPC Controller factory allows frameworks to change the RPC controller. If the configured
   * controller cannot be found in the classpath or loaded, we fall back to the default RPC
   * controller factory.
   */
  public static final String CUSTOM_CONTROLLER_CONF_KEY = "hbase.rpc.controllerfactory.class";
  protected final Configuration conf;

  public RpcControllerFactory(Configuration conf) {
    this.conf = conf;
  }

  public HBaseRpcController newController() {
    // TODO: Set HConstants default rpc timeout here rather than nothing?
    return new HBaseRpcControllerImpl();
  }

  public HBaseRpcController newController(CellScanner cellScanner) {
    return new HBaseRpcControllerImpl(null, cellScanner);
  }

  public HBaseRpcController newController(RegionInfo regionInfo, CellScanner cellScanner) {
    return new HBaseRpcControllerImpl(regionInfo, cellScanner);
  }

  public HBaseRpcController newController(final List<CellScannable> cellIterables) {
    return new HBaseRpcControllerImpl(null, cellIterables);
  }

  public HBaseRpcController newController(RegionInfo regionInfo,
      final List<CellScannable> cellIterables) {
    return new HBaseRpcControllerImpl(regionInfo, cellIterables);
  }

  public static RpcControllerFactory instantiate(Configuration configuration) {
    String rpcControllerFactoryClazz =
        configuration.get(CUSTOM_CONTROLLER_CONF_KEY,
          RpcControllerFactory.class.getName());
    try {
      return ReflectionUtils.instantiateWithCustomCtor(rpcControllerFactoryClazz,
        new Class[] { Configuration.class }, new Object[] { configuration });
    } catch (UnsupportedOperationException | NoClassDefFoundError ex) {
      // HBASE-14960: In case the RPCController is in a non-HBase jar (Phoenix), but the application
      // is a pure HBase application, we want to fallback to the default one.
      String msg = "Cannot load configured \"" + CUSTOM_CONTROLLER_CONF_KEY + "\" ("
          + rpcControllerFactoryClazz + ") from hbase-site.xml, falling back to use "
          + "default RpcControllerFactory";
      if (LOG.isDebugEnabled()) {
        LOG.warn(msg, ex); // if DEBUG enabled, we want the exception, but still log in WARN level
      } else {
        LOG.warn(msg);
      }
      return new RpcControllerFactory(configuration);
    }
  }
}
