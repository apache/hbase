/**
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
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.ReflectionUtils;

/**
 * Factory to create a {@link PayloadCarryingRpcController}
 */
@InterfaceAudience.Private
public class RpcControllerFactory {

  public static final String CUSTOM_CONTROLLER_CONF_KEY = "hbase.rpc.controllerfactory.class";
  protected final Configuration conf;

  public RpcControllerFactory(Configuration conf) {
    this.conf = conf;
  }

  public PayloadCarryingRpcController newController() {
    return new PayloadCarryingRpcController();
  }

  public PayloadCarryingRpcController newController(final CellScanner cellScanner) {
    return new PayloadCarryingRpcController(cellScanner);
  }

  public PayloadCarryingRpcController newController(final List<CellScannable> cellIterables) {
    return new PayloadCarryingRpcController(cellIterables);
  }


  public static RpcControllerFactory instantiate(Configuration configuration) {
    String rpcControllerFactoryClazz =
        configuration.get(CUSTOM_CONTROLLER_CONF_KEY,
          RpcControllerFactory.class.getName());
    return ReflectionUtils.instantiateWithCustomCtor(rpcControllerFactoryClazz,
      new Class[] { Configuration.class }, new Object[] { configuration });
  }
}
