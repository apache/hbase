/**
 * Copyright 2014 The Apache Software Foundation
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
package org.apache.hadoop.hbase.coprocessor.endpoints;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ServerCallable;
import org.apache.hadoop.hbase.ipc.thrift.exceptions.ThriftHBaseException;
import org.apache.hadoop.hbase.util.ExceptionUtils;

/**
 * A IEndpointClient served as part of an HTable.
 */
public class HTableEndpointClient implements IEndpointClient {
  private HTable table;

  public HTableEndpointClient(HTable table) {
    this.table = table;
  }

  /**
   * Returns an proxy instance for an IEndpont.
   *
   * @param clazz
   *          the class of the endpoint interface to call.
   * @param region
   *          the region info
   * @param startRow
   *          the start row
   * @param stopRow
   *          the end row
   */
  @SuppressWarnings("unchecked")
  protected <T extends IEndpoint> T getEndpointProxy(final Class<T> clazz,
      final HRegionInfo region, final byte[] startRow, final byte[] stopRow) {

    InvocationHandler handler = new InvocationHandler() {
      @Override
      public Object invoke(Object proxy, final Method method,
          final Object[] args) throws Throwable {
        HConnection conn = table.getConnectionAndResetOperationContext();
        return conn.getRegionServerWithRetries(new ServerCallable<Object>(
            table.getConnection(), table.getTableNameStringBytes(),
            region.getStartKey(), table.getOptions()) {
          @Override
          public Object call() throws ThriftHBaseException {
            byte[] res = server.callEndpoint(clazz.getName(), method.getName(),
                EndpointBytesCodec.encodeArray(args),
                region.getRegionName(), startRow, stopRow);

            return EndpointBytesCodec.decode(method.getReturnType(), res);
          }
        });
      }
    };

    return (T) Proxy.newProxyInstance(clazz.getClassLoader(),
        new Class<?>[] { clazz }, handler);
  }

  @Override
  public <T extends IEndpoint, R> Map<HRegionInfo, R> coprocessorEndpoint(
      Class<T> clazz, byte[] startRow, byte[] stopRow, Caller<T, R> caller)
      throws IOException {
    Map<HRegionInfo, R> results = new HashMap<>();

    NavigableMap<HRegionInfo, HServerAddress> regions = table.getRegionsInfo();

    try {
      for (final HRegionInfo region : regions.keySet()) {
        // TODO compute startRow and stopRow
        T ep =
            getEndpointProxy(clazz, region, HConstants.EMPTY_BYTE_ARRAY,
                HConstants.EMPTY_BYTE_ARRAY);
        results.put(region, caller.call(ep));
      }
    } catch (UndeclaredThrowableException e) {
      throw ExceptionUtils.toIOException(e.getUndeclaredThrowable());
    }

    return results;
  }
}
