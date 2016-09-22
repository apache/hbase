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

package org.apache.hadoop.hbase.thrift;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.thrift.generated.Hbase;


/**
 * Converts a Hbase.Iface using InvocationHandler so that it reports process
 * time of each call to ThriftMetrics.
 */
@InterfaceAudience.Private
public class HbaseHandlerMetricsProxy implements InvocationHandler {

  private static final Log LOG = LogFactory.getLog(
      HbaseHandlerMetricsProxy.class);

  private final Hbase.Iface handler;
  private final ThriftMetrics metrics;

  public static Hbase.Iface newInstance(Hbase.Iface handler,
                                        ThriftMetrics metrics,
                                        Configuration conf) {
    return (Hbase.Iface) Proxy.newProxyInstance(
        handler.getClass().getClassLoader(),
        new Class[]{Hbase.Iface.class},
        new HbaseHandlerMetricsProxy(handler, metrics, conf));
  }

  private HbaseHandlerMetricsProxy(
      Hbase.Iface handler, ThriftMetrics metrics, Configuration conf) {
    this.handler = handler;
    this.metrics = metrics;
  }

  @Override
  public Object invoke(Object proxy, Method m, Object[] args)
      throws Throwable {
    Object result;
    try {
      long start = now();
      result = m.invoke(handler, args);
      long processTime = now() - start;
      metrics.incMethodTime(m.getName(), processTime);
    } catch (InvocationTargetException e) {
      throw e.getTargetException();
    } catch (Exception e) {
      throw new RuntimeException(
          "unexpected invocation exception: " + e.getMessage());
    }
    return result;
  }
  
  private static long now() {
    return System.nanoTime();
  }
}
