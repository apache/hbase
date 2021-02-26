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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.thrift2.generated.THBaseService;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Converts a Hbase.Iface using InvocationHandler so that it reports process
 * time of each call to ThriftMetrics.
 */
@InterfaceAudience.Private
public class HbaseHandlerMetricsProxy implements InvocationHandler {

  private final Object handler;
  private final ThriftMetrics metrics;

  public static Hbase.Iface newInstance(Hbase.Iface handler,
                                        ThriftMetrics metrics,
                                        Configuration conf) {
    return (Hbase.Iface) Proxy.newProxyInstance(
        handler.getClass().getClassLoader(),
        new Class[]{Hbase.Iface.class},
        new HbaseHandlerMetricsProxy(handler, metrics, conf));
  }

  // for thrift 2
  public static THBaseService.Iface newInstance(THBaseService.Iface handler,
      ThriftMetrics metrics,
      Configuration conf) {
    return (THBaseService.Iface) Proxy.newProxyInstance(
        handler.getClass().getClassLoader(),
        new Class[]{THBaseService.Iface.class},
        new HbaseHandlerMetricsProxy(handler, metrics, conf));
  }

  private HbaseHandlerMetricsProxy(
      Object handler, ThriftMetrics metrics, Configuration conf) {
    this.handler = handler;
    this.metrics = metrics;
  }

  @Override
  public Object invoke(Object proxy, Method m, Object[] args)
      throws Throwable {
    Object result;
    long start = now();
    try {
      result = m.invoke(handler, args);
    } catch (InvocationTargetException e) {
      metrics.exception(e.getCause());
      throw e.getTargetException();
    } catch (Exception e) {
      metrics.exception(e);
      throw new RuntimeException(
          "unexpected invocation exception: " + e.getMessage());
    } finally {
      long processTime = now() - start;
      metrics.incMethodTime(m.getName(), processTime);
    }
    return result;
  }
  
  private static long now() {
    return System.nanoTime();
  }
}
