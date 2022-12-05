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
package org.apache.hadoop.hbase.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Factory to create an {@link RpcRetryingCaller}
 */
@InterfaceAudience.Private
public class RpcRetryingCallerFactory {

  /** Configuration key for a custom {@link RpcRetryingCaller} */
  public static final String CUSTOM_CALLER_CONF_KEY = "hbase.rpc.callerfactory.class";
  protected final Configuration conf;
  private final ConnectionConfiguration connectionConf;
  private final RetryingCallerInterceptor interceptor;
  private final int startLogErrorsCnt;
  private final MetricsConnection metrics;

  public RpcRetryingCallerFactory(Configuration conf) {
    this(conf, RetryingCallerInterceptorFactory.NO_OP_INTERCEPTOR, null);
  }

  public RpcRetryingCallerFactory(Configuration conf, RetryingCallerInterceptor interceptor,
    MetricsConnection metrics) {
    this.conf = conf;
    this.connectionConf = new ConnectionConfiguration(conf);
    startLogErrorsCnt = conf.getInt(AsyncProcess.START_LOG_ERRORS_AFTER_COUNT_KEY,
      AsyncProcess.DEFAULT_START_LOG_ERRORS_AFTER_COUNT);
    this.interceptor = interceptor;
    this.metrics = metrics;
  }

  /**
   * Create a new RetryingCaller with specific rpc timeout.
   */
  public <T> RpcRetryingCaller<T> newCaller(int rpcTimeout) {
    // We store the values in the factory instance. This way, constructing new objects
    // is cheap as it does not require parsing a complex structure.
    return new RpcRetryingCallerImpl<>(connectionConf.getPauseMillis(),
      connectionConf.getPauseMillisForServerOverloaded(), connectionConf.getRetriesNumber(),
      interceptor, startLogErrorsCnt, rpcTimeout, metrics);
  }

  /**
   * Create a new RetryingCaller with configured rpc timeout.
   */
  public <T> RpcRetryingCaller<T> newCaller() {
    // We store the values in the factory instance. This way, constructing new objects
    // is cheap as it does not require parsing a complex structure.
    return new RpcRetryingCallerImpl<>(connectionConf.getPauseMillis(),
      connectionConf.getPauseMillisForServerOverloaded(), connectionConf.getRetriesNumber(),
      interceptor, startLogErrorsCnt, connectionConf.getRpcTimeout(), metrics);
  }

  public static RpcRetryingCallerFactory instantiate(Configuration configuration,
    MetricsConnection metrics) {
    return instantiate(configuration, RetryingCallerInterceptorFactory.NO_OP_INTERCEPTOR, null,
      metrics);
  }

  public static RpcRetryingCallerFactory instantiate(Configuration configuration,
    ServerStatisticTracker stats, MetricsConnection metrics) {
    return instantiate(configuration, RetryingCallerInterceptorFactory.NO_OP_INTERCEPTOR, stats,
      metrics);
  }

  public static RpcRetryingCallerFactory instantiate(Configuration configuration,
    RetryingCallerInterceptor interceptor, ServerStatisticTracker stats,
    MetricsConnection metrics) {
    String clazzName = RpcRetryingCallerFactory.class.getName();
    String rpcCallerFactoryClazz =
      configuration.get(RpcRetryingCallerFactory.CUSTOM_CALLER_CONF_KEY, clazzName);
    RpcRetryingCallerFactory factory;
    if (rpcCallerFactoryClazz.equals(clazzName)) {
      factory = new RpcRetryingCallerFactory(configuration, interceptor, metrics);
    } else {
      factory = ReflectionUtils.instantiateWithCustomCtor(rpcCallerFactoryClazz,
        new Class[] { Configuration.class }, new Object[] { configuration });
    }
    return factory;
  }
}
