/*
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hbase.thrift;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;

/**
 * This class is for maintaining the various statistics of thrift server
 * and publishing them through the metrics interfaces.
 */
@InterfaceAudience.Private
public class ThriftMetrics  {


  public enum ThriftServerType {
    ONE,
    TWO
  }

  public MetricsThriftServerSource getSource() {
    return source;
  }

  public void setSource(MetricsThriftServerSource source) {
    this.source = source;
  }

  private MetricsThriftServerSource source;
  private final long slowResponseTime;
  public static final String SLOW_RESPONSE_NANO_SEC =
    "hbase.thrift.slow.response.nano.second";
  public static final long DEFAULT_SLOW_RESPONSE_NANO_SEC = 10 * 1000 * 1000;


  public ThriftMetrics(Configuration conf, ThriftServerType t) {
    slowResponseTime = conf.getLong( SLOW_RESPONSE_NANO_SEC, DEFAULT_SLOW_RESPONSE_NANO_SEC);

    if (t == ThriftServerType.ONE) {
      source = CompatibilitySingletonFactory.getInstance(MetricsThriftServerSourceFactory.class).createThriftOneSource();
    } else if (t == ThriftServerType.TWO) {
      source = CompatibilitySingletonFactory.getInstance(MetricsThriftServerSourceFactory.class).createThriftTwoSource();
    }

  }

  public void incTimeInQueue(long time) {
    source.incTimeInQueue(time);
  }

  public void setCallQueueLen(int len) {
    source.setCallQueueLen(len);
  }

  public void incNumRowKeysInBatchGet(int diff) {
    source.incNumRowKeysInBatchGet(diff);
  }

  public void incNumRowKeysInBatchMutate(int diff) {
    source.incNumRowKeysInBatchMutate(diff);
  }

  public void incMethodTime(String name, long time) {
    source.incMethodTime(name, time);
    // inc general processTime
    source.incCall(time);
    if (time > slowResponseTime) {
      source.incSlowCall(time);
    }
  }

}
