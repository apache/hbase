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

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.util.Threads;

/**
 * This coproceesor sleep 2s at first increment/append rpc call.
 */
public class SleepAtFirstRpcCall implements RegionCoprocessor, RegionObserver {
  static final AtomicLong ct = new AtomicLong(0);
  static final String SLEEP_TIME_CONF_KEY = "hbase.coprocessor.SleepAtFirstRpcCall.sleepTime";
  static final long DEFAULT_SLEEP_TIME = 2000;
  static final AtomicLong sleepTime = new AtomicLong(DEFAULT_SLEEP_TIME);

  @Override
  public Optional<RegionObserver> getRegionObserver() {
    return Optional.of(this);
  }

  public SleepAtFirstRpcCall() {
  }

  @Override
  public void postOpen(ObserverContext<? extends RegionCoprocessorEnvironment> c) {
    RegionCoprocessorEnvironment env = c.getEnvironment();
    Configuration conf = env.getConfiguration();
    sleepTime.set(conf.getLong(SLEEP_TIME_CONF_KEY, DEFAULT_SLEEP_TIME));
  }

  @Override
  public Result postIncrement(final ObserverContext<? extends RegionCoprocessorEnvironment> e,
    final Increment increment, final Result result) throws IOException {
    if (ct.incrementAndGet() == 1) {
      Threads.sleep(sleepTime.get());
    }
    return result;
  }

  @Override
  public Result postAppend(final ObserverContext<? extends RegionCoprocessorEnvironment> e,
    final Append append, final Result result) throws IOException {
    if (ct.incrementAndGet() == 1) {
      Threads.sleep(sleepTime.get());
    }
    return result;
  }
}
