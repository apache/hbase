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

package org.apache.hadoop.hbase.chaos.util;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * This class can be used to control chaos monkeys life cycle.
 */
public class Monkeys implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(Monkeys.class);

  private final Configuration conf;
  private final ChaosMonkeyRunner monkeyRunner;
  private final Runnable runner;
  private final ExecutorService executor;

  public Monkeys() {
    this(HBaseConfiguration.create());
  }

  public Monkeys(Configuration conf) {
    this.conf = Preconditions.checkNotNull(conf, "Should specify a configuration");
    this.monkeyRunner = new ChaosMonkeyRunner();
    this.runner = () -> {
      try {
        monkeyRunner.getAndStartMonkey();
      } catch (Exception e) {
        LOG.error("Exception occurred when running chaos monkeys: ", e);
      }
    };
    this.executor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
        .setDaemon(true).setNameFormat("ChaosMonkey").build());
    IntegrationTestingUtility.setUseDistributedCluster(this.conf);
  }

  public void addResource(Configuration otherConf) {
    conf.addResource(otherConf);
    monkeyRunner.setConf(conf);
  }

  public void addResource(String otherConf) {
    conf.addResource(otherConf);
    monkeyRunner.setConf(conf);
  }

  public void startChaos() {
    executor.execute(runner);
    LOG.info("Chaos monkeys are running.");
  }

  public void stopChaos() {
    monkeyRunner.stopRunner();
    LOG.info("Chaos monkeys are stopped.");
  }

  @Override
  public void close() throws IOException {
    executor.shutdown();
    try {
      // wait 10 seconds.
      executor.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn("Interruption occurred while stopping chaos monkeys " + e);
    }
  }
}
