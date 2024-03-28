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
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.conf.ConfigurationManager;
import org.apache.hadoop.hbase.conf.PropagatingConfigurationObserver;
import org.apache.hadoop.hbase.io.hfile.PrefetchExecutor;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to submit requests for PrefetchExecutor depending on configuration change
 */
@InterfaceAudience.Private
public final class PrefetchExecutorNotifier implements PropagatingConfigurationObserver {
  private static final Logger LOG = LoggerFactory.getLogger(CompactSplit.class);

  /** Wait time in miliseconds before executing prefetch */
  public static final String PREFETCH_DELAY = "hbase.hfile.prefetch.delay";
  private final Configuration conf;

  // only for test
  public PrefetchExecutorNotifier(Configuration conf) {
    this.conf = conf;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void onConfigurationChange(Configuration newConf) {
    // Update prefetch delay in the prefetch executor class
    // interrupt and restart threads which have not started executing
    PrefetchExecutor.loadConfiguration(conf);
    LOG.info("Config hbase.hfile.prefetch.delay is changed to {}",
      conf.getInt(PREFETCH_DELAY, 1000));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void registerChildren(ConfigurationManager manager) {
    // No children to register.
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void deregisterChildren(ConfigurationManager manager) {
    // No children to register
  }

  public int getPrefetchDelay() {
    return PrefetchExecutor.getPrefetchDelay();
  }
}
