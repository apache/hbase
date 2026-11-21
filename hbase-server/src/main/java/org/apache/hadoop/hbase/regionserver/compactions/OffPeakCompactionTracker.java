/**
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

package org.apache.hadoop.hbase.regionserver.compactions;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.conf.ConfigurationManager;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class OffPeakCompactionTracker extends Semaphore implements ConfigurationObserver {
  private static final Logger LOG = LoggerFactory.getLogger(OffPeakCompactionTracker.class);

  private static final OffPeakCompactionTracker INSTANCE = new OffPeakCompactionTracker(
    CompactionConfiguration.DEFAULT_OFFPEAK_COMPACTION_CONCURRENCY);

  private final AtomicBoolean registered;

  private int concurrency;

  public OffPeakCompactionTracker(int concurrency) {
    super(concurrency);
    this.concurrency = concurrency;
    this.registered = new AtomicBoolean(false);
  }

  public static OffPeakCompactionTracker getInstance() {
    return INSTANCE;
  }

  public void registerIfNeeded(ConfigurationManager manager, Configuration conf) {
    if (registered.get()) {
      return;
    }
    if (registered.compareAndSet(false, true)) {
      manager.registerObserver(this);
      updateConcurrency(conf);
    }
  }

  public void updateConcurrency(Configuration conf) {
    int newVal = conf.getInt(
                 CompactionConfiguration.HBASE_HSTORE_OFFPEAK_COMPACTION_CONCURRENCY_KEY,
                 CompactionConfiguration.DEFAULT_OFFPEAK_COMPACTION_CONCURRENCY);

    if (this.concurrency == newVal) {
      return;
    }

    LOG.info("Changing the value of {} from {} to {}",
              CompactionConfiguration.HBASE_HSTORE_OFFPEAK_COMPACTION_CONCURRENCY_KEY,
              this.concurrency,
              newVal);

    int delta = newVal - this.concurrency;
    if (delta > 0) {
      release(delta);
    }

    if (delta < 0) {
      reducePermits(-delta);
    }

    this.concurrency = newVal;
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
    updateConcurrency(conf);
  }
}
