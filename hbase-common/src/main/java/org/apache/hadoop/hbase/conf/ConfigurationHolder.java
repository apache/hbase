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
package org.apache.hadoop.hbase.conf;

import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * ConfigurationHolder class that holds Configuration so that they can be easily accessed anywhere.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ConfigurationHolder implements ConfigurationObserver {
  private final ReentrantLock confLock = new ReentrantLock();
  private Configuration conf;

  private ConfigurationHolder() {
  }

  private static class SingletonHelper {
    private static final ConfigurationHolder INSTANCE = new ConfigurationHolder();
  }

  /**
   * Returns a ConfigurationHolder
   * @return Configuration
   */
  public static ConfigurationHolder getInstance() {
    return SingletonHelper.INSTANCE;
  }

  /**
   * Update Configuration
   * @param conf Configuration
   */
  public void setConf(Configuration conf) {
    confLock.lock();
    try {
      this.conf = conf;
    } finally {
      confLock.unlock();
    }
  }

  /**
   * Check if a Configuration exists. If it does, return it. If not, create a new Configuration and
   * return it.
   * @return Configuration
   */
  public Configuration getConf() {
    confLock.lock();
    try {
      if (conf == null) {
        setConf(HBaseConfiguration.create());
      }

      return conf;
    } finally {
      confLock.unlock();
    }
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
    setConf(conf);
  }
}
