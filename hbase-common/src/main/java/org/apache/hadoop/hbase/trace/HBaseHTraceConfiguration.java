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

package org.apache.hadoop.hbase.trace;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.htrace.HTraceConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

@InterfaceAudience.Private
public class HBaseHTraceConfiguration extends HTraceConfiguration {
  private static final Log LOG =
    LogFactory.getLog(HBaseHTraceConfiguration.class);

  public static final String KEY_PREFIX = "hbase.htrace.";

  private Configuration conf;

  private void handleDeprecation(String key) {
    String oldKey = "hbase." + key;
    String newKey = KEY_PREFIX + key;
    String oldValue = conf.get(oldKey);
    if (oldValue != null) {
      LOG.warn("Warning: using deprecated configuration key " + oldKey +
          ".  Please use " + newKey + " instead.");
      String newValue = conf.get(newKey);
      if (newValue == null) {
        conf.set(newKey, oldValue);
      } else {
        LOG.warn("Conflicting values for " + newKey + " and " + oldKey +
            ".  Using " + newValue);
      }
    }
  }

  public HBaseHTraceConfiguration(Configuration conf) {
    this.conf = conf;
    handleDeprecation("local-file-span-receiver.path");
    handleDeprecation("local-file-span-receiver.capacity");
    handleDeprecation("sampler.frequency");
    handleDeprecation("sampler.fraction");
    handleDeprecation("zipkin.collector-hostname");
    handleDeprecation("zipkin.collector-port");
    handleDeprecation("zipkin.num-threads");
    handleDeprecation("zipkin.traced-service-hostname");
    handleDeprecation("zipkin.traced-service-port");
  }

  @Override
  public String get(String key) {
    return conf.get(KEY_PREFIX +key);
  }

  @Override
  public String get(String key, String defaultValue) {
    return conf.get(KEY_PREFIX + key,defaultValue);

  }

  @Override
  public boolean getBoolean(String key, boolean defaultValue) {
    return conf.getBoolean(KEY_PREFIX + key, defaultValue);
  }
}
