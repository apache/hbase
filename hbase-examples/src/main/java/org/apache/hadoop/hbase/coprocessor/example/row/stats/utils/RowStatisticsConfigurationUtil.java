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
package org.apache.hadoop.hbase.coprocessor.example.row.stats.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class RowStatisticsConfigurationUtil {

  private static final String ROW_STATISTICS_PREFIX = "hubspot.row.statistics.";

  public static int getInt(Configuration conf, String name, int defaultValue) {
    return conf.getInt(ROW_STATISTICS_PREFIX + name, defaultValue);
  }

  public static long getLong(Configuration conf, String name, long defaultValue) {
    return conf.getLong(ROW_STATISTICS_PREFIX + name, defaultValue);
  }

  private static void setInt(Configuration conf, String name, int defaultValue) {
    conf.setInt(name, conf.getInt(ROW_STATISTICS_PREFIX + name, defaultValue));
  }

  private static void setLong(Configuration conf, String name, long defaultValue) {
    conf.setLong(name, conf.getLong(ROW_STATISTICS_PREFIX + name, defaultValue));
  }
}
