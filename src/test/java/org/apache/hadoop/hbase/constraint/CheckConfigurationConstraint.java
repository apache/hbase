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
package org.apache.hadoop.hbase.constraint;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;

/**
 * Test Constraint to check to make sure the configuration is set
 */
public class CheckConfigurationConstraint extends BaseConstraint {


  private static String key = "testKey";
  private static String value = "testValue";

  public static Configuration getConfiguration() {
    Configuration conf = new Configuration();
    conf.set(key, value);
    return conf;
  }

  @Override
  public void check(Put p) {
    // NOOP
  }

  @Override
  public void setConf(Configuration conf) {
    String val = conf.get(key);
    if (val == null || !val.equals(value))
      throw new IllegalArgumentException(
          "Configuration was not passed correctly");
    super.setConf(conf);
  }

}
