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

package org.apache.hadoop.hbase.chaos.factories;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.chaos.monkies.ChaosMonkey;

import com.google.common.collect.ImmutableMap;

/**
 * Base class of the factory that will create a ChaosMonkey.
 */
public abstract class MonkeyFactory {

  protected String tableName;
  protected Set<String> columnFamilies;
  protected IntegrationTestingUtility util;
  protected Properties properties = new Properties();

  public MonkeyFactory setTableName(String tableName) {
    this.tableName = tableName;
    return this;
  }

  public MonkeyFactory setColumnFamilies(Set<String> columnFamilies) {
    this.columnFamilies = columnFamilies;
    return this;
  }

  public MonkeyFactory setUtil(IntegrationTestingUtility util) {
    this.util = util;
    return this;
  }

  public MonkeyFactory setProperties(Properties props) {
    if (props != null) {
      this.properties = props;
    }
    return this;
  }

  public abstract ChaosMonkey build();

  public static final String CALM = "calm";
  // TODO: the name has become a misnomer since the default (not-slow) monkey has been removed
  public static final String SLOW_DETERMINISTIC = "slowDeterministic";
  public static final String UNBALANCE = "unbalance";
  public static final String SERVER_KILLING = "serverKilling";
  public static final String STRESS_AM = "stressAM";
  public static final String NO_KILL = "noKill";
  public static final String SERVER_AND_DEPENDENCIES_KILLING = "serverAndDependenciesKilling";

  public static Map<String, MonkeyFactory> FACTORIES = ImmutableMap.<String,MonkeyFactory>builder()
    .put(CALM, new CalmMonkeyFactory())
    .put(SLOW_DETERMINISTIC, new SlowDeterministicMonkeyFactory())
    .put(UNBALANCE, new UnbalanceMonkeyFactory())
    .put(SERVER_KILLING, new ServerKillingMonkeyFactory())
    .put(SERVER_AND_DEPENDENCIES_KILLING, new ServerAndDependenciesKillingMonkeyFactory())
    .put(STRESS_AM, new StressAssignmentManagerMonkeyFactory())
    .put(NO_KILL, new NoKillMonkeyFactory())
    .build();

  public static MonkeyFactory getFactory(String factoryName) {
    return FACTORIES.get(factoryName);
  }
}
