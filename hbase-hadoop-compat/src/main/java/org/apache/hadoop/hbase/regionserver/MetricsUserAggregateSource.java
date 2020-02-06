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

package org.apache.hadoop.hbase.regionserver;

import java.util.Map;

import org.apache.hadoop.hbase.metrics.BaseSource;
import org.apache.yetus.audience.InterfaceAudience;

/**
* This interface will be implemented by a MetricsSource that will export metrics from
* multiple users into the hadoop metrics system.
*/
@InterfaceAudience.Private
public interface MetricsUserAggregateSource extends BaseSource {

  /**
   * The name of the metrics
   */
  static final String METRICS_NAME = "Users";

  /**
   * The name of the metrics context that metrics will be under.
   */
  static final String METRICS_CONTEXT = "regionserver";

  /**
   * Description
   */
  static final String METRICS_DESCRIPTION = "Metrics about users connected to the regionserver";

  /**
   * The name of the metrics context that metrics will be under in jmx
   */
  static final String METRICS_JMX_CONTEXT = "RegionServer,sub=" + METRICS_NAME;

  static final String NUM_USERS = "numUsers";
  static final String NUMBER_OF_USERS_DESC = "Number of users in the metrics system";

  /**
   * Returns a MetricsUserSource if already exists, or creates and registers one for this user
   * @param user the user name
   * @return a metrics user source
   */
  MetricsUserSource getOrCreateMetricsUser(String user);

  void deregister(MetricsUserSource toRemove);

  Map<String, MetricsUserSource> getUserSources();
}
