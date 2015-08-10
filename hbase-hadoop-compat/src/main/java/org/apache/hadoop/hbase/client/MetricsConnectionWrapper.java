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
package org.apache.hadoop.hbase.client;

/**
 * This is the interface that will expose Connection information to hadoop1/hadoop2
 * implementations of the {@link MetricsConnectionSource}.
 */
public interface MetricsConnectionWrapper {

  /** Get the connection's unique identifier */
  String getId();

  /** Get the User's name. */
  String getUserName();

  /** Get the Cluster ID */
  String getClusterId();

  /** Get the Zookeeper Quorum Info */
  String getZookeeperQuorum();

  /** Get the base ZNode for this cluster. */
  String getZookeeperBaseNode();

  int getMetaLookupPoolActiveCount();

  int getMetaLookupPoolLargestPoolSize();

  String getBatchPoolId();

  int getBatchPoolActiveCount();

  int getBatchPoolLargestPoolSize();
}
