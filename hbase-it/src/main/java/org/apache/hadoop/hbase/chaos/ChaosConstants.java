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

package org.apache.hadoop.hbase.chaos;

import org.apache.yetus.audience.InterfaceAudience;

/***
 * ChaosConstant holds a bunch of Choas-related Constants
 */
@InterfaceAudience.Public
public final class ChaosConstants {

  /*Base ZNode for whole Chaos Testing*/
  public static final String CHAOS_TEST_ROOT_ZNODE = "/hbase";

  /*Just a / used for path separator*/
  public static final String ZNODE_PATH_SEPARATOR = "/";

  /*ZNode used for ChaosAgents registration.*/
  public static final String CHAOS_AGENT_REGISTRATION_EPIMERAL_ZNODE =
    CHAOS_TEST_ROOT_ZNODE + ZNODE_PATH_SEPARATOR + "chaosAgents";

  /*ZNode used for getting status of tasks assigned*/
  public static final String CHAOS_AGENT_STATUS_PERSISTENT_ZNODE =
    CHAOS_TEST_ROOT_ZNODE + ZNODE_PATH_SEPARATOR + "chaosAgentTaskStatus";

  /*Config property for getting number of retries to execute a command*/
  public static final String RETRY_ATTEMPTS_KEY = "hbase.it.clustermanager.retry.attempts";

  /*Default value for number of retries*/
  public static final int DEFAULT_RETRY_ATTEMPTS = 5;

  /*Config property to sleep in between retries*/
  public static final String RETRY_SLEEP_INTERVAL_KEY =
    "hbase.it.clustermanager.retry.sleep.interval";

  /*Default Sleep time between each retry*/
  public static final int DEFAULT_RETRY_SLEEP_INTERVAL = 5000;

  /*Config property for executing command as specific user*/
  public static final String CHAOSAGENT_SHELL_USER = "hbase.it.clustermanager.ssh.user";

  /*default user for executing local commands*/
  public static final String DEFAULT_SHELL_USER = "";

  /*timeout used while creating ZooKeeper connection*/
  public static final int SESSION_TIMEOUT_ZK = 60000 * 10;

  /*Time given to ChaosAgent to set status*/
  public static final int SET_STATUS_SLEEP_TIME = 30 * 1000;

  /*Status String when you get an ERROR while executing task*/
  public static final String TASK_ERROR_STRING = "error";

  /*Status String when your command gets executed correctly*/
  public static final String TASK_COMPLETION_STRING = "done";

  /*Name of ChoreService to use*/
  public static final String CHORE_SERVICE_PREFIX = "ChaosService";

}
