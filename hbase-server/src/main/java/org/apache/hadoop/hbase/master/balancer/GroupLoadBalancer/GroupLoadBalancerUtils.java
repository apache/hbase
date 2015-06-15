/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.master.balancer.grouploadbalancer;

public final class GroupLoadBalancerUtils {

  // not called
  private GroupLoadBalancerUtils() {
  }

  private static final String NAME_DELIMITER = ",";

  /**
   * Removes the startcode from a full server name to be matched with the config.
   * Eg. "10.255.196.145,60020,143396959045" -> "10.255.196.145,60020"
   * @param serverName full server name with start code
   * @return server name without start code to be matched with the config
   */
  public static String getServerNameWithoutStartCode(String serverName) {
    String[] serverNameArray = serverName.split(NAME_DELIMITER);
    return serverNameArray[0] + NAME_DELIMITER + serverNameArray[1];
  }

  /**
   * Given a region name return the table name.
   * Eg. "test_table_2,,1433969590459.a2e88feefb1bd49ae07a6f67ade7f526" -> "test_table_2"
   * @param regionName name of the region
   * @return the table as a string
   */
  public static String getTableNameFromRegionName(String regionName) {
    return regionName.split(NAME_DELIMITER)[0];
  }

}
