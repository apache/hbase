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
package org.apache.hadoop.hbase.master;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.ScriptBasedMapping;
/**
 * Wrapper over the rack resolution utility in Hadoop. The rack resolution
 * utility in Hadoop does resolution from hosts to the racks they belong to.
 *
 */
@InterfaceAudience.Private
public class RackManager {
  public static final String UNKNOWN_RACK = "Unknown Rack";

  private DNSToSwitchMapping switchMapping;

  public RackManager() {
  }

  public RackManager(Configuration conf) {
    switchMapping = ReflectionUtils.instantiateWithCustomCtor(
        conf.getClass("hbase.util.ip.to.rack.determiner", ScriptBasedMapping.class,
             DNSToSwitchMapping.class).getName(), new Class<?>[]{Configuration.class},
               new Object[]{conf});
  }

  /**
   * Get the name of the rack containing a server, according to the DNS to
   * switch mapping.
   * @param server the server for which to get the rack name
   * @return the rack name of the server
   */
  public String getRack(ServerName server) {
    if (server == null) {
      return UNKNOWN_RACK;
    }
    // just a note - switchMapping caches results (at least the implementation should unless the
    // resolution is really a lightweight process)
    List<String> racks = switchMapping.resolve(Collections.singletonList(server.getHostname()));
    if (racks != null && !racks.isEmpty()) {
      return racks.get(0);
    }

    return UNKNOWN_RACK;
  }

  /**
   * Same as {@link #getRack(ServerName)} except that a list is passed
   * @param servers list of servers we're requesting racks information for
   * @return list of racks for the given list of servers
   */
  public List<String> getRack(List<ServerName> servers) {
    // just a note - switchMapping caches results (at least the implementation should unless the
    // resolution is really a lightweight process)
    List<String> serversAsString = new ArrayList<>(servers.size());
    for (ServerName server : servers) {
      serversAsString.add(server.getHostname());
    }
    List<String> racks = switchMapping.resolve(serversAsString);
    return racks;
  }
}
