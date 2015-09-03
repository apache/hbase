/**
 *
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

package org.apache.hadoop.hbase.zookeeper;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;

/**
 * Tool for reading ZooKeeper servers from HBase XML configuration and producing
 * a line-by-line list for use by bash scripts.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
public class ZKServerTool {

  public static ServerName[] readZKNodes(Configuration conf) {
    List<ServerName> hosts = new LinkedList<ServerName>();

    // Note that we do not simply grab the property
    // HConstants.ZOOKEEPER_QUORUM from the HBaseConfiguration because the
    // user may be using a zoo.cfg file.
    Properties zkProps = ZKConfig.makeZKProps(conf);
    for (Entry<Object, Object> entry : zkProps.entrySet()) {
      String key = entry.getKey().toString().trim();
      String value = entry.getValue().toString().trim();
      if (key.startsWith("server.")) {
        String[] parts = value.split(":");
        String host = parts[0];

        int port = HConstants.DEFAULT_ZOOKEPER_CLIENT_PORT;
        if (parts.length > 1) {
          port = Integer.parseInt(parts[1]);
        }
        hosts.add(ServerName.valueOf(host, port, -1));
      }
    }
    return hosts.toArray(new ServerName[hosts.size()]);
  }

  /**
   * Run the tool.
   * @param args Command line arguments.
   */
  public static void main(String args[]) {
    for(ServerName server: readZKNodes(HBaseConfiguration.create())) {
      System.out.println("Zk host: " + server.getHostname());
    }
  }
}
