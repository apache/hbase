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

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.zookeeper.ZooKeeperMain;

/**
 * Tool for running ZookeeperMain from HBase by  reading a ZooKeeper server
 * from HBase XML configuration.
 */
public class ZooKeeperMainServer {
  public String parse(final Configuration c) {
    // Note that we do not simply grab the property
    // HConstants.ZOOKEEPER_QUORUM from the HBaseConfiguration because the
    // user may be using a zoo.cfg file.
    Properties zkProps = ZKConfig.makeZKProps(c);
    String clientPort = null;
    List<String> hosts = new ArrayList<String>();
    for (Entry<Object, Object> entry: zkProps.entrySet()) {
      String key = entry.getKey().toString().trim();
      String value = entry.getValue().toString().trim();
      if (key.startsWith("server.")) {
        String[] parts = value.split(":");
        hosts.add(parts[0]);
      } else if (key.endsWith("clientPort")) {
        clientPort = value;
      }
    }
    if (hosts.isEmpty() || clientPort == null) return null;
    StringBuilder host = new StringBuilder();
    for (int i = 0; i < hosts.size(); i++) {
      if (i > 0)  host.append("," + hosts.get(i));
      else host.append(hosts.get(i));
      host.append(":");
      host.append(clientPort);
    }
    return host.toString();
  }

  /**
   * Run the tool.
   * @param args Command line arguments. First arg is path to zookeepers file.
   */
  public static void main(String args[]) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    String hostport = new ZooKeeperMainServer().parse(conf);
    String[] newArgs = args;
    if (hostport != null && hostport.length() > 0) {
      newArgs = new String[args.length + 2];
      System.arraycopy(args, 0, newArgs, 2, args.length);
      newArgs[0] = "-server";
      newArgs[1] = hostport;
    }
    ZooKeeperMain.main(newArgs);
  }
}