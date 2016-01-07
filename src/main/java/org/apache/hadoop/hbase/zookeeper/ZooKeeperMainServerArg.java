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

/**
 * Tool for reading a ZooKeeper server from HBase XML configuration producing
 * the '-server host:port' argument to pass ZooKeeperMain.  This program
 * emits either '-server HOST:PORT" where HOST is one of the zk ensemble
 * members plus zk client port OR it emits '' if no zk servers found (Yes,
 * it emits '-server' too).
 */
public class ZooKeeperMainServerArg {
  public String parse(final Configuration c) {
    // Note that we do not simply grab the property
    // HConstants.ZOOKEEPER_QUORUM from the HBaseConfiguration because the
    // user may be using a zoo.cfg file.
    Properties zkProps = ZKConfig.makeZKProps(c);
    String host = null;
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
    if (hosts.isEmpty() || clientPort == null)
      return null;
    for (int i = 0; i < hosts.size(); i++) {
      if (i > 0)
        host += "," + hosts.get(i);
      else
        host = hosts.get(i);
    }
    return host != null ? host + ":" + clientPort : null;
  }

  /**
   * Run the tool.
   * @param args Command line arguments. First arg is path to zookeepers file.
   */
  public static void main(String args[]) {
    Configuration conf = HBaseConfiguration.create();
    String hostport = new ZooKeeperMainServerArg().parse(conf);
    System.out.println((hostport == null || hostport.length() == 0)? "":
      "-server " + hostport);
  }
}