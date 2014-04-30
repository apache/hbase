/**
 * Copyright 2010 The Apache Software Foundation
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;

import java.net.SocketException;
import java.util.Map.Entry;
import java.util.Properties;

/**
 * Tool for reading ZooKeeper servers from HBase XML configuation and producing
 * a line-by-line list for use by bash scripts.
 */
public class ZKServerTool {
  /**
   * Run the tool.
   * @param args Command line arguments. First arg is path to zookeepers file.
   */
  public static void main(String args[]) throws SocketException {
    Configuration conf = HBaseConfiguration.create();
    // Note that we do not simply grab the property
    // HConstants.ZOOKEEPER_QUORUM from the HBaseConfiguration because the
    // user may be using a zoo.cfg file.
    Properties zkProps = HQuorumPeer.makeZKProps(conf);
    for (Entry<Object, Object> entry : zkProps.entrySet()) {
      String key = entry.getKey().toString().trim();
      String value = entry.getValue().toString().trim();
      if (key.startsWith("server.")) {
        /* According to QuorumPeerConfig$parseProperties
         * Treat an address contained with [ ] as an IPv6 address if it
         * contains only hex digits and colons. IPv6 addresses will
         * recognized only if specified in this format.
         */
        boolean ipv6 = value.matches("\\[[0-9a-fA-F:]*\\].*");
        String parts[];
        if (ipv6) {
          String blocks[] = value.split("]");
          String ipv6Address = blocks[0].substring(1);
          parts = blocks[1].split(":");
          // The first element in "parts" should be the IP address.
          parts[0] = ipv6Address;
        } else {
          parts = value.split(":");
        }

        String host = parts[0];
        System.out.println(host);
      }
    }
  }
}
