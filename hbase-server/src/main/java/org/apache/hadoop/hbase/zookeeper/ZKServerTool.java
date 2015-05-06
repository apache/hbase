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

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Tool for reading ZooKeeper servers from HBase XML configuration and producing
 * a line-by-line list for use by bash scripts.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
public class ZKServerTool {
  /**
   * Run the tool.
   * @param args Command line arguments.
   */
  public static void main(String args[]) {
    Configuration conf = HBaseConfiguration.create();
    Properties zkProps = ZKConfig.makeZKProps(conf);
    String quorum = zkProps.getProperty(HConstants.ZOOKEEPER_QUORUM);

    String[] values = quorum.split(",");
    for (String value : values) {
      String[] parts = value.split(":");
      String host = parts[0];
      System.out.println("ZK host:" + host);
    }

  }
}
