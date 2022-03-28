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

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * ChaosUtils holds a bunch of useful functions like getting hostname and getting ZooKeeper quorum.
 */
@InterfaceAudience.Private
public class ChaosUtils {

  public static String getHostName() throws UnknownHostException {
    return InetAddress.getLocalHost().getHostName();
  }


  public static String getZKQuorum(Configuration conf) {
    String port =
      Integer.toString(conf.getInt(HConstants.ZOOKEEPER_CLIENT_PORT, 2181));
    String[] serverHosts = conf.getStrings(HConstants.ZOOKEEPER_QUORUM, "localhost");
    for (int i = 0; i < serverHosts.length; i++) {
      serverHosts[i] = serverHosts[i] + ":" + port;
    }
    return String.join(",", serverHosts);
  }

}
