/*
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
package org.apache.hadoop.hbase.util;

import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Utility used by both Master and Region Server web UI JSP pages.
 */
@InterfaceAudience.Private
public final class ZKStringFormatter {

  private ZKStringFormatter() {
    // Do not instantiate.
  }

  public static String formatZKString(ZKWatcher zookeeper) {
    StringBuilder quorums = new StringBuilder();
    String zkQuorum = zookeeper.getQuorum();

    if (null == zkQuorum) {
      return quorums.toString();
    }

    String[] zks = zkQuorum.split(",");

    if (zks.length == 0) {
      return quorums.toString();
    }

    for (int i = 0; i < zks.length; ++i) {
      quorums.append(zks[i].trim());

      if (i != (zks.length - 1)) {
        quorums.append("<br/>");
      }
    }

    return quorums.toString();
  }
}
