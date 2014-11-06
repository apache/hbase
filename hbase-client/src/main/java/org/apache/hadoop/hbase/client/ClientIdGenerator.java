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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.lang.management.ManagementFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * The class that is able to determine some unique strings for the client,
 * such as an IP address, PID, and composite deterministic ID.
 */
@InterfaceAudience.Private
class ClientIdGenerator {
  static final Log LOG = LogFactory.getLog(ClientIdGenerator.class);

  /**
   * @return a unique ID incorporating IP address, PID, TID and timer. Might be an overkill...
   * Note though that new UUID in java by default is just a random number.
   */
  public static byte[] generateClientId() {
    byte[] selfBytes = getIpAddressBytes();
    Long pid = getPid();
    long tid = Thread.currentThread().getId();
    long ts = System.currentTimeMillis();

    byte[] id = new byte[selfBytes.length + ((pid != null ? 1 : 0) + 2) * Bytes.SIZEOF_LONG];
    int offset = Bytes.putBytes(id, 0, selfBytes, 0, selfBytes.length);
    if (pid != null) {
      offset = Bytes.putLong(id, offset, pid);
    }
    offset = Bytes.putLong(id, offset, tid);
    offset = Bytes.putLong(id, offset, ts);
    assert offset == id.length;
    return id;
  }

  /**
   * @return PID of the current process, if it can be extracted from JVM name, or null.
   */
  public static Long getPid() {
    String name = ManagementFactory.getRuntimeMXBean().getName();
    String[] nameParts = name.split("@");
    if (nameParts.length == 2) { // 12345@somewhere
      try {
        return Long.parseLong(nameParts[0]);
      } catch (NumberFormatException ex) {
        LOG.warn("Failed to get PID from [" + name + "]", ex);
      }
    } else {
      LOG.warn("Don't know how to get PID from [" + name + "]");
    }
    return null;
  }

  /**
   * @return Some IPv4/IPv6 address available on the current machine that is up, not virtual
   *         and not a loopback address. Empty array if none can be found or error occured.
   */
  public static byte[] getIpAddressBytes() {
    try {
      return Addressing.getIpAddress().getAddress();
    } catch (IOException ex) {
      LOG.warn("Failed to get IP address bytes", ex);
    }
    return new byte[0];
  }
}
