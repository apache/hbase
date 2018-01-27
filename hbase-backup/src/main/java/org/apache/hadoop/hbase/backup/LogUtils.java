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
package org.apache.hadoop.hbase.backup;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Utility class for disabling Zk and client logging
 *
 */
@InterfaceAudience.Private
final class LogUtils {
  private LogUtils() {
  }

  /**
   * Disables Zk- and HBase client logging
   */
  static void disableZkAndClientLoggers() {
    // disable zookeeper log to avoid it mess up command output
    Logger zkLogger = LogManager.getLogger("org.apache.zookeeper");
    zkLogger.setLevel(Level.OFF);
    // disable hbase zookeeper tool log to avoid it mess up command output
    Logger hbaseZkLogger = LogManager.getLogger("org.apache.hadoop.hbase.zookeeper");
    hbaseZkLogger.setLevel(Level.OFF);
    // disable hbase client log to avoid it mess up command output
    Logger hbaseClientLogger = LogManager.getLogger("org.apache.hadoop.hbase.client");
    hbaseClientLogger.setLevel(Level.OFF);
  }
}
