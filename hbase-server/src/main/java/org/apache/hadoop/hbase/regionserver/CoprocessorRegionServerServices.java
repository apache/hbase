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
package org.apache.hadoop.hbase.regionserver;

import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Services exposed to CPs by {@link HRegionServer}
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public interface CoprocessorRegionServerServices extends ImmutableOnlineRegions {

  /**
   * @return True if this regionserver is stopping.
   */
  boolean isStopping();

  /**
   * @return Return the FileSystem object used by the regionserver
   */
  FileSystem getFileSystem();

  /**
   * @return all the online tables in this RS
   */
  Set<TableName> getOnlineTables();

  /**
   * Returns a reference to the servers' connection.
   *
   * Important note: this method returns a reference to Connection which is managed
   * by Server itself, so callers must NOT attempt to close connection obtained.
   */
  Connection getConnection();

  /**
   * @return The unique server name for this server.
   */
  ServerName getServerName();
}