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
package org.apache.hadoop.hbase.master;

import java.io.IOException;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;

/**
 * Get notification of assignment events. The invocations are inline
 * so make sure your implementation is fast else you'll slow hbase.
 */
@InterfaceAudience.Private
public interface AssignmentListener {
  /**
   * The region was opened on the specified server.
   * @param regionInfo The opened region.
   * @param serverName The remote servers name.
   */
  void regionOpened(final HRegionInfo regionInfo, final ServerName serverName);

  /**
   * The region was closed on the region server.
   * @param regionInfo The closed region.
   * @param serverName The remote servers name.
   */
  void regionClosed(final HRegionInfo regionInfo);
}
