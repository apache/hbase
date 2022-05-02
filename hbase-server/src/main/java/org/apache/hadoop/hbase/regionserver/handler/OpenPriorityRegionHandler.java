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
package org.apache.hadoop.hbase.regionserver.handler;

import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Handles opening of a high priority region on a region server.
 * <p>
 * This is executed after receiving an OPEN RPC from the master or client.
 */
@InterfaceAudience.Private
public class OpenPriorityRegionHandler extends OpenRegionHandler {
  public OpenPriorityRegionHandler(Server server, RegionServerServices rsServices,
    RegionInfo regionInfo, TableDescriptor htd, long masterSystemTime) {
    super(server, rsServices, regionInfo, htd, masterSystemTime,
      EventType.M_RS_OPEN_PRIORITY_REGION);
  }
}
