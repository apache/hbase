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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.master.assignment.ServerState;
import org.apache.hadoop.hbase.master.region.MasterRegion;
import org.apache.hadoop.hbase.master.region.MasterRegionFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link MasterRegion} based {@link RegionServerList}.
 * <p/>
 * This is useful when we want to restart a cluster with only the data on file system, as when
 * restarting, we need to get the previous live region servers for scheduling SCP. Before we have
 * this class, we need to scan the WAL directory on WAL file system to find out the previous live
 * region servers, which means we can not restart a cluster without the previous WAL file system,
 * even if we have flushed all the data.
 * <p/>
 * Please see HBASE-26245 for more details.
 */
@InterfaceAudience.Private
public class MasterRegionServerList implements RegionServerList {

  private static final Logger LOG = LoggerFactory.getLogger(MasterRegionServerList.class);

  private final MasterRegion region;

  private final Abortable abortable;

  public MasterRegionServerList(MasterRegion region, Abortable abortable) {
    this.region = region;
    this.abortable = abortable;
  }

  @Override
  public void started(ServerName sn) {
    Put put =
      new Put(Bytes.toBytes(sn.getServerName())).addColumn(MasterRegionFactory.REGION_SERVER_FAMILY,
        HConstants.STATE_QUALIFIER, Bytes.toBytes(ServerState.ONLINE.name()));
    try {
      region.update(r -> r.put(put));
    } catch (IOException e) {
      LOG.error(HBaseMarkers.FATAL, "Failed to record region server {} as started, aborting...", sn,
        e);
      abortable.abort("Failed to record region server as started");
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void expired(ServerName sn) {
    Delete delete = new Delete(Bytes.toBytes(sn.getServerName()))
      .addFamily(MasterRegionFactory.REGION_SERVER_FAMILY);
    try {
      region.update(r -> r.delete(delete));
    } catch (IOException e) {
      LOG.error(HBaseMarkers.FATAL, "Failed to record region server {} as expired, aborting...", sn,
        e);
      abortable.abort("Failed to record region server as expired");
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public Set<ServerName> getAll() throws IOException {
    Set<ServerName> rsList = new HashSet<>();
    try (ResultScanner scanner =
      region.getScanner(new Scan().addFamily(MasterRegionFactory.REGION_SERVER_FAMILY))) {
      for (;;) {
        Result result = scanner.next();
        if (result == null) {
          break;
        }
        rsList.add(ServerName.valueOf(Bytes.toString(result.getRow())));
      }
    }
    return rsList;
  }

}
