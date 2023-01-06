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
package org.apache.hadoop.hbase.master.replication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AbstractClientScanner;
import org.apache.hadoop.hbase.client.ClientSideRegionScanner;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationGroupOffset;
import org.apache.hadoop.hbase.replication.ReplicationQueueData;
import org.apache.hadoop.hbase.replication.ReplicationQueueId;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.TableReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.ZKReplicationQueueStorageForMigration;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;

public class OfflineTableReplicationQueueStorage extends AbstractClientScanner
  implements ReplicationQueueStorage {

  private final Configuration conf;
  private final FileSystem fs;
  private final Path rootDir;
  private final TableDescriptor tableDesc;

  private ArrayList<RegionInfo> regions;
  private ClientSideRegionScanner currentRegionScanner = null;
  private int currentRegion = -1;
  private Scan scan;

  public OfflineTableReplicationQueueStorage(Configuration conf, TableName tableName)
    throws IOException {
    this.conf = conf;
    rootDir = CommonFSUtils.getRootDir(conf);
    Path tableDir = CommonFSUtils.getTableDir(rootDir, tableName);
    fs = tableDir.getFileSystem(conf);
    FileStatus[] regionDirs =
      CommonFSUtils.listStatus(fs, tableDir, new FSUtils.RegionDirFilter(fs));
    if (regionDirs != null) {
      regions = new ArrayList<>(regionDirs.length);
      for (int i = 0; i < regionDirs.length; ++i) {
        RegionInfo hri = HRegionFileSystem.loadRegionInfoFileContent(fs, regionDirs[i].getPath());
        regions.add(hri);
      }
    }

    tableDesc = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(TableReplicationQueueStorage.QUEUE_FAMILY))
      .setColumnFamily(
        ColumnFamilyDescriptorBuilder.of(TableReplicationQueueStorage.LAST_SEQUENCE_ID_FAMILY))
      .setColumnFamily(
        ColumnFamilyDescriptorBuilder.of(TableReplicationQueueStorage.HFILE_REF_FAMILY))
      .build();
  }

  @Override
  public void setOffset(ReplicationQueueId queueId, String walGroup, ReplicationGroupOffset offset,
    Map<String, Long> lastSeqIds) throws ReplicationException {

  }

  @Override
  public Map<String, ReplicationGroupOffset> getOffsets(ReplicationQueueId queueId)
    throws ReplicationException {
    return null;
  }

  @Override
  public List<ReplicationQueueId> listAllQueueIds(String peerId) throws ReplicationException {
    return null;
  }

  @Override
  public List<ReplicationQueueId> listAllQueueIds(ServerName serverName)
    throws ReplicationException {
    return null;
  }

  @Override
  public List<ReplicationQueueId> listAllQueueIds(String peerId, ServerName serverName)
    throws ReplicationException {
    return null;
  }

  @Override
  public List<ReplicationQueueData> listAllQueues() throws ReplicationException {
    return null;
  }

  @Override
  public List<ServerName> listAllReplicators() throws ReplicationException {
    return null;
  }

  @Override
  public Map<String, ReplicationGroupOffset> claimQueue(ReplicationQueueId queueId,
    ServerName targetServerName) throws ReplicationException {
    return null;
  }

  @Override
  public void removeQueue(ReplicationQueueId queueId) throws ReplicationException {

  }

  @Override
  public void removeAllQueues(String peerId) throws ReplicationException {

  }

  @Override
  public long getLastSequenceId(String encodedRegionName, String peerId)
    throws ReplicationException {
    return 0;
  }

  @Override
  public void setLastSequenceIds(String peerId, Map<String, Long> lastSeqIds)
    throws ReplicationException {

  }

  @Override
  public void removeLastSequenceIds(String peerId) throws ReplicationException {

  }

  @Override
  public void removeLastSequenceIds(String peerId, List<String> encodedRegionNames)
    throws ReplicationException {

  }

  @Override
  public void removePeerFromHFileRefs(String peerId) throws ReplicationException {

  }

  @Override
  public void addHFileRefs(String peerId, List<Pair<Path, Path>> pairs)
    throws ReplicationException {

  }

  @Override
  public void removeHFileRefs(String peerId, List<String> files) throws ReplicationException {

  }

  @Override
  public List<String> getAllPeersFromHFileRefsQueue() throws ReplicationException {
    return null;
  }

  @Override
  public List<String> getReplicableHFiles(String peerId) throws ReplicationException {
    return null;
  }

  @Override
  public Set<String> getAllHFileRefs() throws ReplicationException {
    return null;
  }

  @Override
  public boolean hasData() throws ReplicationException {
    return false;
  }

  @Override
  public void batchUpdateQueues(ServerName serverName, List<ReplicationQueueData> datas)
    throws ReplicationException {

  }

  @Override
  public void batchUpdateLastSequenceIds(
    List<ZKReplicationQueueStorageForMigration.ZkLastPushedSeqId> lastPushedSeqIds)
    throws ReplicationException {

  }

  @Override
  public void batchUpdateHFileRefs(String peerId, List<String> hfileRefs)
    throws ReplicationException {

  }

  @Override
  public Result next() throws IOException {
    Result result = null;
    while (true) {
      if (currentRegionScanner == null) {
        currentRegion++;
        if (currentRegion >= regions.size()) {
          return null;
        }

        RegionInfo hri = regions.get(currentRegion);

        currentRegionScanner =
          new ClientSideRegionScanner(conf, fs, rootDir, tableDesc, hri, scan, scanMetrics);
        if (this.scanMetrics != null) {
          this.scanMetrics.countOfRegions.incrementAndGet();
        }
      }

      try {
        result = currentRegionScanner.next();
        if (result != null) {
          return result;
        }
      } finally {
        if (result == null) {
          currentRegionScanner.close();
          currentRegionScanner = null;
        }
      }
    }
  }

  @Override
  public void close() {

  }

  @Override
  public boolean renewLease() {
    return false;
  }
}
