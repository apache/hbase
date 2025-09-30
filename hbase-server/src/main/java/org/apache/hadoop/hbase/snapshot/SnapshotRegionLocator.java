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
package org.apache.hadoop.hbase.snapshot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos;

/**
 * {@link RegionLocator} built using the most recent full backup's snapshot manifest for a given
 * table. Useful for aligning any subsequent incremental backups along the same splits as the full
 * backup
 */
@InterfaceAudience.Private
public final class SnapshotRegionLocator implements RegionLocator {

  private static final String SNAPSHOT_MANIFEST_DIR_PREFIX =
    "region.locator.snapshot.manifest.dir.";

  private static final ServerName FAKE_SERVER_NAME =
    ServerName.parseServerName("www.example.net,1234,1212121212");

  private final TableName tableName;
  private final TreeMap<byte[], HRegionReplicas> regions;

  private final List<HRegionLocation> rawLocations;

  public static SnapshotRegionLocator create(Configuration conf, TableName table)
    throws IOException {
    Path workingDir = new Path(conf.get(getSnapshotManifestDirKey(table)));
    FileSystem fs = workingDir.getFileSystem(conf);
    SnapshotProtos.SnapshotDescription desc =
      SnapshotDescriptionUtils.readSnapshotInfo(fs, workingDir);
    SnapshotManifest manifest = SnapshotManifest.open(conf, fs, workingDir, desc);

    TableName tableName = manifest.getTableDescriptor().getTableName();
    TreeMap<byte[], HRegionReplicas> replicas = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    List<HRegionLocation> rawLocations = new ArrayList<>();

    for (SnapshotProtos.SnapshotRegionManifest region : manifest.getRegionManifests()) {
      HBaseProtos.RegionInfo ri = region.getRegionInfo();
      byte[] key = ri.getStartKey().toByteArray();

      if (ri.getSplit()) {
        continue;
      }

      SnapshotHRegionLocation location = toLocation(ri, tableName);
      rawLocations.add(location);
      HRegionReplicas hrr = replicas.get(key);

      if (hrr == null) {
        hrr = new HRegionReplicas(location);
      } else {
        hrr.addReplica(location);
      }

      replicas.put(key, hrr);
    }

    return new SnapshotRegionLocator(tableName, replicas, rawLocations);
  }

  private SnapshotRegionLocator(TableName tableName, TreeMap<byte[], HRegionReplicas> regions,
    List<HRegionLocation> rawLocations) {
    this.tableName = tableName;
    this.regions = regions;
    this.rawLocations = rawLocations;
  }

  @Override
  public HRegionLocation getRegionLocation(byte[] row, int replicaId, boolean reload)
    throws IOException {
    return regions.floorEntry(row).getValue().getReplica(replicaId);
  }

  @Override
  public List<HRegionLocation> getRegionLocations(byte[] row, boolean reload) throws IOException {
    return Collections.singletonList(getRegionLocation(row, reload));
  }

  @Override
  public void clearRegionLocationCache() {

  }

  @Override
  public List<HRegionLocation> getAllRegionLocations() throws IOException {
    return rawLocations;
  }

  @Override
  public TableName getName() {
    return tableName;
  }

  @Override
  public void close() throws IOException {

  }

  public static boolean shouldUseSnapshotRegionLocator(Configuration conf, TableName table) {
    return conf.get(getSnapshotManifestDirKey(table)) != null;
  }

  public static void setSnapshotManifestDir(Configuration conf, String dir, TableName table) {
    conf.set(getSnapshotManifestDirKey(table), dir);
  }

  private static String getSnapshotManifestDirKey(TableName table) {
    return SNAPSHOT_MANIFEST_DIR_PREFIX + table.getNameWithNamespaceInclAsString();
  }

  private static SnapshotHRegionLocation toLocation(HBaseProtos.RegionInfo ri,
    TableName tableName) {
    RegionInfo region = RegionInfoBuilder.newBuilder(tableName)
      .setStartKey(ri.getStartKey().toByteArray()).setEndKey(ri.getEndKey().toByteArray())
      .setRegionId(ri.getRegionId()).setReplicaId(ri.getReplicaId()).build();

    return new SnapshotHRegionLocation(region);
  }

  private static final class HRegionReplicas {
    private final Map<Integer, SnapshotHRegionLocation> replicas = new HashMap<>();

    private HRegionReplicas(SnapshotHRegionLocation region) {
      addReplica(region);
    }

    private void addReplica(SnapshotHRegionLocation replica) {
      this.replicas.put(replica.getRegion().getReplicaId(), replica);
    }

    private HRegionLocation getReplica(int replicaId) {
      return replicas.get(replicaId);
    }
  }

  public static final class SnapshotHRegionLocation extends HRegionLocation {

    public SnapshotHRegionLocation(RegionInfo regionInfo) {
      super(regionInfo, FAKE_SERVER_NAME);
    }

    @Override
    public ServerName getServerName() {
      throw new NotImplementedException("SnapshotHRegionLocation doesn't have a server name");
    }

    @Override
    public String getHostname() {
      throw new NotImplementedException("SnapshotHRegionLocation doesn't have a host name");
    }

    @Override
    public int getPort() {
      throw new NotImplementedException("SnapshotHRegionLocation doesn't have a port");
    }

    @Override
    public int hashCode() {
      return this.getRegion().hashCode();
    }

    @Override
    public boolean equals(Object o) {
      return super.equals(o);
    }

    @Override
    public int compareTo(HRegionLocation o) {
      return this.getRegion().compareTo(o.getRegion());
    }
  }
}
