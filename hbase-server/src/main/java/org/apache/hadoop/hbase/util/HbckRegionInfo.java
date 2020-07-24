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
package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Joiner;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * Maintain information about a particular region.  It gathers information
 * from three places -- HDFS, META, and region servers.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class HbckRegionInfo implements KeyRange {
  private static final Logger LOG = LoggerFactory.getLogger(HbckRegionInfo.class.getName());

  private MetaEntry metaEntry = null; // info in META
  private HdfsEntry hdfsEntry = null; // info in HDFS
  private List<OnlineEntry> deployedEntries = Lists.newArrayList(); // on Region Server
  private List<ServerName> deployedOn = Lists.newArrayList(); // info on RS's
  private boolean skipChecks = false; // whether to skip further checks to this region info.
  private boolean isMerged = false;// whether this region has already been merged into another one
  private int deployedReplicaId = RegionInfo.DEFAULT_REPLICA_ID;
  private RegionInfo primaryHRIForDeployedReplica = null;

  public HbckRegionInfo(MetaEntry metaEntry) {
    this.metaEntry = metaEntry;
  }

  public synchronized int getReplicaId() {
    return metaEntry != null? metaEntry.getReplicaId(): deployedReplicaId;
  }

  public synchronized void addServer(RegionInfo regionInfo, ServerName serverName) {
    OnlineEntry rse = new OnlineEntry(regionInfo, serverName) ;
    this.deployedEntries.add(rse);
    this.deployedOn.add(serverName);
    // save the replicaId that we see deployed in the cluster
    this.deployedReplicaId = regionInfo.getReplicaId();
    this.primaryHRIForDeployedReplica =
        RegionReplicaUtil.getRegionInfoForDefaultReplica(regionInfo);
  }

  @Override
  public synchronized String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{ meta => ");
    sb.append((metaEntry != null)? metaEntry.getRegionNameAsString() : "null");
    sb.append(", hdfs => " + getHdfsRegionDir());
    sb.append(", deployed => " + Joiner.on(", ").join(deployedEntries));
    sb.append(", replicaId => " + getReplicaId());
    sb.append(" }");
    return sb.toString();
  }

  @Override
  public byte[] getStartKey() {
    if (this.metaEntry != null) {
      return this.metaEntry.getStartKey();
    } else if (this.hdfsEntry != null) {
      return this.hdfsEntry.hri.getStartKey();
    } else {
      LOG.error("Entry " + this + " has no meta or hdfs region start key.");
      return null;
    }
  }

  @Override
  public byte[] getEndKey() {
    if (this.metaEntry != null) {
      return this.metaEntry.getEndKey();
    } else if (this.hdfsEntry != null) {
      return this.hdfsEntry.hri.getEndKey();
    } else {
      LOG.error("Entry " + this + " has no meta or hdfs region start key.");
      return null;
    }
  }

  public MetaEntry getMetaEntry() {
    return this.metaEntry;
  }

  public void setMetaEntry(MetaEntry metaEntry) {
    this.metaEntry = metaEntry;
  }

  public HdfsEntry getHdfsEntry() {
    return this.hdfsEntry;
  }

  public void setHdfsEntry(HdfsEntry hdfsEntry) {
    this.hdfsEntry = hdfsEntry;
  }

  public List<OnlineEntry> getOnlineEntries() {
    return this.deployedEntries;
  }

  public List<ServerName> getDeployedOn() {
    return this.deployedOn;
  }

  /**
   * Read the .regioninfo file from the file system.  If there is no
   * .regioninfo, add it to the orphan hdfs region list.
   */
  public void loadHdfsRegioninfo(Configuration conf) throws IOException {
    Path regionDir = getHdfsRegionDir();
    if (regionDir == null) {
      if (getReplicaId() == RegionInfo.DEFAULT_REPLICA_ID) {
        // Log warning only for default/ primary replica with no region dir
        LOG.warn("No HDFS region dir found: " + this + " meta=" + metaEntry);
      }
      return;
    }

    if (hdfsEntry.hri != null) {
      // already loaded data
      return;
    }

    FileSystem fs = FileSystem.get(conf);
    RegionInfo hri = HRegionFileSystem.loadRegionInfoFileContent(fs, regionDir);
    LOG.debug("RegionInfo read: " + hri.toString());
    hdfsEntry.hri = hri;
  }

  public TableName getTableName() {
    if (this.metaEntry != null) {
      return this.metaEntry.getTable();
    } else if (this.hdfsEntry != null) {
      // we are only guaranteed to have a path and not an HRI for hdfsEntry,
      // so we get the name from the Path
      Path tableDir = this.hdfsEntry.regionDir.getParent();
      return CommonFSUtils.getTableName(tableDir);
    } else {
      // return the info from the first online/deployed hri
      for (OnlineEntry e : deployedEntries) {
        return e.getRegionInfo().getTable();
      }
      return null;
    }
  }

  public String getRegionNameAsString() {
    if (metaEntry != null) {
      return metaEntry.getRegionNameAsString();
    } else if (hdfsEntry != null) {
      if (hdfsEntry.hri != null) {
        return hdfsEntry.hri.getRegionNameAsString();
      }
    } else {
      // return the info from the first online/deployed hri
      for (OnlineEntry e : deployedEntries) {
        return e.getRegionInfo().getRegionNameAsString();
      }
    }
    return null;
  }

  public byte[] getRegionName() {
    if (metaEntry != null) {
      return metaEntry.getRegionName();
    } else if (hdfsEntry != null) {
      return hdfsEntry.hri.getRegionName();
    } else {
      // return the info from the first online/deployed hri
      for (OnlineEntry e : deployedEntries) {
        return e.getRegionInfo().getRegionName();
      }
      return null;
    }
  }

  public RegionInfo getPrimaryHRIForDeployedReplica() {
    return primaryHRIForDeployedReplica;
  }

  public Path getHdfsRegionDir() {
    if (hdfsEntry == null) {
      return null;
    }
    return hdfsEntry.regionDir;
  }

  public boolean containsOnlyHdfsEdits() {
    if (hdfsEntry == null) {
      return false;
    }
    return hdfsEntry.hdfsOnlyEdits;
  }

  public boolean isHdfsRegioninfoPresent() {
    if (hdfsEntry == null) {
      return false;
    }
    return hdfsEntry.hdfsRegioninfoFilePresent;
  }

  public long getModTime() {
    if (hdfsEntry == null) {
      return 0;
    }
    return hdfsEntry.regionDirModTime;
  }

  public RegionInfo getHdfsHRI() {
    if (hdfsEntry == null) {
      return null;
    }
    return hdfsEntry.hri;
  }

  public void setSkipChecks(boolean skipChecks) {
    this.skipChecks = skipChecks;
  }

  public boolean isSkipChecks() {
    return skipChecks;
  }

  public void setMerged(boolean isMerged) {
    this.isMerged = isMerged;
  }

  public boolean isMerged() {
    return this.isMerged;
  }

  /**
   * Stores the regioninfo entries scanned from META
   */
  public static class MetaEntry extends HRegionInfo {
    ServerName regionServer;   // server hosting this region
    long modTime;          // timestamp of most recent modification metadata
    RegionInfo splitA, splitB; //split daughters

    public MetaEntry(RegionInfo rinfo, ServerName regionServer, long modTime) {
      this(rinfo, regionServer, modTime, null, null);
    }

    public MetaEntry(RegionInfo rinfo, ServerName regionServer, long modTime,
        RegionInfo splitA, RegionInfo splitB) {
      super(rinfo);
      this.regionServer = regionServer;
      this.modTime = modTime;
      this.splitA = splitA;
      this.splitB = splitB;
    }

    public ServerName getRegionServer() {
      return this.regionServer;
    }

    @Override
    public boolean equals(Object o) {
      boolean superEq = super.equals(o);
      if (!superEq) {
        return superEq;
      }

      MetaEntry me = (MetaEntry) o;
      if (!regionServer.equals(me.regionServer)) {
        return false;
      }
      return (modTime == me.modTime);
    }

    @Override
    public int hashCode() {
      int hash = Arrays.hashCode(getRegionName());
      hash = (int) (hash ^ getRegionId());
      hash ^= Arrays.hashCode(getStartKey());
      hash ^= Arrays.hashCode(getEndKey());
      hash ^= Boolean.valueOf(isOffline()).hashCode();
      hash ^= getTable().hashCode();
      if (regionServer != null) {
        hash ^= regionServer.hashCode();
      }
      hash = (int) (hash ^ modTime);
      return hash;
    }
  }

  /**
   * Stores the regioninfo entries from HDFS
   */
  public static class HdfsEntry {
    RegionInfo hri;
    Path regionDir = null;
    long regionDirModTime = 0;
    boolean hdfsRegioninfoFilePresent = false;
    boolean hdfsOnlyEdits = false;

    HdfsEntry() {
    }

    public HdfsEntry(Path regionDir) {
      this.regionDir = regionDir;
    }
  }

  /**
   * Stores the regioninfo retrieved from Online region servers.
   */
  static class OnlineEntry {
    private RegionInfo regionInfo;
    private ServerName serverName;

    OnlineEntry(RegionInfo regionInfo, ServerName serverName) {
      this.regionInfo = regionInfo;
      this.serverName = serverName;
    }

    public RegionInfo getRegionInfo() {
      return regionInfo;
    }

    public ServerName getServerName() {
      return serverName;
    }

    @Override
    public String toString() {
      return serverName.toString() + ";" + regionInfo.getRegionNameAsString();
    }
  }

  final static Comparator<HbckRegionInfo> COMPARATOR = new Comparator<HbckRegionInfo>() {
    @Override
    public int compare(HbckRegionInfo l, HbckRegionInfo r) {
      if (l == r) {
        // same instance
        return 0;
      }

      int tableCompare = l.getTableName().compareTo(r.getTableName());
      if (tableCompare != 0) {
        return tableCompare;
      }

      int startComparison = RegionSplitCalculator.BYTES_COMPARATOR.compare(
          l.getStartKey(), r.getStartKey());
      if (startComparison != 0) {
        return startComparison;
      }

      // Special case for absolute endkey
      byte[] endKey = r.getEndKey();
      endKey = (endKey.length == 0) ? null : endKey;
      byte[] endKey2 = l.getEndKey();
      endKey2 = (endKey2.length == 0) ? null : endKey2;
      int endComparison = RegionSplitCalculator.BYTES_COMPARATOR.compare(
          endKey2,  endKey);

      if (endComparison != 0) {
        return endComparison;
      }

      // use regionId as tiebreaker.
      // Null is considered after all possible values so make it bigger.
      if (l.getHdfsEntry() == null && r.getHdfsEntry() == null) {
        return 0;
      }
      if (l.getHdfsEntry() == null && r.getHdfsEntry() != null) {
        return 1;
      }
      // l.hdfsEntry must not be null
      if (r.getHdfsEntry() == null) {
        return -1;
      }
      // both l.hdfsEntry and r.hdfsEntry must not be null.
      return Long.compare(l.getHdfsEntry().hri.getRegionId(), r.getHdfsEntry().hri.getRegionId());
    }
  };
}