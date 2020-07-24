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

package org.apache.hadoop.hbase.client;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Utility methods which contain the logic for regions and replicas.
 */
@InterfaceAudience.Private
public class RegionReplicaUtil {

  /**
   * Whether or not the secondary region will wait for observing a flush / region open event
   * from the primary region via async wal replication before enabling read requests. Since replayed
   * edits from async wal replication from primary is not persisted in WAL, the memstore of the
   * secondary region might be non-empty at the time of close or crash. For ensuring seqId's not
   * "going back in time" in the secondary region replica, this should be enabled. However, in some
   * cases the above semantics might be ok for some application classes.
   * See HBASE-11580 for more context.
   */
  public static final String REGION_REPLICA_WAIT_FOR_PRIMARY_FLUSH_CONF_KEY
    = "hbase.region.replica.wait.for.primary.flush";
  protected static final boolean DEFAULT_REGION_REPLICA_WAIT_FOR_PRIMARY_FLUSH = true;

  /**
   * The default replicaId for the region
   */
  static final int DEFAULT_REPLICA_ID = 0;

  /**
   * Returns the RegionInfo for the given replicaId.
   * RegionInfo's correspond to a range of a table, but more than one
   * "instance" of the same range can be deployed which are differentiated by
   * the replicaId.
   * @param regionInfo
   * @param replicaId the replicaId to use
   * @return an RegionInfo object corresponding to the same range (table, start and
   * end key), but for the given replicaId.
   */
  public static RegionInfo getRegionInfoForReplica(RegionInfo regionInfo, int replicaId) {
    if (regionInfo.getReplicaId() == replicaId) {
      return regionInfo;
    }
    return RegionInfoBuilder.newBuilder(regionInfo).setReplicaId(replicaId).build();
  }

  /**
   * Returns the RegionInfo for the default replicaId (0). RegionInfo's correspond to
   * a range of a table, but more than one "instance" of the same range can be
   * deployed which are differentiated by the replicaId.
   * @return an RegionInfo object corresponding to the same range (table, start and
   * end key), but for the default replicaId.
   */
  public static RegionInfo getRegionInfoForDefaultReplica(RegionInfo regionInfo) {
    return getRegionInfoForReplica(regionInfo, DEFAULT_REPLICA_ID);
  }

  /** @return true if this replicaId corresponds to default replica for the region */
  public static boolean isDefaultReplica(int replicaId) {
    return DEFAULT_REPLICA_ID == replicaId;
  }

  /** @return true if this region is a default replica for the region */
  public static boolean isDefaultReplica(RegionInfo hri) {
    return  hri.getReplicaId() == DEFAULT_REPLICA_ID;
  }

  /**
   * Removes the non-default replicas from the passed regions collection
   * @param regions
   */
  public static void removeNonDefaultRegions(Collection<RegionInfo> regions) {
    Iterator<RegionInfo> iterator = regions.iterator();
    while (iterator.hasNext()) {
      RegionInfo hri = iterator.next();
      if (!RegionReplicaUtil.isDefaultReplica(hri)) {
        iterator.remove();
      }
    }
  }

  public static boolean isReplicasForSameRegion(RegionInfo regionInfoA, RegionInfo regionInfoB) {
    return compareRegionInfosWithoutReplicaId(regionInfoA, regionInfoB) == 0;
  }

  private static int compareRegionInfosWithoutReplicaId(RegionInfo regionInfoA,
      RegionInfo regionInfoB) {
    int result = regionInfoA.getTable().compareTo(regionInfoB.getTable());
    if (result != 0) {
      return result;
    }

    // Compare start keys.
    result = Bytes.compareTo(regionInfoA.getStartKey(), regionInfoB.getStartKey());
    if (result != 0) {
      return result;
    }

    // Compare end keys.
    result = Bytes.compareTo(regionInfoA.getEndKey(), regionInfoB.getEndKey());

    if (result != 0) {
      if (regionInfoA.getStartKey().length != 0
              && regionInfoA.getEndKey().length == 0) {
          return 1; // this is last region
      }
      if (regionInfoB.getStartKey().length != 0
              && regionInfoB.getEndKey().length == 0) {
          return -1; // o is the last region
      }
      return result;
    }

    // regionId is usually milli timestamp -- this defines older stamps
    // to be "smaller" than newer stamps in sort order.
    if (regionInfoA.getRegionId() > regionInfoB.getRegionId()) {
      return 1;
    } else if (regionInfoA.getRegionId() < regionInfoB.getRegionId()) {
      return -1;
    }
    return 0;
  }

  /**
   * Create any replicas for the regions (the default replicas that was already created is passed to
   * the method)
   * @param regions existing regions
   * @param oldReplicaCount existing replica count
   * @param newReplicaCount updated replica count due to modify table
   * @return the combined list of default and non-default replicas
   */
  public static List<RegionInfo> addReplicas(final List<RegionInfo> regions, int oldReplicaCount,
    int newReplicaCount) {
    if ((newReplicaCount - 1) <= 0) {
      return regions;
    }
    List<RegionInfo> hRegionInfos = new ArrayList<>((newReplicaCount) * regions.size());
    for (RegionInfo ri : regions) {
      if (RegionReplicaUtil.isDefaultReplica(ri) &&
        (!ri.isOffline() || (!ri.isSplit() && !ri.isSplitParent()))) {
        // region level replica index starts from 0. So if oldReplicaCount was 2 then the max replicaId for
        // the existing regions would be 1
        for (int j = oldReplicaCount; j < newReplicaCount; j++) {
          hRegionInfos.add(RegionReplicaUtil.getRegionInfoForReplica(ri, j));
        }
      }
    }
    hRegionInfos.addAll(regions);
    return hRegionInfos;
  }
}
