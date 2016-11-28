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

package org.apache.hadoop.hbase.snapshot;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Describe the set of operations needed to update hbase:meta after restore.
 */
public class SnapshotRestoreMetaChanges {
  private static final Log LOG = LogFactory.getLog(SnapshotRestoreMetaChanges.class);

  private final Map<String, Pair<String, String> > parentsMap;
  private final HTableDescriptor htd;

  private List<HRegionInfo> regionsToRestore = null;
  private List<HRegionInfo> regionsToRemove = null;
  private List<HRegionInfo> regionsToAdd = null;

  public SnapshotRestoreMetaChanges(HTableDescriptor htd, Map<String,
      Pair<String, String> > parentsMap) {
    this.parentsMap = parentsMap;
    this.htd = htd;
  }

  public HTableDescriptor getTableDescriptor() {
    return htd;
  }

  /**
   * Returns the map of parent-children_pair.
   * @return the map
   */
  public Map<String, Pair<String, String>> getParentToChildrenPairMap() {
    return this.parentsMap;
  }

  /**
   * @return true if there're new regions
   */
  public boolean hasRegionsToAdd() {
    return this.regionsToAdd != null && this.regionsToAdd.size() > 0;
  }

  /**
   * Returns the list of new regions added during the on-disk restore.
   * The caller is responsible to add the regions to META.
   * e.g MetaTableAccessor.addRegionsToMeta(...)
   * @return the list of regions to add to META
   */
  public List<HRegionInfo> getRegionsToAdd() {
    return this.regionsToAdd;
  }

  /**
   * @return true if there're regions to restore
   */
  public boolean hasRegionsToRestore() {
    return this.regionsToRestore != null && this.regionsToRestore.size() > 0;
  }

  /**
   * Returns the list of 'restored regions' during the on-disk restore.
   * The caller is responsible to add the regions to hbase:meta if not present.
   * @return the list of regions restored
   */
  public List<HRegionInfo> getRegionsToRestore() {
    return this.regionsToRestore;
  }

  /**
   * @return true if there're regions to remove
   */
  public boolean hasRegionsToRemove() {
    return this.regionsToRemove != null && this.regionsToRemove.size() > 0;
  }

  /**
   * Returns the list of regions removed during the on-disk restore.
   * The caller is responsible to remove the regions from META.
   * e.g. MetaTableAccessor.deleteRegions(...)
   * @return the list of regions to remove from META
   */
  public List<HRegionInfo> getRegionsToRemove() {
    return this.regionsToRemove;
  }

  public void setNewRegions(final HRegionInfo[] hris) {
    if (hris != null) {
      regionsToAdd = Arrays.asList(hris);
    } else {
      regionsToAdd = null;
    }
  }

  public void addRegionToRemove(final HRegionInfo hri) {
    if (regionsToRemove == null) {
      regionsToRemove = new LinkedList<HRegionInfo>();
    }
    regionsToRemove.add(hri);
  }

  public void addRegionToRestore(final HRegionInfo hri) {
    if (regionsToRestore == null) {
      regionsToRestore = new LinkedList<HRegionInfo>();
    }
    regionsToRestore.add(hri);
  }

  public void updateMetaParentRegions(Connection connection,
      final List<HRegionInfo> regionInfos) throws IOException {
    if (regionInfos == null || parentsMap.isEmpty()) return;

    // Extract region names and offlined regions
    Map<String, HRegionInfo> regionsByName = new HashMap<String, HRegionInfo>(regionInfos.size());
    List<HRegionInfo> parentRegions = new LinkedList<>();
    for (HRegionInfo regionInfo: regionInfos) {
      if (regionInfo.isSplitParent()) {
        parentRegions.add(regionInfo);
      } else {
        regionsByName.put(regionInfo.getEncodedName(), regionInfo);
      }
    }

    // Update Offline parents
    for (HRegionInfo regionInfo: parentRegions) {
      Pair<String, String> daughters = parentsMap.get(regionInfo.getEncodedName());
      if (daughters == null) {
        // The snapshot contains an unreferenced region.
        // It will be removed by the CatalogJanitor.
        LOG.warn("Skip update of unreferenced offline parent: " + regionInfo);
        continue;
      }

      // One side of the split is already compacted
      if (daughters.getSecond() == null) {
        daughters.setSecond(daughters.getFirst());
      }

      LOG.debug("Update splits parent " + regionInfo.getEncodedName() + " -> " + daughters);
      MetaTableAccessor.addRegionToMeta(connection, regionInfo,
        regionsByName.get(daughters.getFirst()),
        regionsByName.get(daughters.getSecond()));
    }
  }
}
