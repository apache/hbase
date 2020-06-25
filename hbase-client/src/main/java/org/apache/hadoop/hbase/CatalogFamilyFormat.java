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
package org.apache.hadoop.hbase;

import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Helper class for generating/parsing
 * {@value org.apache.hadoop.hbase.HConstants#CATALOG_FAMILY_STR} family cells in meta table.
 * <p/>
 * The cells in catalog family are:
 *
 * <pre>
 * For each table range ('Region'), there is a single row, formatted as:
 * &lt;tableName&gt;,&lt;startKey&gt;,&lt;regionId&gt;,&lt;encodedRegionName&gt;.
 * This row is the serialized regionName of the default region replica.
 * Columns are:
 * info:regioninfo         => contains serialized HRI for the default region replica
 * info:server             => contains hostname:port (in string form) for the server hosting
 *                            the default regionInfo replica
 * info:server_&lt;replicaId&gt => contains hostname:port (in string form) for the server hosting
 *                                 the regionInfo replica with replicaId
 * info:serverstartcode    => contains server start code (in binary long form) for the server
 *                            hosting the default regionInfo replica
 * info:serverstartcode_&lt;replicaId&gt => contains server start code (in binary long form) for
 *                                          the server hosting the regionInfo replica with
 *                                          replicaId
 * info:seqnumDuringOpen   => contains seqNum (in binary long form) for the region at the time
 *                            the server opened the region with default replicaId
 * info:seqnumDuringOpen_&lt;replicaId&gt => contains seqNum (in binary long form) for the region
 *                                           at the time the server opened the region with
 *                                           replicaId
 * info:splitA             => contains a serialized HRI for the first daughter region if the
 *                            region is split
 * info:splitB             => contains a serialized HRI for the second daughter region if the
 *                            region is split
 * info:merge*             => contains a serialized HRI for a merge parent region. There will be two
 *                            or more of these columns in a row. A row that has these columns is
 *                            undergoing a merge and is the result of the merge. Columns listed
 *                            in marge* columns are the parents of this merged region. Example
 *                            columns: info:merge0001, info:merge0002. You make also see 'mergeA',
 *                            and 'mergeB'. This is old form replaced by the new format that allows
 *                            for more than two parents to be merged at a time.
 * </pre>
 */
@InterfaceAudience.Private
public class CatalogFamilyFormat {

  private static final Logger LOG = LoggerFactory.getLogger(CatalogFamilyFormat.class);

  /** A regex for parsing server columns from meta. See above javadoc for meta layout */
  private static final Pattern SERVER_COLUMN_PATTERN =
    Pattern.compile("^server(_[0-9a-fA-F]{4})?$");

  /**
   * Returns an HRI parsed from this regionName. Not all the fields of the HRI is stored in the
   * name, so the returned object should only be used for the fields in the regionName.
   * <p/>
   * Since the returned object does not contain all the fields, we do not expose this method in
   * public API, such as {@link RegionInfo} or {@link RegionInfoBuilder}.
   */
  public static RegionInfo parseRegionInfoFromRegionName(byte[] regionName) throws IOException {
    byte[][] fields = RegionInfo.parseRegionName(regionName);
    long regionId = Long.parseLong(Bytes.toString(fields[2]));
    int replicaId = fields.length > 3 ? Integer.parseInt(Bytes.toString(fields[3]), 16) : 0;
    return RegionInfoBuilder.newBuilder(TableName.valueOf(fields[0])).setStartKey(fields[1])
      .setEndKey(fields[2]).setSplit(false).setRegionId(regionId).setReplicaId(replicaId).build();
  }

  /**
   * Returns the RegionInfo object from the column {@link HConstants#CATALOG_FAMILY} and
   * <code>qualifier</code> of the catalog table result.
   * @param r a Result object from the catalog table scan
   * @param qualifier Column family qualifier
   * @return An RegionInfo instance or null.
   */
  @Nullable
  public static RegionInfo getRegionInfo(final Result r, byte[] qualifier) {
    Cell cell = r.getColumnLatestCell(HConstants.CATALOG_FAMILY, qualifier);
    if (cell == null) {
      return null;
    }
    return RegionInfo.parseFromOrNull(cell.getValueArray(), cell.getValueOffset(),
      cell.getValueLength());
  }

  /**
   * Returns RegionInfo object from the column
   * HConstants.CATALOG_FAMILY:HConstants.REGIONINFO_QUALIFIER of the catalog table Result.
   * @param data a Result object from the catalog table scan
   * @return RegionInfo or null
   */
  public static RegionInfo getRegionInfo(Result data) {
    return getRegionInfo(data, HConstants.REGIONINFO_QUALIFIER);
  }

  /**
   * Returns the HRegionLocation parsed from the given meta row Result for the given regionInfo and
   * replicaId. The regionInfo can be the default region info for the replica.
   * @param r the meta row result
   * @param regionInfo RegionInfo for default replica
   * @param replicaId the replicaId for the HRegionLocation
   * @return HRegionLocation parsed from the given meta row Result for the given replicaId
   */
  public static HRegionLocation getRegionLocation(final Result r, final RegionInfo regionInfo,
    final int replicaId) {
    ServerName serverName = getServerName(r, replicaId);
    long seqNum = getSeqNumDuringOpen(r, replicaId);
    RegionInfo replicaInfo = RegionReplicaUtil.getRegionInfoForReplica(regionInfo, replicaId);
    return new HRegionLocation(replicaInfo, serverName, seqNum);
  }

  /**
   * Returns an HRegionLocationList extracted from the result.
   * @return an HRegionLocationList containing all locations for the region range or null if we
   *         can't deserialize the result.
   */
  @Nullable
  public static RegionLocations getRegionLocations(final Result r) {
    if (r == null) {
      return null;
    }
    RegionInfo regionInfo = getRegionInfo(r, HConstants.REGIONINFO_QUALIFIER);
    if (regionInfo == null) {
      return null;
    }

    List<HRegionLocation> locations = new ArrayList<>(1);
    NavigableMap<byte[], NavigableMap<byte[], byte[]>> familyMap = r.getNoVersionMap();

    locations.add(getRegionLocation(r, regionInfo, 0));

    NavigableMap<byte[], byte[]> infoMap = familyMap.get(HConstants.CATALOG_FAMILY);
    if (infoMap == null) {
      return new RegionLocations(locations);
    }

    // iterate until all serverName columns are seen
    int replicaId = 0;
    byte[] serverColumn = getServerColumn(replicaId);
    SortedMap<byte[], byte[]> serverMap;
    serverMap = infoMap.tailMap(serverColumn, false);

    if (serverMap.isEmpty()) {
      return new RegionLocations(locations);
    }

    for (Map.Entry<byte[], byte[]> entry : serverMap.entrySet()) {
      replicaId = parseReplicaIdFromServerColumn(entry.getKey());
      if (replicaId < 0) {
        break;
      }
      HRegionLocation location = getRegionLocation(r, regionInfo, replicaId);
      // In case the region replica is newly created, it's location might be null. We usually do not
      // have HRL's in RegionLocations object with null ServerName. They are handled as null HRLs.
      if (location.getServerName() == null) {
        locations.add(null);
      } else {
        locations.add(location);
      }
    }

    return new RegionLocations(locations);
  }

  /**
   * Returns a {@link ServerName} from catalog table {@link Result}.
   * @param r Result to pull from
   * @return A ServerName instance or null if necessary fields not found or empty.
   */
  @Nullable
  public static ServerName getServerName(Result r, int replicaId) {
    byte[] serverColumn = getServerColumn(replicaId);
    Cell cell = r.getColumnLatestCell(HConstants.CATALOG_FAMILY, serverColumn);
    if (cell == null || cell.getValueLength() == 0) {
      return null;
    }
    String hostAndPort =
      Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
    byte[] startcodeColumn = getStartCodeColumn(replicaId);
    cell = r.getColumnLatestCell(HConstants.CATALOG_FAMILY, startcodeColumn);
    if (cell == null || cell.getValueLength() == 0) {
      return null;
    }
    try {
      return ServerName.valueOf(hostAndPort,
        Bytes.toLong(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
    } catch (IllegalArgumentException e) {
      LOG.error("Ignoring invalid region for server " + hostAndPort + "; cell=" + cell, e);
      return null;
    }
  }

  /**
   * Returns the column qualifier for server column for replicaId
   * @param replicaId the replicaId of the region
   * @return a byte[] for server column qualifier
   */
  public static byte[] getServerColumn(int replicaId) {
    return replicaId == 0 ? HConstants.SERVER_QUALIFIER :
      Bytes.toBytes(HConstants.SERVER_QUALIFIER_STR + META_REPLICA_ID_DELIMITER +
        String.format(RegionInfo.REPLICA_ID_FORMAT, replicaId));
  }

  /**
   * Returns the column qualifier for server start code column for replicaId
   * @param replicaId the replicaId of the region
   * @return a byte[] for server start code column qualifier
   */
  public static byte[] getStartCodeColumn(int replicaId) {
    return replicaId == 0 ? HConstants.STARTCODE_QUALIFIER :
      Bytes.toBytes(HConstants.STARTCODE_QUALIFIER_STR + META_REPLICA_ID_DELIMITER +
        String.format(RegionInfo.REPLICA_ID_FORMAT, replicaId));
  }

  /**
   * The latest seqnum that the server writing to meta observed when opening the region. E.g. the
   * seqNum when the result of {@link getServerName} was written.
   * @param r Result to pull the seqNum from
   * @return SeqNum, or HConstants.NO_SEQNUM if there's no value written.
   */
  private static long getSeqNumDuringOpen(final Result r, final int replicaId) {
    Cell cell = r.getColumnLatestCell(HConstants.CATALOG_FAMILY, getSeqNumColumn(replicaId));
    if (cell == null || cell.getValueLength() == 0) {
      return HConstants.NO_SEQNUM;
    }
    return Bytes.toLong(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
  }

  /**
   * Returns the column qualifier for seqNum column for replicaId
   * @param replicaId the replicaId of the region
   * @return a byte[] for seqNum column qualifier
   */
  public static byte[] getSeqNumColumn(int replicaId) {
    return replicaId == 0 ? HConstants.SEQNUM_QUALIFIER :
      Bytes.toBytes(HConstants.SEQNUM_QUALIFIER_STR + META_REPLICA_ID_DELIMITER +
        String.format(RegionInfo.REPLICA_ID_FORMAT, replicaId));
  }

  /** The delimiter for meta columns for replicaIds &gt; 0 */
  @VisibleForTesting
  static final char META_REPLICA_ID_DELIMITER = '_';

  /**
   * Parses the replicaId from the server column qualifier. See top of the class javadoc for the
   * actual meta layout
   * @param serverColumn the column qualifier
   * @return an int for the replicaId
   */
  @VisibleForTesting
  static int parseReplicaIdFromServerColumn(byte[] serverColumn) {
    String serverStr = Bytes.toString(serverColumn);

    Matcher matcher = SERVER_COLUMN_PATTERN.matcher(serverStr);
    if (matcher.matches() && matcher.groupCount() > 0) {
      String group = matcher.group(1);
      if (group != null && group.length() > 0) {
        return Integer.parseInt(group.substring(1), 16);
      } else {
        return 0;
      }
    }
    return -1;
  }

  /** Returns the row key to use for this regionInfo */
  public static byte[] getMetaKeyForRegion(RegionInfo regionInfo) {
    return RegionReplicaUtil.getRegionInfoForDefaultReplica(regionInfo).getRegionName();
  }

  /**
   * Returns the column qualifier for serialized region state
   * @param replicaId the replicaId of the region
   * @return a byte[] for state qualifier
   */
  @VisibleForTesting
  static byte[] getRegionStateColumn(int replicaId) {
    return replicaId == 0 ? HConstants.STATE_QUALIFIER :
      Bytes.toBytes(HConstants.STATE_QUALIFIER_STR + META_REPLICA_ID_DELIMITER +
        String.format(RegionInfo.REPLICA_ID_FORMAT, replicaId));
  }

  /**
   * Returns the column qualifier for serialized region state
   * @param replicaId the replicaId of the region
   * @return a byte[] for sn column qualifier
   */
  public static byte[] getServerNameColumn(int replicaId) {
    return replicaId == 0 ? HConstants.SERVERNAME_QUALIFIER :
      Bytes.toBytes(HConstants.SERVERNAME_QUALIFIER_STR + META_REPLICA_ID_DELIMITER +
        String.format(RegionInfo.REPLICA_ID_FORMAT, replicaId));
  }

  /**
   * Decode table state from META Result. Should contain cell from HConstants.TABLE_FAMILY
   * @return null if not found
   */
  @Nullable
  public static TableState getTableState(Result r) throws IOException {
    Cell cell = r.getColumnLatestCell(HConstants.TABLE_FAMILY, HConstants.TABLE_STATE_QUALIFIER);
    if (cell == null) {
      return null;
    }
    try {
      return TableState.parseFrom(TableName.valueOf(r.getRow()),
        Arrays.copyOfRange(cell.getValueArray(), cell.getValueOffset(),
          cell.getValueOffset() + cell.getValueLength()));
    } catch (DeserializationException e) {
      throw new IOException(e);
    }
  }

  public static Delete removeRegionReplica(byte[] metaRow, int replicaIndexToDeleteFrom,
    int numReplicasToRemove) {
    int absoluteIndex = replicaIndexToDeleteFrom + numReplicasToRemove;
    long now = EnvironmentEdgeManager.currentTime();
    Delete deleteReplicaLocations = new Delete(metaRow);
    for (int i = replicaIndexToDeleteFrom; i < absoluteIndex; i++) {
      deleteReplicaLocations.addColumns(HConstants.CATALOG_FAMILY, getServerColumn(i), now);
      deleteReplicaLocations.addColumns(HConstants.CATALOG_FAMILY, getSeqNumColumn(i), now);
      deleteReplicaLocations.addColumns(HConstants.CATALOG_FAMILY, getStartCodeColumn(i), now);
      deleteReplicaLocations.addColumns(HConstants.CATALOG_FAMILY, getServerNameColumn(i), now);
      deleteReplicaLocations.addColumns(HConstants.CATALOG_FAMILY, getRegionStateColumn(i), now);
    }
    return deleteReplicaLocations;
  }
}
