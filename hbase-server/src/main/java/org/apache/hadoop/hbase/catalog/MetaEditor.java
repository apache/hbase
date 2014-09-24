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
package org.apache.hadoop.hbase.catalog;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NotAllMetaRegionsOnlineException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos.MultiRowMutationService;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos.MutateRowsRequest;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Threads;

import com.google.protobuf.ServiceException;

/**
 * Writes region and assignment information to <code>hbase:meta</code>.
 * TODO: Put MetaReader and MetaEditor together; doesn't make sense having
 * them distinct. see HBASE-3475.
 */
@InterfaceAudience.Private
public class MetaEditor {
  // TODO: Strip CatalogTracker from this class.  Its all over and in the end
  // its only used to get its Configuration so we can get associated
  // Connection.
  private static final Log LOG = LogFactory.getLog(MetaEditor.class);

  /**
   * Generates and returns a Put containing the region into for the catalog table
   */
  public static Put makePutFromRegionInfo(HRegionInfo regionInfo)
  throws IOException {
    Put put = new Put(regionInfo.getRegionName());
    addRegionInfo(put, regionInfo);
    return put;
  }

  /**
   * Generates and returns a Delete containing the region info for the catalog
   * table
   */
  public static Delete makeDeleteFromRegionInfo(HRegionInfo regionInfo) {
    if (regionInfo == null) {
      throw new IllegalArgumentException("Can't make a delete for null region");
    }
    Delete delete = new Delete(regionInfo.getRegionName());
    return delete;
  }

  /**
   * Adds split daughters to the Put
   */
  public static Put addDaughtersToPut(Put put, HRegionInfo splitA, HRegionInfo splitB) {
    if (splitA != null) {
      put.addImmutable(
          HConstants.CATALOG_FAMILY, HConstants.SPLITA_QUALIFIER, splitA.toByteArray());
    }
    if (splitB != null) {
      put.addImmutable(
          HConstants.CATALOG_FAMILY, HConstants.SPLITB_QUALIFIER, splitB.toByteArray());
    }
    return put;
  }

  /**
   * Put the passed <code>p</code> to the <code>hbase:meta</code> table.
   * @param ct CatalogTracker on whose back we will ride the edit.
   * @param p Put to add to hbase:meta
   * @throws IOException
   */
  static void putToMetaTable(final CatalogTracker ct, final Put p)
  throws IOException {
    put(MetaReader.getMetaHTable(ct), p);
  }

  /**
   * Put the passed <code>p</code> to a catalog table.
   * @param ct CatalogTracker on whose back we will ride the edit.
   * @param p Put to add
   * @throws IOException
   */
  static void putToCatalogTable(final CatalogTracker ct, final Put p)
  throws IOException {
    put(MetaReader.getCatalogHTable(ct), p);
  }

  /**
   * @param t Table to use (will be closed when done).
   * @param p
   * @throws IOException
   */
  private static void put(final HTable t, final Put p) throws IOException {
    try {
      t.put(p);
    } finally {
      t.close();
    }
  }

  /**
   * Put the passed <code>ps</code> to the <code>hbase:meta</code> table.
   * @param ct CatalogTracker on whose back we will ride the edit.
   * @param ps Put to add to hbase:meta
   * @throws IOException
   */
  public static void putsToMetaTable(final CatalogTracker ct, final List<Put> ps)
  throws IOException {
    HTable t = MetaReader.getMetaHTable(ct);
    try {
      t.put(ps);
    } finally {
      t.close();
    }
  }

  /**
   * Delete the passed <code>d</code> from the <code>hbase:meta</code> table.
   * @param ct CatalogTracker on whose back we will ride the edit.
   * @param d Delete to add to hbase:meta
   * @throws IOException
   */
  static void deleteFromMetaTable(final CatalogTracker ct, final Delete d)
      throws IOException {
    List<Delete> dels = new ArrayList<Delete>(1);
    dels.add(d);
    deleteFromMetaTable(ct, dels);
  }

  /**
   * Delete the passed <code>deletes</code> from the <code>hbase:meta</code> table.
   * @param ct CatalogTracker on whose back we will ride the edit.
   * @param deletes Deletes to add to hbase:meta  This list should support #remove.
   * @throws IOException
   */
  public static void deleteFromMetaTable(final CatalogTracker ct, final List<Delete> deletes)
      throws IOException {
    HTable t = MetaReader.getMetaHTable(ct);
    try {
      t.delete(deletes);
    } finally {
      t.close();
    }
  }

  /**
   * Execute the passed <code>mutations</code> against <code>hbase:meta</code> table.
   * @param ct CatalogTracker on whose back we will ride the edit.
   * @param mutations Puts and Deletes to execute on hbase:meta
   * @throws IOException
   */
  public static void mutateMetaTable(final CatalogTracker ct, final List<Mutation> mutations)
      throws IOException {
    HTable t = MetaReader.getMetaHTable(ct);
    try {
      t.batch(mutations);
    } catch (InterruptedException e) {
      InterruptedIOException ie = new InterruptedIOException(e.getMessage());
      ie.initCause(e);
      throw ie;
    } finally {
      t.close();
    }
  }

  /**
   * Adds a hbase:meta row for the specified new region.
   * @param regionInfo region information
   * @throws IOException if problem connecting or updating meta
   */
  public static void addRegionToMeta(CatalogTracker catalogTracker,
      HRegionInfo regionInfo)
  throws IOException {
    putToMetaTable(catalogTracker, makePutFromRegionInfo(regionInfo));
    LOG.info("Added " + regionInfo.getRegionNameAsString());
  }

  /**
   * Adds a hbase:meta row for the specified new region to the given catalog table. The
   * HTable is not flushed or closed.
   * @param meta the HTable for META
   * @param regionInfo region information
   * @throws IOException if problem connecting or updating meta
   */
  public static void addRegionToMeta(HTable meta, HRegionInfo regionInfo) throws IOException {
    addRegionToMeta(meta, regionInfo, null, null);
  }

  /**
   * Adds a (single) hbase:meta row for the specified new region and its daughters. Note that this does
   * not add its daughter's as different rows, but adds information about the daughters
   * in the same row as the parent. Use
   * {@link #splitRegion(CatalogTracker, HRegionInfo, HRegionInfo, HRegionInfo, ServerName)}
   * if you want to do that.
   * @param meta the HTable for META
   * @param regionInfo region information
   * @param splitA first split daughter of the parent regionInfo
   * @param splitB second split daughter of the parent regionInfo
   * @throws IOException if problem connecting or updating meta
   */
  public static void addRegionToMeta(HTable meta, HRegionInfo regionInfo,
      HRegionInfo splitA, HRegionInfo splitB) throws IOException {
    Put put = makePutFromRegionInfo(regionInfo);
    addDaughtersToPut(put, splitA, splitB);
    meta.put(put);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Added " + regionInfo.getRegionNameAsString());
    }
  }

  /**
   * Adds a (single) hbase:meta row for the specified new region and its daughters. Note that this does
   * not add its daughter's as different rows, but adds information about the daughters
   * in the same row as the parent. Use
   * {@link #splitRegion(CatalogTracker, HRegionInfo, HRegionInfo, HRegionInfo, ServerName)}
   * if you want to do that.
   * @param catalogTracker CatalogTracker on whose back we will ride the edit.
   * @param regionInfo region information
   * @param splitA first split daughter of the parent regionInfo
   * @param splitB second split daughter of the parent regionInfo
   * @throws IOException if problem connecting or updating meta
   */
  public static void addRegionToMeta(CatalogTracker catalogTracker, HRegionInfo regionInfo,
      HRegionInfo splitA, HRegionInfo splitB) throws IOException {
    HTable meta = MetaReader.getMetaHTable(catalogTracker);
    try {
      addRegionToMeta(meta, regionInfo, splitA, splitB);
    } finally {
      meta.close();
    }
  }

  /**
   * Adds a hbase:meta row for each of the specified new regions.
   * @param catalogTracker CatalogTracker
   * @param regionInfos region information list
   * @throws IOException if problem connecting or updating meta
   */
  public static void addRegionsToMeta(CatalogTracker catalogTracker,
      List<HRegionInfo> regionInfos)
  throws IOException {
    List<Put> puts = new ArrayList<Put>();
    for (HRegionInfo regionInfo : regionInfos) {
      puts.add(makePutFromRegionInfo(regionInfo));
    }
    putsToMetaTable(catalogTracker, puts);
    LOG.info("Added " + puts.size());
  }

  /**
   * Adds a daughter region entry to meta.
   * @param regionInfo the region to put
   * @param sn the location of the region
   * @param openSeqNum the latest sequence number obtained when the region was open
   */
  public static void addDaughter(final CatalogTracker catalogTracker,
      final HRegionInfo regionInfo, final ServerName sn, final long openSeqNum)
  throws NotAllMetaRegionsOnlineException, IOException {
    Put put = new Put(regionInfo.getRegionName());
    addRegionInfo(put, regionInfo);
    if (sn != null) {
      addLocation(put, sn, openSeqNum);
    }
    putToMetaTable(catalogTracker, put);
    LOG.info("Added daughter " + regionInfo.getEncodedName() +
      (sn == null? ", serverName=null": ", serverName=" + sn.toString()));
  }

  /**
   * Merge the two regions into one in an atomic operation. Deletes the two
   * merging regions in hbase:meta and adds the merged region with the information of
   * two merging regions.
   * @param catalogTracker the catalog tracker
   * @param mergedRegion the merged region
   * @param regionA
   * @param regionB
   * @param sn the location of the region
   * @throws IOException
   */
  public static void mergeRegions(final CatalogTracker catalogTracker,
      HRegionInfo mergedRegion, HRegionInfo regionA, HRegionInfo regionB,
      ServerName sn) throws IOException {
    HTable meta = MetaReader.getMetaHTable(catalogTracker);
    try {
      HRegionInfo copyOfMerged = new HRegionInfo(mergedRegion);

      // Put for parent
      Put putOfMerged = makePutFromRegionInfo(copyOfMerged);
      putOfMerged.addImmutable(HConstants.CATALOG_FAMILY, HConstants.MERGEA_QUALIFIER,
          regionA.toByteArray());
      putOfMerged.addImmutable(HConstants.CATALOG_FAMILY, HConstants.MERGEB_QUALIFIER,
          regionB.toByteArray());

      // Deletes for merging regions
      Delete deleteA = makeDeleteFromRegionInfo(regionA);
      Delete deleteB = makeDeleteFromRegionInfo(regionB);

      // The merged is a new region, openSeqNum = 1 is fine.
      addLocation(putOfMerged, sn, 1);

      byte[] tableRow = Bytes.toBytes(mergedRegion.getRegionNameAsString()
          + HConstants.DELIMITER);
      multiMutate(meta, tableRow, putOfMerged, deleteA, deleteB);
    } finally {
      meta.close();
    }
  }

  /**
   * Splits the region into two in an atomic operation. Offlines the parent
   * region with the information that it is split into two, and also adds
   * the daughter regions. Does not add the location information to the daughter
   * regions since they are not open yet.
   * @param catalogTracker the catalog tracker
   * @param parent the parent region which is split
   * @param splitA Split daughter region A
   * @param splitB Split daughter region A
   * @param sn the location of the region
   */
  public static void splitRegion(final CatalogTracker catalogTracker,
      HRegionInfo parent, HRegionInfo splitA, HRegionInfo splitB,
      ServerName sn) throws IOException {
    HTable meta = MetaReader.getMetaHTable(catalogTracker);
    try {
      HRegionInfo copyOfParent = new HRegionInfo(parent);
      copyOfParent.setOffline(true);
      copyOfParent.setSplit(true);

      //Put for parent
      Put putParent = makePutFromRegionInfo(copyOfParent);
      addDaughtersToPut(putParent, splitA, splitB);

      //Puts for daughters
      Put putA = makePutFromRegionInfo(splitA);
      Put putB = makePutFromRegionInfo(splitB);

      addLocation(putA, sn, 1); //these are new regions, openSeqNum = 1 is fine.
      addLocation(putB, sn, 1);

      byte[] tableRow = Bytes.toBytes(parent.getRegionNameAsString() + HConstants.DELIMITER);
      multiMutate(meta, tableRow, putParent, putA, putB);
    } finally {
      meta.close();
    }
  }

  /**
   * Performs an atomic multi-Mutate operation against the given table.
   */
  private static void multiMutate(HTable table, byte[] row, Mutation... mutations) throws IOException {
    CoprocessorRpcChannel channel = table.coprocessorService(row);
    MutateRowsRequest.Builder mmrBuilder = MutateRowsRequest.newBuilder();
    for (Mutation mutation : mutations) {
      if (mutation instanceof Put) {
        mmrBuilder.addMutationRequest(ProtobufUtil.toMutation(MutationType.PUT, mutation));
      } else if (mutation instanceof Delete) {
        mmrBuilder.addMutationRequest(ProtobufUtil.toMutation(MutationType.DELETE, mutation));
      } else {
        throw new DoNotRetryIOException("multi in MetaEditor doesn't support "
            + mutation.getClass().getName());
      }
    }

    MultiRowMutationService.BlockingInterface service =
        MultiRowMutationService.newBlockingStub(channel);
    try {
      service.mutateRows(null, mmrBuilder.build());
    } catch (ServiceException ex) {
      ProtobufUtil.toIOException(ex);
    }
  }


  /**
   * Updates the location of the specified hbase:meta region in ROOT to be the
   * specified server hostname and startcode.
   * <p>
   * Uses passed catalog tracker to get a connection to the server hosting
   * ROOT and makes edits to that region.
   *
   * @param catalogTracker catalog tracker
   * @param regionInfo region to update location of
   * @param sn Server name
   * @param openSeqNum the latest sequence number obtained when the region was open
   * @throws IOException
   * @throws ConnectException Usually because the regionserver carrying hbase:meta
   * is down.
   * @throws NullPointerException Because no -ROOT- server connection
   */
  public static void updateMetaLocation(CatalogTracker catalogTracker,
      HRegionInfo regionInfo, ServerName sn, long openSeqNum)
  throws IOException, ConnectException {
    updateLocation(catalogTracker, regionInfo, sn, openSeqNum);
  }

  /**
   * Updates the location of the specified region in hbase:meta to be the specified
   * server hostname and startcode.
   * <p>
   * Uses passed catalog tracker to get a connection to the server hosting
   * hbase:meta and makes edits to that region.
   *
   * @param catalogTracker catalog tracker
   * @param regionInfo region to update location of
   * @param sn Server name
   * @throws IOException
   */
  public static void updateRegionLocation(CatalogTracker catalogTracker,
      HRegionInfo regionInfo, ServerName sn, long updateSeqNum)
  throws IOException {
    updateLocation(catalogTracker, regionInfo, sn, updateSeqNum);
  }

  /**
   * Updates the location of the specified region to be the specified server.
   * <p>
   * Connects to the specified server which should be hosting the specified
   * catalog region name to perform the edit.
   *
   * @param catalogTracker
   * @param regionInfo region to update location of
   * @param sn Server name
   * @param openSeqNum the latest sequence number obtained when the region was open
   * @throws IOException In particular could throw {@link java.net.ConnectException}
   * if the server is down on other end.
   */
  private static void updateLocation(final CatalogTracker catalogTracker,
      HRegionInfo regionInfo, ServerName sn, long openSeqNum)
  throws IOException {
    Put put = new Put(regionInfo.getRegionName());
    addLocation(put, sn, openSeqNum);
    putToCatalogTable(catalogTracker, put);
    LOG.info("Updated row " + regionInfo.getRegionNameAsString() +
      " with server=" + sn);
  }

  /**
   * Deletes the specified region from META.
   * @param catalogTracker
   * @param regionInfo region to be deleted from META
   * @throws IOException
   */
  public static void deleteRegion(CatalogTracker catalogTracker,
      HRegionInfo regionInfo)
  throws IOException {
    Delete delete = new Delete(regionInfo.getRegionName());
    deleteFromMetaTable(catalogTracker, delete);
    LOG.info("Deleted " + regionInfo.getRegionNameAsString());
  }

  /**
   * Deletes the specified regions from META.
   * @param catalogTracker
   * @param regionsInfo list of regions to be deleted from META
   * @throws IOException
   */
  public static void deleteRegions(CatalogTracker catalogTracker,
      List<HRegionInfo> regionsInfo) throws IOException {
    List<Delete> deletes = new ArrayList<Delete>(regionsInfo.size());
    for (HRegionInfo hri: regionsInfo) {
      deletes.add(new Delete(hri.getRegionName()));
    }
    deleteFromMetaTable(catalogTracker, deletes);
    LOG.info("Deleted " + regionsInfo);
  }

  /**
   * Adds and Removes the specified regions from hbase:meta
   * @param catalogTracker
   * @param regionsToRemove list of regions to be deleted from META
   * @param regionsToAdd list of regions to be added to META
   * @throws IOException
   */
  public static void mutateRegions(CatalogTracker catalogTracker,
      final List<HRegionInfo> regionsToRemove, final List<HRegionInfo> regionsToAdd)
      throws IOException {
    List<Mutation> mutation = new ArrayList<Mutation>();
    if (regionsToRemove != null) {
      for (HRegionInfo hri: regionsToRemove) {
        mutation.add(new Delete(hri.getRegionName()));
      }
    }
    if (regionsToAdd != null) {
      for (HRegionInfo hri: regionsToAdd) {
        mutation.add(makePutFromRegionInfo(hri));
      }
    }
    mutateMetaTable(catalogTracker, mutation);
    if (regionsToRemove != null && regionsToRemove.size() > 0) {
      LOG.debug("Deleted " + regionsToRemove);
    }
    if (regionsToAdd != null && regionsToAdd.size() > 0) {
      LOG.debug("Added " + regionsToAdd);
    }
  }

  /**
   * Overwrites the specified regions from hbase:meta
   * @param catalogTracker
   * @param regionInfos list of regions to be added to META
   * @throws IOException
   */
  public static void overwriteRegions(CatalogTracker catalogTracker,
      List<HRegionInfo> regionInfos) throws IOException {
    deleteRegions(catalogTracker, regionInfos);
    // Why sleep? This is the easiest way to ensure that the previous deletes does not
    // eclipse the following puts, that might happen in the same ts from the server.
    // See HBASE-9906, and HBASE-9879. Once either HBASE-9879, HBASE-8770 is fixed,
    // or HBASE-9905 is fixed and meta uses seqIds, we do not need the sleep.
    Threads.sleep(20);
    addRegionsToMeta(catalogTracker, regionInfos);
    LOG.info("Overwritten " + regionInfos);
  }

  /**
   * Deletes merge qualifiers for the specified merged region.
   * @param catalogTracker
   * @param mergedRegion
   * @throws IOException
   */
  public static void deleteMergeQualifiers(CatalogTracker catalogTracker,
      final HRegionInfo mergedRegion) throws IOException {
    Delete delete = new Delete(mergedRegion.getRegionName());
    delete.deleteColumns(HConstants.CATALOG_FAMILY, HConstants.MERGEA_QUALIFIER);
    delete.deleteColumns(HConstants.CATALOG_FAMILY, HConstants.MERGEB_QUALIFIER);
    deleteFromMetaTable(catalogTracker, delete);
    LOG.info("Deleted references in merged region "
        + mergedRegion.getRegionNameAsString() + ", qualifier="
        + Bytes.toStringBinary(HConstants.MERGEA_QUALIFIER) + " and qualifier="
        + Bytes.toStringBinary(HConstants.MERGEB_QUALIFIER));
  }

  private static Put addRegionInfo(final Put p, final HRegionInfo hri)
  throws IOException {
    p.addImmutable(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER,
        hri.toByteArray());
    return p;
  }

  private static Put addLocation(final Put p, final ServerName sn, long openSeqNum) {
    // using regionserver's local time as the timestamp of Put.
    // See: HBASE-11536
    long now = EnvironmentEdgeManager.currentTimeMillis();
    p.addImmutable(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER, now,
      Bytes.toBytes(sn.getHostAndPort()));
    p.addImmutable(HConstants.CATALOG_FAMILY, HConstants.STARTCODE_QUALIFIER, now,
      Bytes.toBytes(sn.getStartcode()));
    p.addImmutable(HConstants.CATALOG_FAMILY, HConstants.SEQNUM_QUALIFIER, now,
        Bytes.toBytes(openSeqNum));
    return p;
  }
}
