/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitationsME
 * under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MetaMutationAnnotation;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coordination.BaseCoordinatedStateManager;
import org.apache.hadoop.hbase.coordination.RegionMergeCoordination.RegionMergeDetails;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;
import org.apache.hadoop.hbase.regionserver.SplitTransactionImpl.LoggingProgressable;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ConfigUtil;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.zookeeper.KeeperException;

@InterfaceAudience.Private
public class RegionMergeTransactionImpl implements RegionMergeTransaction {
  private static final Log LOG = LogFactory.getLog(RegionMergeTransactionImpl.class);

  // Merged region info
  private HRegionInfo mergedRegionInfo;
  // region_a sorts before region_b
  private final HRegion region_a;
  private final HRegion region_b;
  // merges dir is under region_a
  private final Path mergesdir;
  // We only merge adjacent regions if forcible is false
  private final boolean forcible;
  private boolean useCoordinationForAssignment;
  private final long masterSystemTime;

  /*
   * Transaction state for listener, only valid during execute and
   * rollback
   */
  private RegionMergeTransactionPhase currentPhase = RegionMergeTransactionPhase.STARTED;
  private Server server;
  private RegionServerServices rsServices;

  public static class JournalEntryImpl implements JournalEntry {
    private RegionMergeTransactionPhase type;
    private long timestamp;

    public JournalEntryImpl(RegionMergeTransactionPhase type) {
      this(type, EnvironmentEdgeManager.currentTime());
    }

    public JournalEntryImpl(RegionMergeTransactionPhase type, long timestamp) {
      this.type = type;
      this.timestamp = timestamp;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(type);
      sb.append(" at ");
      sb.append(timestamp);
      return sb.toString();
    }

    @Override
    public RegionMergeTransactionPhase getPhase() {
      return type;
    }

    @Override
    public long getTimeStamp() {
      return timestamp;
    }
  }

  /*
   * Journal of how far the merge transaction has progressed.
   */
  private final List<JournalEntry> journal = new ArrayList<JournalEntry>();

  /*
   * Listeners
   */
  private final ArrayList<TransactionListener> listeners = new ArrayList<TransactionListener>();

  private static IOException closedByOtherException = new IOException(
      "Failed to close region: already closed by another thread");

  private RegionServerCoprocessorHost rsCoprocessorHost = null;

  private RegionMergeDetails rmd;

  /**
   * Constructor
   * @param a region a to merge
   * @param b region b to merge
   * @param forcible if false, we will only merge adjacent regions
   */
  public RegionMergeTransactionImpl(final Region a, final Region b,
      final boolean forcible) {
    this(a, b, forcible, EnvironmentEdgeManager.currentTime());
  }
  /**
   * Constructor
   * @param a region a to merge
   * @param b region b to merge
   * @param forcible if false, we will only merge adjacent regions
   * @param masterSystemTime the time at the master side
   */
  public RegionMergeTransactionImpl(final Region a, final Region b,
      final boolean forcible, long masterSystemTime) {
    if (a.getRegionInfo().compareTo(b.getRegionInfo()) <= 0) {
      this.region_a = (HRegion)a;
      this.region_b = (HRegion)b;
    } else {
      this.region_a = (HRegion)b;
      this.region_b = (HRegion)a;
    }
    this.forcible = forcible;
    this.masterSystemTime = masterSystemTime;
    this.mergesdir = region_a.getRegionFileSystem().getMergesDir();
  }

  private void transition(RegionMergeTransactionPhase nextPhase) throws IOException {
    transition(nextPhase, false);
  }

  private void transition(RegionMergeTransactionPhase nextPhase, boolean isRollback)
      throws IOException {
    if (!isRollback) {
      // Add to the journal first, because if the listener throws an exception
      // we need to roll back starting at 'nextPhase'
      this.journal.add(new JournalEntryImpl(nextPhase));
    }
    for (int i = 0; i < listeners.size(); i++) {
      TransactionListener listener = listeners.get(i);
      if (!isRollback) {
        listener.transition(this, currentPhase, nextPhase);
      } else {
        listener.rollback(this, currentPhase, nextPhase);
      }
    }
    currentPhase = nextPhase;
  }

  /**
   * Does checks on merge inputs.
   * @param services
   * @return <code>true</code> if the regions are mergeable else
   *         <code>false</code> if they are not (e.g. its already closed, etc.).
   */
  @Override
  public boolean prepare(final RegionServerServices services) throws IOException {
    if (!region_a.getTableDesc().getTableName()
        .equals(region_b.getTableDesc().getTableName())) {
      LOG.info("Can't merge regions " + region_a + "," + region_b
          + " because they do not belong to the same table");
      return false;
    }
    if (region_a.getRegionInfo().equals(region_b.getRegionInfo())) {
      LOG.info("Can't merge the same region " + region_a);
      return false;
    }
    if (!forcible && !HRegionInfo.areAdjacent(region_a.getRegionInfo(),
            region_b.getRegionInfo())) {
      String msg = "Skip merging " + this.region_a.getRegionInfo().getRegionNameAsString()
          + " and " + this.region_b.getRegionInfo().getRegionNameAsString()
          + ", because they are not adjacent.";
      LOG.info(msg);
      return false;
    }
    if (!this.region_a.isMergeable() || !this.region_b.isMergeable()) {
      return false;
    }
    try {
      boolean regionAHasMergeQualifier = hasMergeQualifierInMeta(services,
          region_a.getRegionInfo().getRegionName());
      if (regionAHasMergeQualifier ||
          hasMergeQualifierInMeta(services, region_b.getRegionInfo().getRegionName())) {
        LOG.debug("Region " + (regionAHasMergeQualifier ?
              region_a.getRegionInfo().getRegionNameAsString() :
                region_b.getRegionInfo().getRegionNameAsString())
            + " is not mergeable because it has merge qualifier in META");
        return false;
      }
    } catch (IOException e) {
      LOG.warn("Failed judging whether merge transaction is available for "
              + region_a.getRegionInfo().getRegionNameAsString() + " and "
              + region_b.getRegionInfo().getRegionNameAsString(), e);
      return false;
    }

    // WARN: make sure there is no parent region of the two merging regions in
    // hbase:meta If exists, fixing up daughters would cause daughter regions(we
    // have merged one) online again when we restart master, so we should clear
    // the parent region to prevent the above case
    // Since HBASE-7721, we don't need fix up daughters any more. so here do
    // nothing

    this.mergedRegionInfo = getMergedRegionInfo(region_a.getRegionInfo(),
        region_b.getRegionInfo());

    transition(RegionMergeTransactionPhase.PREPARED);
    return true;
  }

  /**
   * Run the transaction.
   * @param server Hosting server instance. Can be null when testing
   * @param services Used to online/offline regions.
   * @throws IOException If thrown, transaction failed. Call
   *           {@link #rollback(Server, RegionServerServices)}
   * @return merged region
   * @throws IOException
   * @see #rollback(Server, RegionServerServices)
   */
  @Override
  public HRegion execute(final Server server,
      final RegionServerServices services) throws IOException {
    if (User.isHBaseSecurityEnabled(region_a.getBaseConf())) {
      LOG.warn("Should use execute(Server, RegionServerServices, User)");
    }
    return execute(server, services, null);
  }

  @Override
  public HRegion execute(final Server server, final RegionServerServices services, User user)
      throws IOException {
    this.server = server;
    this.rsServices = services;
    useCoordinationForAssignment =
        server == null ? true : ConfigUtil.useZKForAssignment(server.getConfiguration());
    if (rmd == null) {
      rmd = server != null && server.getCoordinatedStateManager() != null ?
          ((BaseCoordinatedStateManager) server.getCoordinatedStateManager())
            .getRegionMergeCoordination().getDefaultDetails()
        : null;
    }
    if (rsCoprocessorHost == null) {
      rsCoprocessorHost = server != null ?
        ((HRegionServer) server).getRegionServerCoprocessorHost() : null;
    }
    final HRegion mergedRegion = createMergedRegion(server, services, user);
    if (rsCoprocessorHost != null) {
      if (user == null) {
        rsCoprocessorHost.postMergeCommit(this.region_a, this.region_b, mergedRegion);
      } else {
        try {
          user.getUGI().doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
              rsCoprocessorHost.postMergeCommit(region_a, region_b, mergedRegion);
              return null;
            }
          });
        } catch (InterruptedException ie) {
          InterruptedIOException iioe = new InterruptedIOException();
          iioe.initCause(ie);
          throw iioe;
        }
      }
    }
    stepsAfterPONR(server, services, mergedRegion, user);

    transition(RegionMergeTransactionPhase.COMPLETED);

    return mergedRegion;
  }

  @Deprecated
  public void stepsAfterPONR(final Server server, final RegionServerServices services,
      final HRegion mergedRegion) throws IOException {
    stepsAfterPONR(server, services, mergedRegion, null);
  }

  public void stepsAfterPONR(final Server server, final RegionServerServices services,
      final HRegion mergedRegion, User user) throws IOException {
    openMergedRegion(server, services, mergedRegion);
    if (useCoordination(server)) {
      ((BaseCoordinatedStateManager) server.getCoordinatedStateManager())
        .getRegionMergeCoordination().completeRegionMergeTransaction(services,
          mergedRegionInfo, region_a, region_b, rmd, mergedRegion);
    }
    if (rsCoprocessorHost != null) {
      if (user == null) {
        rsCoprocessorHost.postMerge(region_a, region_b, mergedRegion);
      } else {
        try {
          user.getUGI().doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
              rsCoprocessorHost.postMerge(region_a, region_b, mergedRegion);
              return null;
            }
          });
        } catch (InterruptedException ie) {
          InterruptedIOException iioe = new InterruptedIOException();
          iioe.initCause(ie);
          throw iioe;
        }
      }
    }
  }

  /**
   * Prepare the merged region and region files.
   * @param server Hosting server instance. Can be null when testing
   * @param services Used to online/offline regions.
   * @return merged region
   * @throws IOException If thrown, transaction failed. Call
   *           {@link #rollback(Server, RegionServerServices)}
   */
  HRegion createMergedRegion(final Server server,
      final RegionServerServices services, User user) throws IOException {
    LOG.info("Starting merge of " + region_a + " and "
        + region_b.getRegionInfo().getRegionNameAsString() + ", forcible=" + forcible);
    if ((server != null && server.isStopped())
        || (services != null && services.isStopping())) {
      throw new IOException("Server is stopped or stopping");
    }

    if (rsCoprocessorHost != null) {
      boolean ret = false;
      if (user == null) {
        ret = rsCoprocessorHost.preMerge(region_a, region_b);
      } else {
        try {
          ret = user.getUGI().doAs(new PrivilegedExceptionAction<Boolean>() {
            @Override
            public Boolean run() throws Exception {
              return rsCoprocessorHost.preMerge(region_a, region_b);
            }
          });
        } catch (InterruptedException ie) {
          InterruptedIOException iioe = new InterruptedIOException();
          iioe.initCause(ie);
          throw iioe;
        }
      }
      if (ret) {
        throw new IOException("Coprocessor bypassing regions " + this.region_a + " "
            + this.region_b + " merge.");
      }
    }

    // If true, no cluster to write meta edits to or to use coordination.
    boolean testing = server == null ? true : server.getConfiguration()
        .getBoolean("hbase.testing.nocluster", false);

    HRegion mergedRegion = stepsBeforePONR(server, services, testing);

    @MetaMutationAnnotation
    final List<Mutation> metaEntries = new ArrayList<Mutation>();
    if (rsCoprocessorHost != null) {
      boolean ret = false;
      if (user == null) {
        ret = rsCoprocessorHost.preMergeCommit(region_a, region_b, metaEntries);
      } else {
        try {
          ret = user.getUGI().doAs(new PrivilegedExceptionAction<Boolean>() {
            @Override
            public Boolean run() throws Exception {
              return rsCoprocessorHost.preMergeCommit(region_a, region_b, metaEntries);
            }
          });
        } catch (InterruptedException ie) {
          InterruptedIOException iioe = new InterruptedIOException();
          iioe.initCause(ie);
          throw iioe;
        }
      }

      if (ret) {
        throw new IOException("Coprocessor bypassing regions " + this.region_a + " "
            + this.region_b + " merge.");
      }
      try {
        for (Mutation p : metaEntries) {
          HRegionInfo.parseRegionName(p.getRow());
        }
      } catch (IOException e) {
        LOG.error("Row key of mutation from coprocessor is not parsable as region name."
            + "Mutations from coprocessor should only be for hbase:meta table.", e);
        throw e;
      }
    }

    // This is the point of no return. Similar with SplitTransaction.
    // IF we reach the PONR then subsequent failures need to crash out this
    // regionserver
    transition(RegionMergeTransactionPhase.PONR);

    // Add merged region and delete region_a and region_b
    // as an atomic update. See HBASE-7721. This update to hbase:meta makes the region
    // will determine whether the region is merged or not in case of failures.
    // If it is successful, master will roll-forward, if not, master will
    // rollback
    if (!testing && useCoordinationForAssignment) {
      if (metaEntries.isEmpty()) {
        MetaTableAccessor.mergeRegions(server.getConnection(),
          mergedRegion.getRegionInfo(), region_a.getRegionInfo(), region_b.getRegionInfo(),
          server.getServerName(), region_a.getTableDesc().getRegionReplication(), masterSystemTime);
      } else {
        mergeRegionsAndPutMetaEntries(server.getConnection(),
          mergedRegion.getRegionInfo(), region_a.getRegionInfo(), region_b.getRegionInfo(),
          server.getServerName(), metaEntries, region_a.getTableDesc().getRegionReplication());
      }
    } else if (services != null && !useCoordinationForAssignment) {
      if (!services.reportRegionStateTransition(TransitionCode.MERGE_PONR,
          mergedRegionInfo, region_a.getRegionInfo(), region_b.getRegionInfo())) {
        // Passed PONR, let SSH clean it up
        throw new IOException("Failed to notify master that merge passed PONR: "
          + region_a.getRegionInfo().getRegionNameAsString() + " and "
          + region_b.getRegionInfo().getRegionNameAsString());
      }
    }
    return mergedRegion;
  }

  private void mergeRegionsAndPutMetaEntries(HConnection hConnection,
      HRegionInfo mergedRegion, HRegionInfo regionA, HRegionInfo regionB,
      ServerName serverName, List<Mutation> metaEntries,
      int regionReplication) throws IOException {
    prepareMutationsForMerge(mergedRegion, regionA, regionB, serverName, metaEntries,
      regionReplication);
    MetaTableAccessor.mutateMetaTable(hConnection, metaEntries);
  }

  public void prepareMutationsForMerge(HRegionInfo mergedRegion, HRegionInfo regionA,
      HRegionInfo regionB, ServerName serverName, List<Mutation> mutations,
      int regionReplication) throws IOException {
    HRegionInfo copyOfMerged = new HRegionInfo(mergedRegion);

    // use the maximum of what master passed us vs local time.
    long time = Math.max(EnvironmentEdgeManager.currentTime(), masterSystemTime);

    // Put for parent
    Put putOfMerged = MetaTableAccessor.makePutFromRegionInfo(copyOfMerged, time);
    putOfMerged.add(HConstants.CATALOG_FAMILY, HConstants.MERGEA_QUALIFIER,
      regionA.toByteArray());
    putOfMerged.add(HConstants.CATALOG_FAMILY, HConstants.MERGEB_QUALIFIER,
      regionB.toByteArray());
    mutations.add(putOfMerged);
    // Deletes for merging regions
    Delete deleteA = MetaTableAccessor.makeDeleteFromRegionInfo(regionA, time);
    Delete deleteB = MetaTableAccessor.makeDeleteFromRegionInfo(regionB, time);
    mutations.add(deleteA);
    mutations.add(deleteB);

    // Add empty locations for region replicas of the merged region so that number of replicas
    // can be cached whenever the primary region is looked up from meta
    for (int i = 1; i < regionReplication; i++) {
      addEmptyLocation(putOfMerged, i);
    }

    // The merged is a new region, openSeqNum = 1 is fine.
    addLocation(putOfMerged, serverName, 1);
  }

  public Put addLocation(final Put p, final ServerName sn, long openSeqNum) {
    p.add(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER, Bytes
        .toBytes(sn.getHostAndPort()));
    p.add(HConstants.CATALOG_FAMILY, HConstants.STARTCODE_QUALIFIER, Bytes.toBytes(sn
        .getStartcode()));
    p.add(HConstants.CATALOG_FAMILY, HConstants.SEQNUM_QUALIFIER, Bytes.toBytes(openSeqNum));
    return p;
  }

  private static Put addEmptyLocation(final Put p, int replicaId) {
    p.addImmutable(HConstants.CATALOG_FAMILY, MetaTableAccessor.getServerColumn(replicaId), null);
    p.addImmutable(HConstants.CATALOG_FAMILY, MetaTableAccessor.getStartCodeColumn(replicaId),
      null);
    p.addImmutable(HConstants.CATALOG_FAMILY, MetaTableAccessor.getSeqNumColumn(replicaId), null);
    return p;
  }

  public HRegion stepsBeforePONR(final Server server, final RegionServerServices services,
      boolean testing) throws IOException {
    if (rmd == null) {
      rmd = server != null && server.getCoordinatedStateManager() != null ?
          ((BaseCoordinatedStateManager) server.getCoordinatedStateManager())
            .getRegionMergeCoordination().getDefaultDetails()
        : null;
    }

    // If server doesn't have a coordination state manager, don't do coordination actions.
    if (useCoordination(server)) {
      try {
        ((BaseCoordinatedStateManager) server.getCoordinatedStateManager())
            .getRegionMergeCoordination().startRegionMergeTransaction(mergedRegionInfo,
              server.getServerName(), region_a.getRegionInfo(), region_b.getRegionInfo());
      } catch (IOException e) {
        throw new IOException("Failed to start region merge transaction for "
            + this.mergedRegionInfo.getRegionNameAsString(), e);
      }
    } else if (services != null && !useCoordinationForAssignment) {
      if (!services.reportRegionStateTransition(TransitionCode.READY_TO_MERGE,
          mergedRegionInfo, region_a.getRegionInfo(), region_b.getRegionInfo())) {
        throw new IOException("Failed to get ok from master to merge "
          + region_a.getRegionInfo().getRegionNameAsString() + " and "
          + region_b.getRegionInfo().getRegionNameAsString());
      }
    }
    transition(RegionMergeTransactionPhase.SET_MERGING);
    if (useCoordination(server)) {
      // After creating the merge node, wait for master to transition it
      // from PENDING_MERGE to MERGING so that we can move on. We want master
      // knows about it and won't transition any region which is merging.
      ((BaseCoordinatedStateManager) server.getCoordinatedStateManager())
        .getRegionMergeCoordination().waitForRegionMergeTransaction(services,
          mergedRegionInfo, region_a, region_b, rmd);
    }

    this.region_a.getRegionFileSystem().createMergesDir();
    transition(RegionMergeTransactionPhase.CREATED_MERGE_DIR);

    Map<byte[], List<StoreFile>> hstoreFilesOfRegionA = closeAndOfflineRegion(
        services, this.region_a, true, testing);
    Map<byte[], List<StoreFile>> hstoreFilesOfRegionB = closeAndOfflineRegion(
        services, this.region_b, false, testing);

    assert hstoreFilesOfRegionA != null && hstoreFilesOfRegionB != null;


    //
    // mergeStoreFiles creates merged region dirs under the region_a merges dir
    // Nothing to unroll here if failure -- clean up of CREATE_MERGE_DIR will
    // clean this up.
    mergeStoreFiles(hstoreFilesOfRegionA, hstoreFilesOfRegionB);

    if (useCoordination(server)) {
      try {
        // Do the final check in case any merging region is moved somehow. If so, the transition
        // will fail.
        ((BaseCoordinatedStateManager) server.getCoordinatedStateManager())
            .getRegionMergeCoordination().confirmRegionMergeTransaction(this.mergedRegionInfo,
              region_a.getRegionInfo(), region_b.getRegionInfo(), server.getServerName(), rmd);
      } catch (IOException e) {
        throw new IOException("Failed setting MERGING on "
            + this.mergedRegionInfo.getRegionNameAsString(), e);
      }
    }

    // Log to the journal that we are creating merged region. We could fail
    // halfway through. If we do, we could have left
    // stuff in fs that needs cleanup -- a storefile or two. Thats why we
    // add entry to journal BEFORE rather than AFTER the change.
    transition(RegionMergeTransactionPhase.STARTED_MERGED_REGION_CREATION);
    HRegion mergedRegion = createMergedRegionFromMerges(this.region_a,
        this.region_b, this.mergedRegionInfo);
    return mergedRegion;
  }

  /**
   * Create a merged region from the merges directory under region a. In order
   * to mock it for tests, place it with a new method.
   * @param a hri of region a
   * @param b hri of region b
   * @param mergedRegion hri of merged region
   * @return merged HRegion.
   * @throws IOException
   */
  HRegion createMergedRegionFromMerges(final HRegion a, final HRegion b,
      final HRegionInfo mergedRegion) throws IOException {
    return a.createMergedRegionFromMerges(mergedRegion, b);
  }

  /**
   * Close the merging region and offline it in regionserver
   * @param services
   * @param region
   * @param isRegionA true if it is merging region a, false if it is region b
   * @param testing true if it is testing
   * @return a map of family name to list of store files
   * @throws IOException
   */
  private Map<byte[], List<StoreFile>> closeAndOfflineRegion(
      final RegionServerServices services, final HRegion region,
      final boolean isRegionA, final boolean testing) throws IOException {
    Map<byte[], List<StoreFile>> hstoreFilesToMerge = null;
    Exception exceptionToThrow = null;
    try {
      hstoreFilesToMerge = region.close(false);
    } catch (Exception e) {
      exceptionToThrow = e;
    }
    if (exceptionToThrow == null && hstoreFilesToMerge == null) {
      // The region was closed by a concurrent thread. We can't continue
      // with the merge, instead we must just abandon the merge. If we
      // reopen or merge this could cause problems because the region has
      // probably already been moved to a different server, or is in the
      // process of moving to a different server.
      exceptionToThrow = closedByOtherException;
    }
    if (exceptionToThrow != closedByOtherException) {
      transition(isRegionA ? RegionMergeTransactionPhase.CLOSED_REGION_A
          : RegionMergeTransactionPhase.CLOSED_REGION_B);
    }
    if (exceptionToThrow != null) {
      if (exceptionToThrow instanceof IOException)
        throw (IOException) exceptionToThrow;
      throw new IOException(exceptionToThrow);
    }

    if (!testing) {
      services.removeFromOnlineRegions(region, null);
    }
    transition(isRegionA ? RegionMergeTransactionPhase.OFFLINED_REGION_A
        : RegionMergeTransactionPhase.OFFLINED_REGION_B);
    return hstoreFilesToMerge;
  }

  /**
   * Get merged region info through the specified two regions
   * @param a merging region A
   * @param b merging region B
   * @return the merged region info
   */
  public static HRegionInfo getMergedRegionInfo(final HRegionInfo a,
      final HRegionInfo b) {
    long rid = EnvironmentEdgeManager.currentTime();
    // Regionid is timestamp. Merged region's id can't be less than that of
    // merging regions else will insert at wrong location in hbase:meta
    if (rid < a.getRegionId() || rid < b.getRegionId()) {
      LOG.warn("Clock skew; merging regions id are " + a.getRegionId()
          + " and " + b.getRegionId() + ", but current time here is " + rid);
      rid = Math.max(a.getRegionId(), b.getRegionId()) + 1;
    }

    byte[] startKey = null;
    byte[] endKey = null;
    // Choose the smaller as start key
    if (a.compareTo(b) <= 0) {
      startKey = a.getStartKey();
    } else {
      startKey = b.getStartKey();
    }
    // Choose the bigger as end key
    if (Bytes.equals(a.getEndKey(), HConstants.EMPTY_BYTE_ARRAY)
        || (!Bytes.equals(b.getEndKey(), HConstants.EMPTY_BYTE_ARRAY)
            && Bytes.compareTo(a.getEndKey(), b.getEndKey()) > 0)) {
      endKey = a.getEndKey();
    } else {
      endKey = b.getEndKey();
    }

    // Merged region is sorted between two merging regions in META
    HRegionInfo mergedRegionInfo = new HRegionInfo(a.getTable(), startKey,
        endKey, false, rid);
    return mergedRegionInfo;
  }

  /**
   * Perform time consuming opening of the merged region.
   * @param server Hosting server instance. Can be null when testing
   * @param services Used to online/offline regions.
   * @param merged the merged region
   * @throws IOException If thrown, transaction failed. Call
   *           {@link #rollback(Server, RegionServerServices)}
   */
  void openMergedRegion(final Server server,
      final RegionServerServices services, HRegion merged) throws IOException {
    boolean stopped = server != null && server.isStopped();
    boolean stopping = services != null && services.isStopping();
    if (stopped || stopping) {
      LOG.info("Not opening merged region  " + merged.getRegionInfo().getRegionNameAsString()
          + " because stopping=" + stopping + ", stopped=" + stopped);
      return;
    }
    HRegionInfo hri = merged.getRegionInfo();
    LoggingProgressable reporter = server == null ? null
        : new LoggingProgressable(hri, server.getConfiguration().getLong(
            "hbase.regionserver.regionmerge.open.log.interval", 10000));
    merged.openHRegion(reporter);

    if (services != null) {
      try {
        if (useCoordinationForAssignment) {
          services.postOpenDeployTasks(merged);
        } else if (!services.reportRegionStateTransition(TransitionCode.MERGED,
            mergedRegionInfo, region_a.getRegionInfo(), region_b.getRegionInfo())) {
          throw new IOException("Failed to report merged region to master: "
            + mergedRegionInfo.getShortNameToLog());
        }
        services.addToOnlineRegions(merged);
      } catch (KeeperException ke) {
        throw new IOException(ke);
      }
    }

  }

  /**
   * Create reference file(s) of merging regions under the region_a merges dir
   * @param hstoreFilesOfRegionA
   * @param hstoreFilesOfRegionB
   * @throws IOException
   */
  private void mergeStoreFiles(
      Map<byte[], List<StoreFile>> hstoreFilesOfRegionA,
      Map<byte[], List<StoreFile>> hstoreFilesOfRegionB)
      throws IOException {
    // Create reference file(s) of region A in mergdir
    HRegionFileSystem fs_a = this.region_a.getRegionFileSystem();
    for (Map.Entry<byte[], List<StoreFile>> entry : hstoreFilesOfRegionA
        .entrySet()) {
      String familyName = Bytes.toString(entry.getKey());
      for (StoreFile storeFile : entry.getValue()) {
        fs_a.mergeStoreFile(this.mergedRegionInfo, familyName, storeFile,
            this.mergesdir);
      }
    }
    // Create reference file(s) of region B in mergedir
    HRegionFileSystem fs_b = this.region_b.getRegionFileSystem();
    for (Map.Entry<byte[], List<StoreFile>> entry : hstoreFilesOfRegionB
        .entrySet()) {
      String familyName = Bytes.toString(entry.getKey());
      for (StoreFile storeFile : entry.getValue()) {
        fs_b.mergeStoreFile(this.mergedRegionInfo, familyName, storeFile,
            this.mergesdir);
      }
    }
  }

  /**
   * @param server Hosting server instance (May be null when testing).
   * @param services Services of regionserver, used to online regions.
   * @throws IOException If thrown, rollback failed. Take drastic action.
   * @return True if we successfully rolled back, false if we got to the point
   *         of no return and so now need to abort the server to minimize
   *         damage.
   */
  @Override
  @SuppressWarnings("deprecation")
  public boolean rollback(final Server server,
      final RegionServerServices services) throws IOException {
    if (User.isHBaseSecurityEnabled(region_a.getBaseConf())) {
      LOG.warn("Should use execute(Server, RegionServerServices, User)");
    }
    return rollback(server, services, null);
  }

  @Override
  public boolean rollback(final Server server,
      final RegionServerServices services, User user) throws IOException {
    assert this.mergedRegionInfo != null;
    // Coprocessor callback
    if (rsCoprocessorHost != null) {
      if (user == null) {
        rsCoprocessorHost.preRollBackMerge(region_a, region_b);
      } else {
        try {
          user.getUGI().doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
              rsCoprocessorHost.preRollBackMerge(region_a, region_b);
              return null;
            }
          });
        } catch (InterruptedException ie) {
          InterruptedIOException iioe = new InterruptedIOException();
          iioe.initCause(ie);
          throw iioe;
        }
      }
    }

    boolean result = true;
    ListIterator<JournalEntry> iterator = this.journal
        .listIterator(this.journal.size());
    // Iterate in reverse.
    while (iterator.hasPrevious()) {
      JournalEntry je = iterator.previous();

      transition(je.getPhase(), true);

      switch (je.getPhase()) {

        case SET_MERGING:
        if (useCoordination(server)) {
          ((BaseCoordinatedStateManager) server.getCoordinatedStateManager())
              .getRegionMergeCoordination().clean(this.mergedRegionInfo);
          } else if (services != null && !useCoordinationForAssignment
              && !services.reportRegionStateTransition(TransitionCode.MERGE_REVERTED,
                  mergedRegionInfo, region_a.getRegionInfo(), region_b.getRegionInfo())) {
            return false;
        }
          break;

        case CREATED_MERGE_DIR:
          this.region_a.writestate.writesEnabled = true;
          this.region_b.writestate.writesEnabled = true;
          this.region_a.getRegionFileSystem().cleanupMergesDir();
          break;

        case CLOSED_REGION_A:
          try {
            // So, this returns a seqid but if we just closed and then reopened,
            // we should be ok. On close, we flushed using sequenceid obtained
            // from hosting regionserver so no need to propagate the sequenceid
            // returned out of initialize below up into regionserver as we
            // normally do.
            this.region_a.initialize();
          } catch (IOException e) {
            LOG.error("Failed rollbacking CLOSED_REGION_A of region "
                + this.region_a.getRegionInfo().getRegionNameAsString(), e);
            throw new RuntimeException(e);
          }
          break;

        case OFFLINED_REGION_A:
          if (services != null)
            services.addToOnlineRegions(this.region_a);
          break;

        case CLOSED_REGION_B:
          try {
            this.region_b.initialize();
          } catch (IOException e) {
            LOG.error("Failed rollbacking CLOSED_REGION_A of region "
                + this.region_b.getRegionInfo().getRegionNameAsString(), e);
            throw new RuntimeException(e);
          }
          break;

        case OFFLINED_REGION_B:
          if (services != null)
            services.addToOnlineRegions(this.region_b);
          break;

        case STARTED_MERGED_REGION_CREATION:
          this.region_a.getRegionFileSystem().cleanupMergedRegion(
              this.mergedRegionInfo);
          break;

        case PONR:
          // We got to the point-of-no-return so we need to just abort. Return
          // immediately. Do not clean up created merged regions.
          return false;

        // Informational only cases
        case STARTED:
        case PREPARED:
        case COMPLETED:
          break;

        default:
          throw new RuntimeException("Unhandled journal entry: " + je);
      }
    }
    // Coprocessor callback
    if (rsCoprocessorHost != null) {
      if (user == null) {
        rsCoprocessorHost.postRollBackMerge(region_a, region_b);
      } else {
        try {
          user.getUGI().doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
              rsCoprocessorHost.postRollBackMerge(region_a, region_b);
              return null;
            }
          });
        } catch (InterruptedException ie) {
          InterruptedIOException iioe = new InterruptedIOException();
          iioe.initCause(ie);
          throw iioe;
        }
      }
    }

    return result;
  }

  @Override
  public HRegionInfo getMergedRegionInfo() {
    return this.mergedRegionInfo;
  }

  // For unit testing.
  Path getMergesDir() {
    return this.mergesdir;
  }

  private boolean useCoordination(final Server server) {
    return server != null && useCoordinationForAssignment
        && server.getCoordinatedStateManager() != null;
  }



  /**
   * Checks if the given region has merge qualifier in hbase:meta
   * @param services
   * @param regionName name of specified region
   * @return true if the given region has merge qualifier in META.(It will be
   *         cleaned by CatalogJanitor)
   * @throws IOException
   */
  boolean hasMergeQualifierInMeta(final RegionServerServices services,
      final byte[] regionName) throws IOException {
    if (services == null) return false;
    // Get merge regions if it is a merged region and already has merge
    // qualifier
    Pair<HRegionInfo, HRegionInfo> mergeRegions = MetaTableAccessor
        .getRegionsFromMergeQualifier(services.getConnection(), regionName);
    if (mergeRegions != null &&
        (mergeRegions.getFirst() != null || mergeRegions.getSecond() != null)) {
      // It has merge qualifier
      return true;
    }
    return false;
  }

  @Override
  public List<JournalEntry> getJournal() {
    return journal;
  }

  @Override
  public RegionMergeTransaction registerTransactionListener(TransactionListener listener) {
    listeners.add(listener);
    return this;
  }

  @Override
  public Server getServer() {
    return server;
  }

  @Override
  public RegionServerServices getRegionServerServices() {
    return rsServices;
  }

}
