/**
 *
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
package org.apache.hadoop.hbase.regionserver;

import static org.apache.hadoop.hbase.executor.EventType.RS_ZK_REQUEST_REGION_SPLIT;
import static org.apache.hadoop.hbase.executor.EventType.RS_ZK_REGION_SPLIT;
import static org.apache.hadoop.hbase.executor.EventType.RS_ZK_REGION_SPLITTING;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RegionTransition;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaEditor;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.ConfigUtil;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HasThread;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.data.Stat;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Executes region split as a "transaction".  Call {@link #prepare()} to setup
 * the transaction, {@link #execute(Server, RegionServerServices)} to run the
 * transaction and {@link #rollback(Server, RegionServerServices)} to cleanup if execute fails.
 *
 * <p>Here is an example of how you would use this class:
 * <pre>
 *  SplitTransaction st = new SplitTransaction(this.conf, parent, midKey)
 *  if (!st.prepare()) return;
 *  try {
 *    st.execute(server, services);
 *  } catch (IOException ioe) {
 *    try {
 *      st.rollback(server, services);
 *      return;
 *    } catch (RuntimeException e) {
 *      myAbortable.abort("Failed split, abort");
 *    }
 *  }
 * </Pre>
 * <p>This class is not thread safe.  Caller needs ensure split is run by
 * one thread only.
 */
@InterfaceAudience.Private
public class SplitTransaction {
  private static final Log LOG = LogFactory.getLog(SplitTransaction.class);

  /*
   * Region to split
   */
  private final HRegion parent;
  private HRegionInfo hri_a;
  private HRegionInfo hri_b;
  private long fileSplitTimeout = 30000;
  private int znodeVersion = -1;
  boolean useZKForAssignment;

  /*
   * Row to split around
   */
  private final byte [] splitrow;

  /**
   * Types to add to the transaction journal.
   * Each enum is a step in the split transaction. Used to figure how much
   * we need to rollback.
   */
  static enum JournalEntryType {
    /**
     * Started
     */
    STARTED,
    /**
     * Prepared (after table lock)
     */
    PREPARED,
    /**
     * Before preSplit coprocessor hook
     */
    BEFORE_PRE_SPLIT_HOOK,
    /**
     * After preSplit coprocessor hook
     */
    AFTER_PRE_SPLIT_HOOK,
    /**
     * Set region as in transition, set it into SPLITTING state.
     */
    SET_SPLITTING_IN_ZK,
    /**
     * We created the temporary split data directory.
     */
    CREATE_SPLIT_DIR,
    /**
     * Closed the parent region.
     */
    CLOSED_PARENT_REGION,
    /**
     * The parent has been taken out of the server's online regions list.
     */
    OFFLINED_PARENT,
    /**
     * Started in on creation of the first daughter region.
     */
    STARTED_REGION_A_CREATION,
    /**
     * Started in on the creation of the second daughter region.
     */
    STARTED_REGION_B_CREATION,
    /**
     * Opened the first daughter region
     */
    OPENED_REGION_A,
    /**
     * Opened the second daughter region
     */
    OPENED_REGION_B,
    /**
     * Before postSplit coprocessor hook
     */
    BEFORE_POST_SPLIT_HOOK,
    /**
     * After postSplit coprocessor hook
     */
    AFTER_POST_SPLIT_HOOK,
    /**
     * Point of no return.
     * If we got here, then transaction is not recoverable other than by
     * crashing out the regionserver.
     */
    PONR
  }

  static class JournalEntry {
    private JournalEntryType type;
    private long timestamp;

    public JournalEntry(JournalEntryType type) {
      this(type, EnvironmentEdgeManager.currentTimeMillis());
    }

    public JournalEntry(JournalEntryType type, long timestamp) {
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
  }

  /*
   * Journal of how far the split transaction has progressed.
   */
  private final List<JournalEntry> journal = new ArrayList<JournalEntry>();

  /**
   * Constructor
   * @param r Region to split
   * @param splitrow Row to split around
   */
  public SplitTransaction(final HRegion r, final byte [] splitrow) {
    this.parent = r;
    this.splitrow = splitrow;
    this.journal.add(new JournalEntry(JournalEntryType.STARTED));
    this.useZKForAssignment = ConfigUtil.useZKForAssignment(r.getBaseConf());
  }

  /**
   * Does checks on split inputs.
   * @return <code>true</code> if the region is splittable else
   * <code>false</code> if it is not (e.g. its already closed, etc.).
   */
  public boolean prepare() {
    if (!this.parent.isSplittable()) return false;
    // Split key can be null if this region is unsplittable; i.e. has refs.
    if (this.splitrow == null) return false;
    HRegionInfo hri = this.parent.getRegionInfo();
    parent.prepareToSplit();
    // Check splitrow.
    byte [] startKey = hri.getStartKey();
    byte [] endKey = hri.getEndKey();
    if (Bytes.equals(startKey, splitrow) ||
        !this.parent.getRegionInfo().containsRow(splitrow)) {
      LOG.info("Split row is not inside region key range or is equal to " +
          "startkey: " + Bytes.toStringBinary(this.splitrow));
      return false;
    }
    long rid = getDaughterRegionIdTimestamp(hri);
    this.hri_a = new HRegionInfo(hri.getTable(), startKey, this.splitrow, false, rid);
    this.hri_b = new HRegionInfo(hri.getTable(), this.splitrow, endKey, false, rid);
    this.journal.add(new JournalEntry(JournalEntryType.PREPARED));
    return true;
  }

  /**
   * Calculate daughter regionid to use.
   * @param hri Parent {@link HRegionInfo}
   * @return Daughter region id (timestamp) to use.
   */
  private static long getDaughterRegionIdTimestamp(final HRegionInfo hri) {
    long rid = EnvironmentEdgeManager.currentTimeMillis();
    // Regionid is timestamp.  Can't be less than that of parent else will insert
    // at wrong location in hbase:meta (See HBASE-710).
    if (rid < hri.getRegionId()) {
      LOG.warn("Clock skew; parent regions id is " + hri.getRegionId() +
        " but current time here is " + rid);
      rid = hri.getRegionId() + 1;
    }
    return rid;
  }

  private static IOException closedByOtherException = new IOException(
      "Failed to close region: already closed by another thread");

  /**
   * Prepare the regions and region files.
   * @param server Hosting server instance.  Can be null when testing (won't try
   * and update in zk if a null server)
   * @param services Used to online/offline regions.
   * @throws IOException If thrown, transaction failed.
   *    Call {@link #rollback(Server, RegionServerServices)}
   * @return Regions created
   */
  /* package */PairOfSameType<HRegion> createDaughters(final Server server,
      final RegionServerServices services) throws IOException {
    LOG.info("Starting split of region " + this.parent);
    if ((server != null && server.isStopped()) ||
        (services != null && services.isStopping())) {
      throw new IOException("Server is stopped or stopping");
    }
    assert !this.parent.lock.writeLock().isHeldByCurrentThread():
      "Unsafe to hold write lock while performing RPCs";

    journal.add(new JournalEntry(JournalEntryType.BEFORE_PRE_SPLIT_HOOK));

    // Coprocessor callback
    if (this.parent.getCoprocessorHost() != null) {
      // TODO: Remove one of these
      this.parent.getCoprocessorHost().preSplit();
      this.parent.getCoprocessorHost().preSplit(this.splitrow);
    }

    journal.add(new JournalEntry(JournalEntryType.AFTER_PRE_SPLIT_HOOK));

    // If true, no cluster to write meta edits to or to update znodes in.
    boolean testing = server == null? true:
        server.getConfiguration().getBoolean("hbase.testing.nocluster", false);
    this.fileSplitTimeout = testing ? this.fileSplitTimeout :
        server.getConfiguration().getLong("hbase.regionserver.fileSplitTimeout",
          this.fileSplitTimeout);

    PairOfSameType<HRegion> daughterRegions = stepsBeforePONR(server, services, testing);

    List<Mutation> metaEntries = new ArrayList<Mutation>();
    if (this.parent.getCoprocessorHost() != null) {
      if (this.parent.getCoprocessorHost().
          preSplitBeforePONR(this.splitrow, metaEntries)) {
        throw new IOException("Coprocessor bypassing region "
            + this.parent.getRegionNameAsString() + " split.");
      }
      try {
        for (Mutation p : metaEntries) {
          HRegionInfo.parseRegionName(p.getRow());
        }
      } catch (IOException e) {
        LOG.error("Row key of mutation from coprossor is not parsable as region name."
            + "Mutations from coprocessor should only for hbase:meta table.");
        throw e;
      }
    }

    // This is the point of no return.  Adding subsequent edits to .META. as we
    // do below when we do the daughter opens adding each to .META. can fail in
    // various interesting ways the most interesting of which is a timeout
    // BUT the edits all go through (See HBASE-3872).  IF we reach the PONR
    // then subsequent failures need to crash out this regionserver; the
    // server shutdown processing should be able to fix-up the incomplete split.
    // The offlined parent will have the daughters as extra columns.  If
    // we leave the daughter regions in place and do not remove them when we
    // crash out, then they will have their references to the parent in place
    // still and the server shutdown fixup of .META. will point to these
    // regions.
    // We should add PONR JournalEntry before offlineParentInMeta,so even if
    // OfflineParentInMeta timeout,this will cause regionserver exit,and then
    // master ServerShutdownHandler will fix daughter & avoid data loss. (See
    // HBase-4562).
    this.journal.add(new JournalEntry(JournalEntryType.PONR));

    // Edit parent in meta.  Offlines parent region and adds splita and splitb
    // as an atomic update. See HBASE-7721. This update to META makes the region
    // will determine whether the region is split or not in case of failures.
    // If it is successful, master will roll-forward, if not, master will rollback
    // and assign the parent region.
    if (!testing && useZKForAssignment) {
      if (metaEntries == null || metaEntries.isEmpty()) {
        MetaEditor.splitRegion(server.getCatalogTracker(), parent.getRegionInfo(), daughterRegions
            .getFirst().getRegionInfo(), daughterRegions.getSecond().getRegionInfo(), server
            .getServerName());
      } else {
        offlineParentInMetaAndputMetaEntries(server.getCatalogTracker(), parent.getRegionInfo(),
          daughterRegions.getFirst().getRegionInfo(), daughterRegions.getSecond().getRegionInfo(),
          server.getServerName(), metaEntries);
      }
    } else if (services != null && !useZKForAssignment) {
      if (!services.reportRegionStateTransition(TransitionCode.SPLIT_PONR, parent.getRegionInfo(),
        hri_a, hri_b)) {
        // Passed PONR, let SSH clean it up
        throw new IOException("Failed to notify master that split passed PONR: "
            + parent.getRegionInfo().getRegionNameAsString());
      }
    }
    return daughterRegions;
  }

  public PairOfSameType<HRegion> stepsBeforePONR(final Server server,
      final RegionServerServices services, boolean testing) throws IOException {
    // Set ephemeral SPLITTING znode up in zk.  Mocked servers sometimes don't
    // have zookeeper so don't do zk stuff if server or zookeeper is null
    if (server != null && server.getZooKeeper() != null && useZKForAssignment) {
      try {
        createNodeSplitting(server.getZooKeeper(),
          parent.getRegionInfo(), server.getServerName(), hri_a, hri_b);
      } catch (KeeperException e) {
        throw new IOException("Failed creating PENDING_SPLIT znode on " +
          this.parent.getRegionNameAsString(), e);
      }
    } else if (services != null && !useZKForAssignment) {
      if (!services.reportRegionStateTransition(TransitionCode.READY_TO_SPLIT,
        parent.getRegionInfo(), hri_a, hri_b)) {
        throw new IOException("Failed to get ok from master to split "
            + parent.getRegionNameAsString());
      }
    }
    this.journal.add(new JournalEntry(JournalEntryType.SET_SPLITTING_IN_ZK));
    if (server != null && server.getZooKeeper() != null && useZKForAssignment) {
      // After creating the split node, wait for master to transition it
      // from PENDING_SPLIT to SPLITTING so that we can move on. We want master
      // knows about it and won't transition any region which is splitting.
      znodeVersion = getZKNode(server, services);
    }

    this.parent.getRegionFileSystem().createSplitsDir();
    this.journal.add(new JournalEntry(JournalEntryType.CREATE_SPLIT_DIR));

    Map<byte[], List<StoreFile>> hstoreFilesToSplit = null;
    Exception exceptionToThrow = null;
    try{
      hstoreFilesToSplit = this.parent.close(false);
    } catch (Exception e) {
      exceptionToThrow = e;
    }
    if (exceptionToThrow == null && hstoreFilesToSplit == null) {
      // The region was closed by a concurrent thread.  We can't continue
      // with the split, instead we must just abandon the split.  If we
      // reopen or split this could cause problems because the region has
      // probably already been moved to a different server, or is in the
      // process of moving to a different server.
      exceptionToThrow = closedByOtherException;
    }
    if (exceptionToThrow != closedByOtherException) {
      this.journal.add(new JournalEntry(JournalEntryType.CLOSED_PARENT_REGION));
    }
    if (exceptionToThrow != null) {
      if (exceptionToThrow instanceof IOException) throw (IOException)exceptionToThrow;
      throw new IOException(exceptionToThrow);
    }
    if (!testing) {
      services.removeFromOnlineRegions(this.parent, null);
    }
    this.journal.add(new JournalEntry(JournalEntryType.OFFLINED_PARENT));

    // TODO: If splitStoreFiles were multithreaded would we complete steps in
    // less elapsed time?  St.Ack 20100920
    //
    // splitStoreFiles creates daughter region dirs under the parent splits dir
    // Nothing to unroll here if failure -- clean up of CREATE_SPLIT_DIR will
    // clean this up.
    Pair<Integer, Integer> expectedReferences = splitStoreFiles(hstoreFilesToSplit);

    // Log to the journal that we are creating region A, the first daughter
    // region.  We could fail halfway through.  If we do, we could have left
    // stuff in fs that needs cleanup -- a storefile or two.  Thats why we
    // add entry to journal BEFORE rather than AFTER the change.
    this.journal.add(new JournalEntry(JournalEntryType.STARTED_REGION_A_CREATION));
    assertReferenceFileCount(expectedReferences.getFirst(),
        this.parent.getRegionFileSystem().getSplitsDir(this.hri_a));
    HRegion a = this.parent.createDaughterRegionFromSplits(this.hri_a);
    assertReferenceFileCount(expectedReferences.getFirst(),
        new Path(this.parent.getRegionFileSystem().getTableDir(), this.hri_a.getEncodedName()));

    // Ditto
    this.journal.add(new JournalEntry(JournalEntryType.STARTED_REGION_B_CREATION));
    assertReferenceFileCount(expectedReferences.getSecond(),
        this.parent.getRegionFileSystem().getSplitsDir(this.hri_b));
    HRegion b = this.parent.createDaughterRegionFromSplits(this.hri_b);
    assertReferenceFileCount(expectedReferences.getSecond(),
        new Path(this.parent.getRegionFileSystem().getTableDir(), this.hri_b.getEncodedName()));

    return new PairOfSameType<HRegion>(a, b);
  }

  void assertReferenceFileCount(int expectedReferenceFileCount, Path dir)
      throws IOException {
    if (expectedReferenceFileCount != 0 &&
        expectedReferenceFileCount != FSUtils.getRegionReferenceFileCount(this.parent.getFilesystem(), dir)) {
      throw new IOException("Failing split. Expected reference file count isn't equal.");
    }
  }

  /**
   * Perform time consuming opening of the daughter regions.
   * @param server Hosting server instance.  Can be null when testing (won't try
   * and update in zk if a null server)
   * @param services Used to online/offline regions.
   * @param a first daughter region
   * @param a second daughter region
   * @throws IOException If thrown, transaction failed.
   *          Call {@link #rollback(Server, RegionServerServices)}
   */
  /* package */void openDaughters(final Server server,
      final RegionServerServices services, HRegion a, HRegion b)
      throws IOException {
    boolean stopped = server != null && server.isStopped();
    boolean stopping = services != null && services.isStopping();
    // TODO: Is this check needed here?
    if (stopped || stopping) {
      LOG.info("Not opening daughters " +
          b.getRegionInfo().getRegionNameAsString() +
          " and " +
          a.getRegionInfo().getRegionNameAsString() +
          " because stopping=" + stopping + ", stopped=" + stopped);
    } else {
      // Open daughters in parallel.
      DaughterOpener aOpener = new DaughterOpener(server, a);
      DaughterOpener bOpener = new DaughterOpener(server, b);
      aOpener.start();
      bOpener.start();
      try {
        aOpener.join();
        if (aOpener.getException() == null) {
          journal.add(new JournalEntry(JournalEntryType.OPENED_REGION_A));
        }
        bOpener.join();
        if (bOpener.getException() == null) {
          journal.add(new JournalEntry(JournalEntryType.OPENED_REGION_B));
        }
      } catch (InterruptedException e) {
        throw (InterruptedIOException)new InterruptedIOException().initCause(e);
      }
      if (aOpener.getException() != null) {
        throw new IOException("Failed " +
          aOpener.getName(), aOpener.getException());
      }
      if (bOpener.getException() != null) {
        throw new IOException("Failed " +
          bOpener.getName(), bOpener.getException());
      }
      if (services != null) {
        try {
          if (useZKForAssignment) {
            // add 2nd daughter first (see HBASE-4335)
            services.postOpenDeployTasks(b, server.getCatalogTracker());
          } else if (!services.reportRegionStateTransition(TransitionCode.SPLIT,
              parent.getRegionInfo(), hri_a, hri_b)) {
            throw new IOException("Failed to report split region to master: "
              + parent.getRegionInfo().getShortNameToLog());
          }
          // Should add it to OnlineRegions
          services.addToOnlineRegions(b);
          if (useZKForAssignment) {
            services.postOpenDeployTasks(a, server.getCatalogTracker());
          }
          services.addToOnlineRegions(a);
        } catch (KeeperException ke) {
          throw new IOException(ke);
        }
      }
    }
  }

  /**
   * Finish off split transaction, transition the zknode
   * @param server Hosting server instance.  Can be null when testing (won't try
   * and update in zk if a null server)
   * @param services Used to online/offline regions.
   * @param a first daughter region
   * @param a second daughter region
   * @throws IOException If thrown, transaction failed.
   *          Call {@link #rollback(Server, RegionServerServices)}
   */
  /* package */void transitionZKNode(final Server server,
      final RegionServerServices services, HRegion a, HRegion b)
      throws IOException {
    // Tell master about split by updating zk.  If we fail, abort.
    if (server != null && server.getZooKeeper() != null) {
      try {
        this.znodeVersion = transitionSplittingNode(server.getZooKeeper(),
          parent.getRegionInfo(), a.getRegionInfo(), b.getRegionInfo(),
          server.getServerName(), this.znodeVersion,
          RS_ZK_REGION_SPLITTING, RS_ZK_REGION_SPLIT);

        int spins = 0;
        // Now wait for the master to process the split. We know it's done
        // when the znode is deleted. The reason we keep tickling the znode is
        // that it's possible for the master to miss an event.
        do {
          if (spins % 10 == 0) {
            LOG.debug("Still waiting on the master to process the split for " +
                this.parent.getRegionInfo().getEncodedName());
          }
          Thread.sleep(100);
          // When this returns -1 it means the znode doesn't exist
          this.znodeVersion = transitionSplittingNode(server.getZooKeeper(),
            parent.getRegionInfo(), a.getRegionInfo(), b.getRegionInfo(),
            server.getServerName(), this.znodeVersion,
            RS_ZK_REGION_SPLIT, RS_ZK_REGION_SPLIT);
          spins++;
        } while (this.znodeVersion != -1 && !server.isStopped()
            && !services.isStopping());
      } catch (Exception e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        throw new IOException("Failed telling master about split", e);
      }
    }

    

    // Leaving here, the splitdir with its dross will be in place but since the
    // split was successful, just leave it; it'll be cleaned when parent is
    // deleted and cleaned up.
  }

  /**
   * Wait for the splitting node to be transitioned from pending_split
   * to splitting by master. That's how we are sure master has processed
   * the event and is good with us to move on. If we don't get any update,
   * we periodically transition the node so that master gets the callback.
   * If the node is removed or is not in pending_split state any more,
   * we abort the split.
   */
  private int getZKNode(final Server server,
      final RegionServerServices services) throws IOException {
    // Wait for the master to process the pending_split.
    try {
      int spins = 0;
      Stat stat = new Stat();
      ZooKeeperWatcher zkw = server.getZooKeeper();
      ServerName expectedServer = server.getServerName();
      String node = parent.getRegionInfo().getEncodedName();
      while (!(server.isStopped() || services.isStopping())) {
        if (spins % 5 == 0) {
          LOG.debug("Still waiting for master to process "
            + "the pending_split for " + node);
          transitionSplittingNode(zkw, parent.getRegionInfo(),
            hri_a, hri_b, expectedServer, -1, RS_ZK_REQUEST_REGION_SPLIT,
            RS_ZK_REQUEST_REGION_SPLIT);
        }
        Thread.sleep(100);
        spins++;
        byte [] data = ZKAssign.getDataNoWatch(zkw, node, stat);
        if (data == null) {
          throw new IOException("Data is null, splitting node "
            + node + " no longer exists");
        }
        RegionTransition rt = RegionTransition.parseFrom(data);
        EventType et = rt.getEventType();
        if (et == RS_ZK_REGION_SPLITTING) {
          ServerName serverName = rt.getServerName();
          if (!serverName.equals(expectedServer)) {
            throw new IOException("Splitting node " + node + " is for "
              + serverName + ", not us " + expectedServer);
          }
          byte [] payloadOfSplitting = rt.getPayload();
          List<HRegionInfo> splittingRegions = HRegionInfo.parseDelimitedFrom(
            payloadOfSplitting, 0, payloadOfSplitting.length);
          assert splittingRegions.size() == 2;
          HRegionInfo a = splittingRegions.get(0);
          HRegionInfo b = splittingRegions.get(1);
          if (!(hri_a.equals(a) && hri_b.equals(b))) {
            throw new IOException("Splitting node " + node + " is for " + a + ", "
              + b + ", not expected daughters: " + hri_a + ", " + hri_b);
          }
          // Master has processed it.
          return stat.getVersion();
        }
        if (et != RS_ZK_REQUEST_REGION_SPLIT) {
          throw new IOException("Splitting node " + node
            + " moved out of splitting to " + et);
        }
      }
      // Server is stopping/stopped
      throw new IOException("Server is "
        + (services.isStopping() ? "stopping" : "stopped"));
    } catch (Exception e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new IOException("Failed getting SPLITTING znode on "
        + parent.getRegionNameAsString(), e);
    }
  }

  /**
   * Run the transaction.
   * @param server Hosting server instance.  Can be null when testing (won't try
   * and update in zk if a null server)
   * @param services Used to online/offline regions.
   * @throws IOException If thrown, transaction failed.
   *          Call {@link #rollback(Server, RegionServerServices)}
   * @return Regions created
   * @throws IOException
   * @see #rollback(Server, RegionServerServices)
   */
  public PairOfSameType<HRegion> execute(final Server server,
      final RegionServerServices services)
  throws IOException {
    useZKForAssignment =
        server == null ? true : ConfigUtil.useZKForAssignment(server.getConfiguration());
    PairOfSameType<HRegion> regions = createDaughters(server, services);
    if (this.parent.getCoprocessorHost() != null) {
      this.parent.getCoprocessorHost().preSplitAfterPONR();
    }
    return stepsAfterPONR(server, services, regions);
  }

  public PairOfSameType<HRegion> stepsAfterPONR(final Server server,
      final RegionServerServices services, PairOfSameType<HRegion> regions)
      throws IOException {
    openDaughters(server, services, regions.getFirst(), regions.getSecond());
    if (server != null && server.getZooKeeper() != null && useZKForAssignment) {
      transitionZKNode(server, services, regions.getFirst(), regions.getSecond());
    }
    journal.add(new JournalEntry(JournalEntryType.BEFORE_POST_SPLIT_HOOK));
    // Coprocessor callback
    if (this.parent.getCoprocessorHost() != null) {
      this.parent.getCoprocessorHost().postSplit(regions.getFirst(), regions.getSecond());
    }
    journal.add(new JournalEntry(JournalEntryType.AFTER_POST_SPLIT_HOOK));
    return regions;
  }

  private void offlineParentInMetaAndputMetaEntries(CatalogTracker catalogTracker,
      HRegionInfo parent, HRegionInfo splitA, HRegionInfo splitB,
      ServerName serverName, List<Mutation> metaEntries) throws IOException {
    List<Mutation> mutations = metaEntries;
    HRegionInfo copyOfParent = new HRegionInfo(parent);
    copyOfParent.setOffline(true);
    copyOfParent.setSplit(true);

    //Put for parent
    Put putParent = MetaEditor.makePutFromRegionInfo(copyOfParent);
    MetaEditor.addDaughtersToPut(putParent, splitA, splitB);
    mutations.add(putParent);
    
    //Puts for daughters
    Put putA = MetaEditor.makePutFromRegionInfo(splitA);
    Put putB = MetaEditor.makePutFromRegionInfo(splitB);

    addLocation(putA, serverName, 1); //these are new regions, openSeqNum = 1 is fine.
    addLocation(putB, serverName, 1);
    mutations.add(putA);
    mutations.add(putB);
    MetaEditor.mutateMetaTable(catalogTracker, mutations);
  }

  public Put addLocation(final Put p, final ServerName sn, long openSeqNum) {
    p.addImmutable(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER,
      Bytes.toBytes(sn.getHostAndPort()));
    p.addImmutable(HConstants.CATALOG_FAMILY, HConstants.STARTCODE_QUALIFIER,
      Bytes.toBytes(sn.getStartcode()));
    p.addImmutable(HConstants.CATALOG_FAMILY, HConstants.SEQNUM_QUALIFIER,
        Bytes.toBytes(openSeqNum));
    return p;
  }

  /*
   * Open daughter region in its own thread.
   * If we fail, abort this hosting server.
   */
  class DaughterOpener extends HasThread {
    private final Server server;
    private final HRegion r;
    private Throwable t = null;

    DaughterOpener(final Server s, final HRegion r) {
      super((s == null? "null-services": s.getServerName()) +
        "-daughterOpener=" + r.getRegionInfo().getEncodedName());
      setDaemon(true);
      this.server = s;
      this.r = r;
    }

    /**
     * @return Null if open succeeded else exception that causes us fail open.
     * Call it after this thread exits else you may get wrong view on result.
     */
    Throwable getException() {
      return this.t;
    }

    @Override
    public void run() {
      try {
        openDaughterRegion(this.server, r);
      } catch (Throwable t) {
        this.t = t;
      }
    }
  }

  /**
   * Open daughter regions, add them to online list and update meta.
   * @param server
   * @param daughter
   * @throws IOException
   * @throws KeeperException
   */
  void openDaughterRegion(final Server server, final HRegion daughter)
  throws IOException, KeeperException {
    HRegionInfo hri = daughter.getRegionInfo();
    LoggingProgressable reporter = server == null ? null
        : new LoggingProgressable(hri, server.getConfiguration().getLong(
            "hbase.regionserver.split.daughter.open.log.interval", 10000));
    daughter.openHRegion(reporter);
  }

  static class LoggingProgressable implements CancelableProgressable {
    private final HRegionInfo hri;
    private long lastLog = -1;
    private final long interval;

    LoggingProgressable(final HRegionInfo hri, final long interval) {
      this.hri = hri;
      this.interval = interval;
    }

    @Override
    public boolean progress() {
      long now = EnvironmentEdgeManager.currentTimeMillis();
      if (now - lastLog > this.interval) {
        LOG.info("Opening " + this.hri.getRegionNameAsString());
        this.lastLog = now;
      }
      return true;
    }
  }


  /**
   * Creates reference files for top and bottom half of the
   * @param hstoreFilesToSplit map of store files to create half file references for.
   * @return the number of reference files that were created.
   * @throws IOException
   */
  private Pair<Integer, Integer> splitStoreFiles(
      final Map<byte[], List<StoreFile>> hstoreFilesToSplit)
      throws IOException {
    if (hstoreFilesToSplit == null) {
      // Could be null because close didn't succeed -- for now consider it fatal
      throw new IOException("Close returned empty list of StoreFiles");
    }
    // The following code sets up a thread pool executor with as many slots as
    // there's files to split. It then fires up everything, waits for
    // completion and finally checks for any exception
    int nbFiles = hstoreFilesToSplit.size();
    if (nbFiles == 0) {
      // no file needs to be splitted.
      return new Pair<Integer, Integer>(0,0);
    }
    LOG.info("Preparing to split " + nbFiles + " storefiles for region " + this.parent);
    ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
    builder.setNameFormat("StoreFileSplitter-%1$d");
    ThreadFactory factory = builder.build();
    ThreadPoolExecutor threadPool =
      (ThreadPoolExecutor) Executors.newFixedThreadPool(nbFiles, factory);
    List<Future<Pair<Path,Path>>> futures = new ArrayList<Future<Pair<Path,Path>>> (nbFiles);

    // Split each store file.
    for (Map.Entry<byte[], List<StoreFile>> entry: hstoreFilesToSplit.entrySet()) {
      for (StoreFile sf: entry.getValue()) {
        StoreFileSplitter sfs = new StoreFileSplitter(entry.getKey(), sf);
        futures.add(threadPool.submit(sfs));
      }
    }
    // Shutdown the pool
    threadPool.shutdown();

    // Wait for all the tasks to finish
    try {
      boolean stillRunning = !threadPool.awaitTermination(
          this.fileSplitTimeout, TimeUnit.MILLISECONDS);
      if (stillRunning) {
        threadPool.shutdownNow();
        // wait for the thread to shutdown completely.
        while (!threadPool.isTerminated()) {
          Thread.sleep(50);
        }
        throw new IOException("Took too long to split the" +
            " files and create the references, aborting split");
      }
    } catch (InterruptedException e) {
      throw (InterruptedIOException)new InterruptedIOException().initCause(e);
    }

    int created_a = 0;
    int created_b = 0;
    // Look for any exception
    for (Future<Pair<Path, Path>> future : futures) {
      try {
        Pair<Path, Path> p = future.get();
        created_a += p.getFirst() != null ? 1 : 0;
        created_b += p.getSecond() != null ? 1 : 0;
      } catch (InterruptedException e) {
        throw (InterruptedIOException) new InterruptedIOException().initCause(e);
      } catch (ExecutionException e) {
        throw new IOException(e);
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Split storefiles for region " + this.parent + " Daugther A: " + created_a
          + " storefiles, Daugther B: " + created_b + " storefiles.");
    }
    return new Pair<Integer, Integer>(created_a, created_b);
  }

  private Pair<Path, Path> splitStoreFile(final byte[] family, final StoreFile sf) throws IOException {
    HRegionFileSystem fs = this.parent.getRegionFileSystem();
    String familyName = Bytes.toString(family);
    Path path_a =
        fs.splitStoreFile(this.hri_a, familyName, sf, this.splitrow, false,
          this.parent.getSplitPolicy());
    Path path_b =
        fs.splitStoreFile(this.hri_b, familyName, sf, this.splitrow, true,
          this.parent.getSplitPolicy());
    return new Pair<Path,Path>(path_a, path_b);
  }

  /**
   * Utility class used to do the file splitting / reference writing
   * in parallel instead of sequentially.
   */
  class StoreFileSplitter implements Callable<Pair<Path,Path>> {
    private final byte[] family;
    private final StoreFile sf;

    /**
     * Constructor that takes what it needs to split
     * @param family Family that contains the store file
     * @param sf which file
     */
    public StoreFileSplitter(final byte[] family, final StoreFile sf) {
      this.sf = sf;
      this.family = family;
    }

    public Pair<Path,Path> call() throws IOException {
      return splitStoreFile(family, sf);
    }
  }

  /**
   * @param server Hosting server instance (May be null when testing).
   * @param services
   * @throws IOException If thrown, rollback failed.  Take drastic action.
   * @return True if we successfully rolled back, false if we got to the point
   * of no return and so now need to abort the server to minimize damage.
   */
  @SuppressWarnings("deprecation")
  public boolean rollback(final Server server, final RegionServerServices services)
  throws IOException {
    // Coprocessor callback
    if (this.parent.getCoprocessorHost() != null) {
      this.parent.getCoprocessorHost().preRollBackSplit();
    }

    boolean result = true;
    ListIterator<JournalEntry> iterator =
      this.journal.listIterator(this.journal.size());
    // Iterate in reverse.
    while (iterator.hasPrevious()) {
      JournalEntry je = iterator.previous();
      switch(je.type) {

      case SET_SPLITTING_IN_ZK:
        if (server != null && server.getZooKeeper() != null && useZKForAssignment) {
          cleanZK(server, this.parent.getRegionInfo());
        } else if (services != null
            && !useZKForAssignment
            && !services.reportRegionStateTransition(TransitionCode.SPLIT_REVERTED,
              parent.getRegionInfo(), hri_a, hri_b)) {
          return false;
        }
        break;

      case CREATE_SPLIT_DIR:
        this.parent.writestate.writesEnabled = true;
        this.parent.getRegionFileSystem().cleanupSplitsDir();
        break;

      case CLOSED_PARENT_REGION:
        try {
          // So, this returns a seqid but if we just closed and then reopened, we
          // should be ok. On close, we flushed using sequenceid obtained from
          // hosting regionserver so no need to propagate the sequenceid returned
          // out of initialize below up into regionserver as we normally do.
          // TODO: Verify.
          this.parent.initialize();
        } catch (IOException e) {
          LOG.error("Failed rollbacking CLOSED_PARENT_REGION of region " +
            this.parent.getRegionNameAsString(), e);
          throw new RuntimeException(e);
        }
        break;

      case STARTED_REGION_A_CREATION:
        this.parent.getRegionFileSystem().cleanupDaughterRegion(this.hri_a);
        break;

      case STARTED_REGION_B_CREATION:
        this.parent.getRegionFileSystem().cleanupDaughterRegion(this.hri_b);
        break;

      case OFFLINED_PARENT:
        if (services != null) services.addToOnlineRegions(this.parent);
        break;

      case PONR:
        // We got to the point-of-no-return so we need to just abort. Return
        // immediately.  Do not clean up created daughter regions.  They need
        // to be in place so we don't delete the parent region mistakenly.
        // See HBASE-3872.
        return false;

      // Informational only cases
      case STARTED:
      case PREPARED:
      case BEFORE_PRE_SPLIT_HOOK:
      case AFTER_PRE_SPLIT_HOOK:
      case BEFORE_POST_SPLIT_HOOK:
      case AFTER_POST_SPLIT_HOOK:
      case OPENED_REGION_A:
      case OPENED_REGION_B:
        break;

      default:
        throw new RuntimeException("Unhandled journal entry: " + je);
      }
    }
    // Coprocessor callback
    if (this.parent.getCoprocessorHost() != null) {
      this.parent.getCoprocessorHost().postRollBackSplit();
    }
    return result;
  }

  HRegionInfo getFirstDaughter() {
    return hri_a;
  }

  HRegionInfo getSecondDaughter() {
    return hri_b;
  }

  private static void cleanZK(final Server server, final HRegionInfo hri) {
    try {
      // Only delete if its in expected state; could have been hijacked.
      if (!ZKAssign.deleteNode(server.getZooKeeper(), hri.getEncodedName(),
          RS_ZK_REQUEST_REGION_SPLIT, server.getServerName())) {
        ZKAssign.deleteNode(server.getZooKeeper(), hri.getEncodedName(),
          RS_ZK_REGION_SPLITTING, server.getServerName());
      }
    } catch (KeeperException.NoNodeException e) {
      LOG.info("Failed cleanup zk node of " + hri.getRegionNameAsString(), e);
    } catch (KeeperException e) {
      server.abort("Failed cleanup of " + hri.getRegionNameAsString(), e);
    }
  }

  /**
   * Creates a new ephemeral node in the PENDING_SPLIT state for the specified region.
   * Create it ephemeral in case regionserver dies mid-split.
   *
   * <p>Does not transition nodes from other states.  If a node already exists
   * for this region, a {@link NodeExistsException} will be thrown.
   *
   * @param zkw zk reference
   * @param region region to be created as offline
   * @param serverName server event originates from
   * @throws KeeperException
   * @throws IOException
   */
  public static void createNodeSplitting(final ZooKeeperWatcher zkw, final HRegionInfo region,
      final ServerName serverName, final HRegionInfo a,
      final HRegionInfo b) throws KeeperException, IOException {
    LOG.debug(zkw.prefix("Creating ephemeral node for " +
      region.getEncodedName() + " in PENDING_SPLIT state"));
    byte [] payload = HRegionInfo.toDelimitedByteArray(a, b);
    RegionTransition rt = RegionTransition.createRegionTransition(
      RS_ZK_REQUEST_REGION_SPLIT, region.getRegionName(), serverName, payload);
    String node = ZKAssign.getNodeName(zkw, region.getEncodedName());
    if (!ZKUtil.createEphemeralNodeAndWatch(zkw, node, rt.toByteArray())) {
      throw new IOException("Failed create of ephemeral " + node);
    }
  }

  /**
   * Transitions an existing ephemeral node for the specified region which is
   * currently in the begin state to be in the end state. Master cleans up the
   * final SPLIT znode when it reads it (or if we crash, zk will clean it up).
   *
   * <p>Does not transition nodes from other states. If for some reason the
   * node could not be transitioned, the method returns -1. If the transition
   * is successful, the version of the node after transition is returned.
   *
   * <p>This method can fail and return false for three different reasons:
   * <ul><li>Node for this region does not exist</li>
   * <li>Node for this region is not in the begin state</li>
   * <li>After verifying the begin state, update fails because of wrong version
   * (this should never actually happen since an RS only does this transition
   * following a transition to the begin state. If two RS are conflicting, one would
   * fail the original transition to the begin state and not this transition)</li>
   * </ul>
   *
   * <p>Does not set any watches.
   *
   * <p>This method should only be used by a RegionServer when splitting a region.
   *
   * @param zkw zk reference
   * @param parent region to be transitioned to opened
   * @param a Daughter a of split
   * @param b Daughter b of split
   * @param serverName server event originates from
   * @param znodeVersion expected version of data before modification
   * @param beginState the expected current state the znode should be
   * @param endState the state to be transition to
   * @return version of node after transition, -1 if unsuccessful transition
   * @throws KeeperException if unexpected zookeeper exception
   * @throws IOException
   */
  public static int transitionSplittingNode(ZooKeeperWatcher zkw,
      HRegionInfo parent, HRegionInfo a, HRegionInfo b, ServerName serverName,
      final int znodeVersion, final EventType beginState,
      final EventType endState) throws KeeperException, IOException {
    byte [] payload = HRegionInfo.toDelimitedByteArray(a, b);
    return ZKAssign.transitionNode(zkw, parent, serverName,
      beginState, endState, znodeVersion, payload);
  }

  List<JournalEntry> getJournal() {
    return journal;
  }
}
