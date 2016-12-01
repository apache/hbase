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

import java.io.IOException;
import java.io.InterruptedIOException;
import java.security.PrivilegedExceptionAction;
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
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HasThread;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.apache.zookeeper.KeeperException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

@InterfaceAudience.Private
public class SplitTransactionImpl implements SplitTransaction {
  private static final Log LOG = LogFactory.getLog(SplitTransactionImpl.class);

  /*
   * Region to split
   */
  private final HRegion parent;
  private HRegionInfo hri_a;
  private HRegionInfo hri_b;
  private long fileSplitTimeout = 30000;

  /*
   * Row to split around
   */
  private final byte [] splitrow;

  /*
   * Transaction state for listener, only valid during execute and
   * rollback
   */
  private SplitTransactionPhase currentPhase = SplitTransactionPhase.STARTED;
  private Server server;
  private RegionServerServices rsServices;

  public static class JournalEntryImpl implements JournalEntry {
    private SplitTransactionPhase type;
    private long timestamp;

    public JournalEntryImpl(SplitTransactionPhase type) {
      this(type, EnvironmentEdgeManager.currentTime());
    }

    public JournalEntryImpl(SplitTransactionPhase type, long timestamp) {
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
    public SplitTransactionPhase getPhase() {
      return type;
    }

    @Override
    public long getTimeStamp() {
      return timestamp;
    }
  }

  /*
   * Journal of how far the split transaction has progressed.
   */
  private final ArrayList<JournalEntry> journal = new ArrayList<JournalEntry>();

  /**
   * Listeners
   */
  private final ArrayList<TransactionListener> listeners = new ArrayList<TransactionListener>();

  /**
   * Constructor
   * @param r Region to split
   * @param splitrow Row to split around
   */
  public SplitTransactionImpl(final Region r, final byte [] splitrow) {
    this.parent = (HRegion)r;
    this.splitrow = splitrow;
    this.journal.add(new JournalEntryImpl(SplitTransactionPhase.STARTED));
  }

  private void transition(SplitTransactionPhase nextPhase) throws IOException {
    transition(nextPhase, false);
  }

  private void transition(SplitTransactionPhase nextPhase, boolean isRollback)
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

  @Override
  public boolean prepare() throws IOException {
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

    transition(SplitTransactionPhase.PREPARED);

    return true;
  }

  /**
   * Calculate daughter regionid to use.
   * @param hri Parent {@link HRegionInfo}
   * @return Daughter region id (timestamp) to use.
   */
  private static long getDaughterRegionIdTimestamp(final HRegionInfo hri) {
    long rid = EnvironmentEdgeManager.currentTime();
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
   * @param user
   * @throws IOException If thrown, transaction failed.
   *    Call {@link #rollback(Server, RegionServerServices)}
   * @return Regions created
   */
  @VisibleForTesting
  PairOfSameType<Region> createDaughters(final Server server,
      final RegionServerServices services, User user) throws IOException {
    LOG.info("Starting split of region " + this.parent);
    if ((server != null && server.isStopped()) ||
        (services != null && services.isStopping())) {
      throw new IOException("Server is stopped or stopping");
    }
    assert !this.parent.lock.writeLock().isHeldByCurrentThread():
      "Unsafe to hold write lock while performing RPCs";

    transition(SplitTransactionPhase.BEFORE_PRE_SPLIT_HOOK);

    // Coprocessor callback
    if (this.parent.getCoprocessorHost() != null) {
      // TODO: Remove one of these
      parent.getCoprocessorHost().preSplit(user);
      parent.getCoprocessorHost().preSplit(splitrow, user);
    }

    transition(SplitTransactionPhase.AFTER_PRE_SPLIT_HOOK);

    // If true, no cluster to write meta edits to or to update znodes in.
    boolean testing = server == null? true:
        server.getConfiguration().getBoolean("hbase.testing.nocluster", false);
    this.fileSplitTimeout = testing ? this.fileSplitTimeout :
        server.getConfiguration().getLong("hbase.regionserver.fileSplitTimeout",
          this.fileSplitTimeout);

    PairOfSameType<Region> daughterRegions = stepsBeforePONR(server, services, testing);

    final List<Mutation> metaEntries = new ArrayList<Mutation>();
    boolean ret = false;
    if (this.parent.getCoprocessorHost() != null) {
      ret = parent.getCoprocessorHost().preSplitBeforePONR(splitrow, metaEntries, user);
      if (ret) {
          throw new IOException("Coprocessor bypassing region "
            + parent.getRegionInfo().getRegionNameAsString() + " split.");
      }
      try {
        for (Mutation p : metaEntries) {
          HRegionInfo.parseRegionName(p.getRow());
        }
      } catch (IOException e) {
        LOG.error("Row key of mutation from coprocessor is not parsable as region name."
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

    transition(SplitTransactionPhase.PONR);

    // Edit parent in meta.  Offlines parent region and adds splita and splitb
    // as an atomic update. See HBASE-7721. This update to META makes the region
    // will determine whether the region is split or not in case of failures.
    // If it is successful, master will roll-forward, if not, master will rollback
    // and assign the parent region.
    if (services != null && !services.reportRegionStateTransition(TransitionCode.SPLIT_PONR,
        parent.getRegionInfo(), hri_a, hri_b)) {
      // Passed PONR, let SSH clean it up
      throw new IOException("Failed to notify master that split passed PONR: "
        + parent.getRegionInfo().getRegionNameAsString());
    }
    return daughterRegions;
  }

  @VisibleForTesting
  Put addLocation(final Put p, final ServerName sn, long openSeqNum) {
    p.addColumn(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER, Bytes
            .toBytes(sn.getHostAndPort()));
    p.addColumn(HConstants.CATALOG_FAMILY, HConstants.STARTCODE_QUALIFIER, Bytes.toBytes(sn
            .getStartcode()));
    p.addColumn(HConstants.CATALOG_FAMILY, HConstants.SEQNUM_QUALIFIER, Bytes.toBytes(openSeqNum));
    return p;
  }

  @VisibleForTesting
  public PairOfSameType<Region> stepsBeforePONR(final Server server,
      final RegionServerServices services, boolean testing) throws IOException {
    if (services != null && !services.reportRegionStateTransition(TransitionCode.READY_TO_SPLIT,
        parent.getRegionInfo(), hri_a, hri_b)) {
      throw new IOException("Failed to get ok from master to split "
        + parent.getRegionInfo().getRegionNameAsString());
    }

    transition(SplitTransactionPhase.SET_SPLITTING);

    this.parent.getRegionFileSystem().createSplitsDir();

    transition(SplitTransactionPhase.CREATE_SPLIT_DIR);

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
      transition(SplitTransactionPhase.CLOSED_PARENT_REGION);
    }
    if (exceptionToThrow != null) {
      if (exceptionToThrow instanceof IOException) throw (IOException)exceptionToThrow;
      throw new IOException(exceptionToThrow);
    }
    if (!testing) {
      services.removeFromOnlineRegions(this.parent, null);
    }

    transition(SplitTransactionPhase.OFFLINED_PARENT);

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

    transition(SplitTransactionPhase.STARTED_REGION_A_CREATION);

    assertReferenceFileCount(expectedReferences.getFirst(),
        this.parent.getRegionFileSystem().getSplitsDir(this.hri_a));
    HRegion a = this.parent.createDaughterRegionFromSplits(this.hri_a);
    assertReferenceFileCount(expectedReferences.getFirst(),
        new Path(this.parent.getRegionFileSystem().getTableDir(), this.hri_a.getEncodedName()));

    // Ditto

    transition(SplitTransactionPhase.STARTED_REGION_B_CREATION);

    assertReferenceFileCount(expectedReferences.getSecond(),
        this.parent.getRegionFileSystem().getSplitsDir(this.hri_b));
    HRegion b = this.parent.createDaughterRegionFromSplits(this.hri_b);
    assertReferenceFileCount(expectedReferences.getSecond(),
        new Path(this.parent.getRegionFileSystem().getTableDir(), this.hri_b.getEncodedName()));

    return new PairOfSameType<Region>(a, b);
  }

  @VisibleForTesting
  void assertReferenceFileCount(int expectedReferenceFileCount, Path dir)
      throws IOException {
    if (expectedReferenceFileCount != 0 &&
        expectedReferenceFileCount != FSUtils.getRegionReferenceFileCount(parent.getFilesystem(),
          dir)) {
      throw new IOException("Failing split. Expected reference file count isn't equal.");
    }
  }

  /**
   * Perform time consuming opening of the daughter regions.
   * @param server Hosting server instance.  Can be null when testing
   * @param services Used to online/offline regions.
   * @param a first daughter region
   * @param a second daughter region
   * @throws IOException If thrown, transaction failed.
   *          Call {@link #rollback(Server, RegionServerServices)}
   */
  @VisibleForTesting
  void openDaughters(final Server server, final RegionServerServices services, Region a,
      Region b) throws IOException {
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
          transition(SplitTransactionPhase.OPENED_REGION_A);
        }
        bOpener.join();
        if (bOpener.getException() == null) {
          transition(SplitTransactionPhase.OPENED_REGION_B);
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
        if (!services.reportRegionStateTransition(TransitionCode.SPLIT,
            parent.getRegionInfo(), hri_a, hri_b)) {
          throw new IOException("Failed to report split region to master: "
            + parent.getRegionInfo().getShortNameToLog());
        }
        // Should add it to OnlineRegions
        services.addToOnlineRegions(b);
        services.addToOnlineRegions(a);
      }
    }
  }

  @Override
  public PairOfSameType<Region> execute(final Server server,
    final RegionServerServices services)
        throws IOException {
    if (User.isHBaseSecurityEnabled(parent.getBaseConf())) {
      LOG.warn("Should use execute(Server, RegionServerServices, User)");
    }
    return execute(server, services, null);
  }

  @Override
  public PairOfSameType<Region> execute(final Server server, final RegionServerServices services,
    User user) throws IOException {
    this.server = server;
    this.rsServices = services;
    PairOfSameType<Region> regions = createDaughters(server, services, user);
    stepsAfterPONR(server, services, regions, user);
    transition(SplitTransactionPhase.COMPLETED);
    return regions;
  }

  @VisibleForTesting
  void stepsAfterPONR(final Server server,
      final RegionServerServices services, final PairOfSameType<Region> regions, User user)
      throws IOException {
    if (this.parent.getCoprocessorHost() != null) {
      parent.getCoprocessorHost().preSplitAfterPONR(user);
    }

    openDaughters(server, services, regions.getFirst(), regions.getSecond());

    transition(SplitTransactionPhase.BEFORE_POST_SPLIT_HOOK);

    // Coprocessor callback
    if (parent.getCoprocessorHost() != null) {
      this.parent.getCoprocessorHost().postSplit(regions.getFirst(), regions.getSecond(), user);
    }

    transition(SplitTransactionPhase.AFTER_POST_SPLIT_HOOK);
  }

  /*
   * Open daughter region in its own thread.
   * If we fail, abort this hosting server.
   */
  private class DaughterOpener extends HasThread {
    private final Server server;
    private final Region r;
    private Throwable t = null;

    DaughterOpener(final Server s, final Region r) {
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
  @VisibleForTesting
  void openDaughterRegion(final Server server, final Region daughter)
      throws IOException, KeeperException {
    HRegionInfo hri = daughter.getRegionInfo();
    LoggingProgressable reporter = server == null ? null
        : new LoggingProgressable(hri, server.getConfiguration().getLong(
            "hbase.regionserver.split.daughter.open.log.interval", 10000));
    ((HRegion)daughter).openHRegion(reporter);
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
      long now = EnvironmentEdgeManager.currentTime();
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
    int nbFiles = 0;
    for (Map.Entry<byte[], List<StoreFile>> entry: hstoreFilesToSplit.entrySet()) {
        nbFiles += entry.getValue().size();
    }
    if (nbFiles == 0) {
      // no file needs to be splitted.
      return new Pair<Integer, Integer>(0,0);
    }
    // Default max #threads to use is the smaller of table's configured number of blocking store
    // files or the available number of logical cores.
    int defMaxThreads = Math.min(parent.conf.getInt(HStore.BLOCKING_STOREFILES_KEY,
                HStore.DEFAULT_BLOCKING_STOREFILE_COUNT),
            Runtime.getRuntime().availableProcessors());
    // Max #threads is the smaller of the number of storefiles or the default max determined above.
    int maxThreads = Math.min(parent.conf.getInt(HConstants.REGION_SPLIT_THREADS_MAX,
                defMaxThreads), nbFiles);
    LOG.info("Preparing to split " + nbFiles + " storefiles for region " + this.parent +
            " using " + maxThreads + " threads");
    ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
    builder.setNameFormat("StoreFileSplitter-%1$d");
    ThreadFactory factory = builder.build();
    ThreadPoolExecutor threadPool =
      (ThreadPoolExecutor) Executors.newFixedThreadPool(maxThreads, factory);
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
      LOG.debug("Split storefiles for region " + this.parent + " Daughter A: " + created_a
          + " storefiles, Daughter B: " + created_b + " storefiles.");
    }
    return new Pair<Integer, Integer>(created_a, created_b);
  }

  private Pair<Path, Path> splitStoreFile(final byte[] family, final StoreFile sf)
      throws IOException {
    if (LOG.isDebugEnabled()) {
        LOG.debug("Splitting started for store file: " + sf.getPath() + " for region: " +
                  this.parent);
    }
    HRegionFileSystem fs = this.parent.getRegionFileSystem();
    String familyName = Bytes.toString(family);
    Path path_a =
        fs.splitStoreFile(this.hri_a, familyName, sf, this.splitrow, false,
          this.parent.getSplitPolicy());
    Path path_b =
        fs.splitStoreFile(this.hri_b, familyName, sf, this.splitrow, true,
          this.parent.getSplitPolicy());
    if (LOG.isDebugEnabled()) {
        LOG.debug("Splitting complete for store file: " + sf.getPath() + " for region: " +
                  this.parent);
    }
    return new Pair<Path,Path>(path_a, path_b);
  }

  /**
   * Utility class used to do the file splitting / reference writing
   * in parallel instead of sequentially.
   */
  private class StoreFileSplitter implements Callable<Pair<Path,Path>> {
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

  @Override
  public boolean rollback(final Server server, final RegionServerServices services)
      throws IOException {
    if (User.isHBaseSecurityEnabled(parent.getBaseConf())) {
      LOG.warn("Should use rollback(Server, RegionServerServices, User)");
    }
    return rollback(server, services, null);
  }

  @Override
  public boolean rollback(final Server server, final RegionServerServices services, User user)
      throws IOException {
    this.server = server;
    this.rsServices = services;
    // Coprocessor callback
    if (this.parent.getCoprocessorHost() != null) {
      this.parent.getCoprocessorHost().preRollBackSplit(user);
    }

    boolean result = true;
    ListIterator<JournalEntry> iterator =
      this.journal.listIterator(this.journal.size());
    // Iterate in reverse.
    while (iterator.hasPrevious()) {
      JournalEntry je = iterator.previous();

      transition(je.getPhase(), true);

      switch (je.getPhase()) {

      case SET_SPLITTING:
        if (services != null
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
            parent.getRegionInfo().getRegionNameAsString(), e);
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
      case COMPLETED:
        break;

      default:
        throw new RuntimeException("Unhandled journal entry: " + je);
      }
    }
    // Coprocessor callback
    if (this.parent.getCoprocessorHost() != null) {
      this.parent.getCoprocessorHost().postRollBackSplit(user);
    }
    return result;
  }

  /* package */ HRegionInfo getFirstDaughter() {
    return hri_a;
  }

  /* package */ HRegionInfo getSecondDaughter() {
    return hri_b;
  }

  @Override
  public List<JournalEntry> getJournal() {
    return journal;
  }

  @Override
  public SplitTransaction registerTransactionListener(TransactionListener listener) {
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
