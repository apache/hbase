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
package org.apache.hadoop.hbase.regionserver.wal;

import static org.apache.hadoop.hbase.wal.DefaultWALProvider.WAL_FILE_NAME_DELIMITER;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.DrainBarrier;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HasThread;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.DefaultWALProvider;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALPrettyPrinter;
import org.apache.hadoop.hbase.wal.WALProvider.Writer;
import org.apache.hadoop.hbase.wal.WALSplitter;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.util.StringUtils;
import org.apache.htrace.NullScope;
import org.apache.htrace.Span;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * Implementation of {@link WAL} to go against {@link FileSystem}; i.e. keep WALs in HDFS.
 * Only one WAL is ever being written at a time.  When a WAL hits a configured maximum size,
 * it is rolled.  This is done internal to the implementation.
 *
 * <p>As data is flushed from the MemStore to other on-disk structures (files sorted by
 * key, hfiles), a WAL becomes obsolete. We can let go of all the log edits/entries for a given
 * HRegion-sequence id.  A bunch of work in the below is done keeping account of these region
 * sequence ids -- what is flushed out to hfiles, and what is yet in WAL and in memory only.
 *
 * <p>It is only practical to delete entire files. Thus, we delete an entire on-disk file
 * <code>F</code> when all of the edits in <code>F</code> have a log-sequence-id that's older
 * (smaller) than the most-recent flush.
 *
 * <p>To read an WAL, call {@link WALFactory#createReader(org.apache.hadoop.fs.FileSystem,
 * org.apache.hadoop.fs.Path)}.
 */
@InterfaceAudience.Private
public class FSHLog implements WAL {
  // IMPLEMENTATION NOTES:
  //
  // At the core is a ring buffer.  Our ring buffer is the LMAX Disruptor.  It tries to
  // minimize synchronizations and volatile writes when multiple contending threads as is the case
  // here appending and syncing on a single WAL.  The Disruptor is configured to handle multiple
  // producers but it has one consumer only (the producers in HBase are IPC Handlers calling append
  // and then sync).  The single consumer/writer pulls the appends and syncs off the ring buffer.
  // When a handler calls sync, it is given back a future. The producer 'blocks' on the future so
  // it does not return until the sync completes.  The future is passed over the ring buffer from
  // the producer/handler to the consumer thread where it does its best to batch up the producer
  // syncs so one WAL sync actually spans multiple producer sync invocations.  How well the
  // batching works depends on the write rate; i.e. we tend to batch more in times of
  // high writes/syncs.
  //
  // Calls to append now also wait until the append has been done on the consumer side of the
  // disruptor.  We used to not wait but it makes the implemenation easier to grok if we have
  // the region edit/sequence id after the append returns.
  // 
  // TODO: Handlers need to coordinate appending AND syncing.  Can we have the threads contend
  // once only?  Probably hard given syncs take way longer than an append.
  //
  // The consumer threads pass the syncs off to multiple syncing threads in a round robin fashion
  // to ensure we keep up back-to-back FS sync calls (FS sync calls are the long poll writing the
  // WAL).  The consumer thread passes the futures to the sync threads for it to complete
  // the futures when done.
  //
  // The 'sequence' in the below is the sequence of the append/sync on the ringbuffer.  It
  // acts as a sort-of transaction id.  It is always incrementing.
  //
  // The RingBufferEventHandler class hosts the ring buffer consuming code.  The threads that
  // do the actual FS sync are implementations of SyncRunner.  SafePointZigZagLatch is a
  // synchronization class used to halt the consumer at a safe point --  just after all outstanding
  // syncs and appends have completed -- so the log roller can swap the WAL out under it.

  static final Log LOG = LogFactory.getLog(FSHLog.class);

  private static final int DEFAULT_SLOW_SYNC_TIME_MS = 100; // in ms
  
  /**
   * The nexus at which all incoming handlers meet.  Does appends and sync with an ordering.
   * Appends and syncs are each put on the ring which means handlers need to
   * smash up against the ring twice (can we make it once only? ... maybe not since time to append
   * is so different from time to sync and sometimes we don't want to sync or we want to async
   * the sync).  The ring is where we make sure of our ordering and it is also where we do
   * batching up of handler sync calls.
   */
  private final Disruptor<RingBufferTruck> disruptor;

  /**
   * An executorservice that runs the disrutpor AppendEventHandler append executor.
   */
  private final ExecutorService appendExecutor;

  /**
   * This fellow is run by the above appendExecutor service but it is all about batching up appends
   * and syncs; it may shutdown without cleaning out the last few appends or syncs.  To guard
   * against this, keep a reference to this handler and do explicit close on way out to make sure
   * all flushed out before we exit.
   */
  private final RingBufferEventHandler ringBufferEventHandler;

  /**
   * Map of {@link SyncFuture}s keyed by Handler objects.  Used so we reuse SyncFutures.
   * TODO: Reus FSWALEntry's rather than create them anew each time as we do SyncFutures here.
   * TODO: Add a FSWalEntry and SyncFuture as thread locals on handlers rather than have them
   * get them from this Map?
   */
  private final Map<Thread, SyncFuture> syncFuturesByHandler;

  /**
   * The highest known outstanding unsync'd WALEdit sequence number where sequence number is the
   * ring buffer sequence.  Maintained by the ring buffer consumer.
   */
  private volatile long highestUnsyncedSequence = -1;

  /**
   * Updated to the ring buffer sequence of the last successful sync call.  This can be less than
   * {@link #highestUnsyncedSequence} for case where we have an append where a sync has not yet
   * come in for it.  Maintained by the syncing threads.
   */
  private final AtomicLong highestSyncedSequence = new AtomicLong(0);

  /**
   * file system instance
   */
  protected final FileSystem fs;

  /**
   * WAL directory, where all WAL files would be placed.
   */
  private final Path fullPathLogDir;
  /**
   * dir path where old logs are kept.
   */
  private final Path fullPathArchiveDir;

  /**
   * Matches just those wal files that belong to this wal instance.
   */
  private final PathFilter ourFiles;

  /**
   * Prefix of a WAL file, usually the region server name it is hosted on.
   */
  private final String logFilePrefix;

  /**
   * Suffix included on generated wal file names 
   */
  private final String logFileSuffix;

  /**
   * Prefix used when checking for wal membership.
   */
  private final String prefixPathStr;

  private final WALCoprocessorHost coprocessorHost;

  /**
   * conf object
   */
  protected final Configuration conf;
  /** Listeners that are called on WAL events. */
  private final List<WALActionsListener> listeners = new CopyOnWriteArrayList<WALActionsListener>();

  @Override
  public void registerWALActionsListener(final WALActionsListener listener) {
    this.listeners.add(listener);
  }
  
  @Override
  public boolean unregisterWALActionsListener(final WALActionsListener listener) {
    return this.listeners.remove(listener);
  }

  @Override
  public WALCoprocessorHost getCoprocessorHost() {
    return coprocessorHost;
  }
  /**
   * FSDataOutputStream associated with the current SequenceFile.writer
   */
  private FSDataOutputStream hdfs_out;

  // All about log rolling if not enough replicas outstanding.

  // Minimum tolerable replicas, if the actual value is lower than it, rollWriter will be triggered
  private final int minTolerableReplication;

  // DFSOutputStream.getNumCurrentReplicas method instance gotten via reflection.
  private final Method getNumCurrentReplicas;
  private final Method getPipeLine; // refers to DFSOutputStream.getPipeLine
  private final int slowSyncNs;

  private final static Object [] NO_ARGS = new Object []{};

  // If live datanode count is lower than the default replicas value,
  // RollWriter will be triggered in each sync(So the RollWriter will be
  // triggered one by one in a short time). Using it as a workaround to slow
  // down the roll frequency triggered by checkLowReplication().
  private final AtomicInteger consecutiveLogRolls = new AtomicInteger(0);

  private final int lowReplicationRollLimit;

  // If consecutiveLogRolls is larger than lowReplicationRollLimit,
  // then disable the rolling in checkLowReplication().
  // Enable it if the replications recover.
  private volatile boolean lowReplicationRollEnabled = true;

  /**
   * Current log file.
   */
  volatile Writer writer;

  /** The barrier used to ensure that close() waits for all log rolls and flushes to finish. */
  private final DrainBarrier closeBarrier = new DrainBarrier();

  /**
   * This lock makes sure only one log roll runs at a time. Should not be taken while any other
   * lock is held. We don't just use synchronized because that results in bogus and tedious
   * findbugs warning when it thinks synchronized controls writer thread safety.  It is held when
   * we are actually rolling the log.  It is checked when we are looking to see if we should roll
   * the log or not.
   */
  private final ReentrantLock rollWriterLock = new ReentrantLock(true);

  private volatile boolean closed = false;
  private final AtomicBoolean shutdown = new AtomicBoolean(false);

  // The timestamp (in ms) when the log file was created.
  private final AtomicLong filenum = new AtomicLong(-1);

  // Number of transactions in the current Wal.
  private final AtomicInteger numEntries = new AtomicInteger(0);

  // If > than this size, roll the log.
  private final long logrollsize;

  /**
   * The total size of wal
   */
  private AtomicLong totalLogSize = new AtomicLong(0);

  /*
   * If more than this many logs, force flush of oldest region to oldest edit
   * goes to disk.  If too many and we crash, then will take forever replaying.
   * Keep the number of logs tidy.
   */
  private final int maxLogs;

  /** Number of log close errors tolerated before we abort */
  private final int closeErrorsTolerated;

  private final AtomicInteger closeErrorCount = new AtomicInteger();

  // Region sequence id accounting across flushes and for knowing when we can GC a WAL.  These
  // sequence id numbers are by region and unrelated to the ring buffer sequence number accounting
  // done above in failedSequence, highest sequence, etc.
  /**
   * This lock ties all operations on lowestFlushingStoreSequenceIds and
   * oldestUnflushedStoreSequenceIds Maps with the exception of append's putIfAbsent call into
   * oldestUnflushedStoreSequenceIds. We use these Maps to find out the low bound regions
   * sequence id, or to find regions with old sequence ids to force flush; we are interested in
   * old stuff not the new additions (TODO: IS THIS SAFE?  CHECK!).
   */
  private final Object regionSequenceIdLock = new Object();

  /**
   * Map of encoded region names and family names to their OLDEST -- i.e. their first,
   * the longest-lived -- sequence id in memstore. Note that this sequence id is the region
   * sequence id.  This is not related to the id we use above for {@link #highestSyncedSequence}
   * and {@link #highestUnsyncedSequence} which is the sequence from the disruptor
   * ring buffer.
   */
  private final ConcurrentMap<byte[], ConcurrentMap<byte[], Long>> oldestUnflushedStoreSequenceIds
    = new ConcurrentSkipListMap<byte[], ConcurrentMap<byte[], Long>>(
      Bytes.BYTES_COMPARATOR);

  /**
   * Map of encoded region names and family names to their lowest or OLDEST sequence/edit id in
   * memstore currently being flushed out to hfiles. Entries are moved here from
   * {@link #oldestUnflushedStoreSequenceIds} while the lock {@link #regionSequenceIdLock} is held
   * (so movement between the Maps is atomic). This is not related to the id we use above for
   * {@link #highestSyncedSequence} and {@link #highestUnsyncedSequence} which is the sequence from
   * the disruptor ring buffer, an internal detail.
   */
  private final Map<byte[], Map<byte[], Long>> lowestFlushingStoreSequenceIds =
    new TreeMap<byte[], Map<byte[], Long>>(Bytes.BYTES_COMPARATOR);

 /**
  * Map of region encoded names to the latest region sequence id.  Updated on each append of
  * WALEdits to the WAL. We create one map for each WAL file at the time it is rolled.
  * <p>When deciding whether to archive a WAL file, we compare the sequence IDs in this map to
  * {@link #lowestFlushingRegionSequenceIds} and {@link #oldestUnflushedRegionSequenceIds}.
  * See {@link FSHLog#areAllRegionsFlushed(Map, Map, Map)} for more info.
  * <p>
  * This map uses byte[] as the key, and uses reference equality. It works in our use case as we
  * use {@link HRegionInfo#getEncodedNameAsBytes()} as keys. For a given region, it always returns
  * the same array.
  */
  private Map<byte[], Long> highestRegionSequenceIds = new HashMap<byte[], Long>();

  /**
   * WAL Comparator; it compares the timestamp (log filenum), present in the log file name.
   * Throws an IllegalArgumentException if used to compare paths from different wals.
   */
  public final Comparator<Path> LOG_NAME_COMPARATOR = new Comparator<Path>() {
    @Override
    public int compare(Path o1, Path o2) {
      long t1 = getFileNumFromFileName(o1);
      long t2 = getFileNumFromFileName(o2);
      if (t1 == t2) return 0;
      return (t1 > t2) ? 1 : -1;
    }
  };

  /**
   * Map of wal log file to the latest sequence ids of all regions it has entries of.
   * The map is sorted by the log file creation timestamp (contained in the log file name).
   */
  private NavigableMap<Path, Map<byte[], Long>> byWalRegionSequenceIds =
    new ConcurrentSkipListMap<Path, Map<byte[], Long>>(LOG_NAME_COMPARATOR);

  /**
   * Exception handler to pass the disruptor ringbuffer.  Same as native implementation only it
   * logs using our logger instead of java native logger.
   */
  static class RingBufferExceptionHandler implements ExceptionHandler {
    @Override
    public void handleEventException(Throwable ex, long sequence, Object event) {
      LOG.error("Sequence=" + sequence + ", event=" + event, ex);
      throw new RuntimeException(ex);
    }

    @Override
    public void handleOnStartException(Throwable ex) {
      LOG.error(ex);
      throw new RuntimeException(ex);
    }

    @Override
    public void handleOnShutdownException(Throwable ex) {
      LOG.error(ex);
      throw new RuntimeException(ex);
    }
  }

  /**
   * Constructor.
   *
   * @param fs filesystem handle
   * @param root path for stored and archived wals
   * @param logDir dir where wals are stored
   * @param conf configuration to use
   * @throws IOException
   */
  public FSHLog(final FileSystem fs, final Path root, final String logDir, final Configuration conf)
      throws IOException {
    this(fs, root, logDir, HConstants.HREGION_OLDLOGDIR_NAME, conf, null, true, null, null);
  }

  /**
   * Create an edit log at the given <code>dir</code> location.
   *
   * You should never have to load an existing log. If there is a log at
   * startup, it should have already been processed and deleted by the time the
   * WAL object is started up.
   *
   * @param fs filesystem handle
   * @param rootDir path to where logs and oldlogs
   * @param logDir dir where wals are stored
   * @param archiveDir dir where wals are archived
   * @param conf configuration to use
   * @param listeners Listeners on WAL events. Listeners passed here will
   * be registered before we do anything else; e.g. the
   * Constructor {@link #rollWriter()}.
   * @param failIfWALExists If true IOException will be thrown if files related to this wal
   *        already exist.
   * @param prefix should always be hostname and port in distributed env and
   *        it will be URL encoded before being used.
   *        If prefix is null, "wal" will be used
   * @param suffix will be url encoded. null is treated as empty. non-empty must start with
   *        {@link DefaultWALProvider#WAL_FILE_NAME_DELIMITER}
   * @throws IOException
   */
  public FSHLog(final FileSystem fs, final Path rootDir, final String logDir,
      final String archiveDir, final Configuration conf,
      final List<WALActionsListener> listeners,
      final boolean failIfWALExists, final String prefix, final String suffix)
      throws IOException {
    this.fs = fs;
    this.fullPathLogDir = new Path(rootDir, logDir);
    this.fullPathArchiveDir = new Path(rootDir, archiveDir);
    this.conf = conf;

    if (!fs.exists(fullPathLogDir) && !fs.mkdirs(fullPathLogDir)) {
      throw new IOException("Unable to mkdir " + fullPathLogDir);
    }

    if (!fs.exists(this.fullPathArchiveDir)) {
      if (!fs.mkdirs(this.fullPathArchiveDir)) {
        throw new IOException("Unable to mkdir " + this.fullPathArchiveDir);
      }
    }

    // If prefix is null||empty then just name it wal
    this.logFilePrefix =
      prefix == null || prefix.isEmpty() ? "wal" : URLEncoder.encode(prefix, "UTF8");
    // we only correctly differentiate suffices when numeric ones start with '.'
    if (suffix != null && !(suffix.isEmpty()) && !(suffix.startsWith(WAL_FILE_NAME_DELIMITER))) {
      throw new IllegalArgumentException("wal suffix must start with '" + WAL_FILE_NAME_DELIMITER +
          "' but instead was '" + suffix + "'");
    }
    // Now that it exists, set the storage policy for the entire directory of wal files related to
    // this FSHLog instance
    FSUtils.setStoragePolicy(fs, conf, this.fullPathLogDir, HConstants.WAL_STORAGE_POLICY,
      HConstants.DEFAULT_WAL_STORAGE_POLICY);
    this.logFileSuffix = (suffix == null) ? "" : URLEncoder.encode(suffix, "UTF8");
    this.prefixPathStr = new Path(fullPathLogDir,
        logFilePrefix + WAL_FILE_NAME_DELIMITER).toString();

    this.ourFiles = new PathFilter() {
      @Override
      public boolean accept(final Path fileName) {
        // The path should start with dir/<prefix> and end with our suffix
        final String fileNameString = fileName.toString();
        if (!fileNameString.startsWith(prefixPathStr)) {
          return false;
        }
        if (logFileSuffix.isEmpty()) {
          // in the case of the null suffix, we need to ensure the filename ends with a timestamp.
          return org.apache.commons.lang.StringUtils.isNumeric(
              fileNameString.substring(prefixPathStr.length()));
        } else if (!fileNameString.endsWith(logFileSuffix)) {
          return false;
        }
        return true;
      }
    };

    if (failIfWALExists) {
      final FileStatus[] walFiles = FSUtils.listStatus(fs, fullPathLogDir, ourFiles);
      if (null != walFiles && 0 != walFiles.length) {
        throw new IOException("Target WAL already exists within directory " + fullPathLogDir);
      }
    }

    // Register listeners.  TODO: Should this exist anymore?  We have CPs?
    if (listeners != null) {
      for (WALActionsListener i: listeners) {
        registerWALActionsListener(i);
      }
    }
    this.coprocessorHost = new WALCoprocessorHost(this, conf);

    // Get size to roll log at. Roll at 95% of HDFS block size so we avoid crossing HDFS blocks
    // (it costs a little x'ing bocks)
    final long blocksize = this.conf.getLong("hbase.regionserver.hlog.blocksize",
        FSUtils.getDefaultBlockSize(this.fs, this.fullPathLogDir));
    this.logrollsize =
      (long)(blocksize * conf.getFloat("hbase.regionserver.logroll.multiplier", 0.95f));

    this.maxLogs = conf.getInt("hbase.regionserver.maxlogs", 32);
    this.minTolerableReplication = conf.getInt( "hbase.regionserver.hlog.tolerable.lowreplication",
        FSUtils.getDefaultReplication(fs, this.fullPathLogDir));
    this.lowReplicationRollLimit =
      conf.getInt("hbase.regionserver.hlog.lowreplication.rolllimit", 5);
    this.closeErrorsTolerated = conf.getInt("hbase.regionserver.logroll.errors.tolerated", 0);
    int maxHandlersCount = conf.getInt(HConstants.REGION_SERVER_HANDLER_COUNT, 200);

    LOG.info("WAL configuration: blocksize=" + StringUtils.byteDesc(blocksize) +
      ", rollsize=" + StringUtils.byteDesc(this.logrollsize) +
      ", prefix=" + this.logFilePrefix + ", suffix=" + logFileSuffix + ", logDir=" +
      this.fullPathLogDir + ", archiveDir=" + this.fullPathArchiveDir);

    // rollWriter sets this.hdfs_out if it can.
    rollWriter();

    this.slowSyncNs =
        1000000 * conf.getInt("hbase.regionserver.hlog.slowsync.ms",
          DEFAULT_SLOW_SYNC_TIME_MS);
    // handle the reflection necessary to call getNumCurrentReplicas(). TODO: Replace with
    // HdfsDataOutputStream#getCurrentBlockReplication() and go without reflection.
    this.getNumCurrentReplicas = getGetNumCurrentReplicas(this.hdfs_out);
    this.getPipeLine = getGetPipeline(this.hdfs_out);

    // This is the 'writer' -- a single threaded executor.  This single thread 'consumes' what is
    // put on the ring buffer.
    String hostingThreadName = Thread.currentThread().getName();
    this.appendExecutor = Executors.
      newSingleThreadExecutor(Threads.getNamedThreadFactory(hostingThreadName + ".append"));
    // Preallocate objects to use on the ring buffer.  The way that appends and syncs work, we will
    // be stuck and make no progress if the buffer is filled with appends only and there is no
    // sync. If no sync, then the handlers will be outstanding just waiting on sync completion
    // before they return.
    final int preallocatedEventCount =
      this.conf.getInt("hbase.regionserver.wal.disruptor.event.count", 1024 * 16);
    // Using BlockingWaitStrategy.  Stuff that is going on here takes so long it makes no sense
    // spinning as other strategies do.
    this.disruptor =
      new Disruptor<RingBufferTruck>(RingBufferTruck.EVENT_FACTORY, preallocatedEventCount,
        this.appendExecutor, ProducerType.MULTI, new BlockingWaitStrategy());
    // Advance the ring buffer sequence so that it starts from 1 instead of 0,
    // because SyncFuture.NOT_DONE = 0.
    this.disruptor.getRingBuffer().next();
    this.ringBufferEventHandler =
      new RingBufferEventHandler(conf.getInt("hbase.regionserver.hlog.syncer.count", 5),
        maxHandlersCount);
    this.disruptor.handleExceptionsWith(new RingBufferExceptionHandler());
    this.disruptor.handleEventsWith(new RingBufferEventHandler [] {this.ringBufferEventHandler});
    // Presize our map of SyncFutures by handler objects.
    this.syncFuturesByHandler = new ConcurrentHashMap<Thread, SyncFuture>(maxHandlersCount);
    // Starting up threads in constructor is a no no; Interface should have an init call.
    this.disruptor.start();
  }

  /**
   * Get the backing files associated with this WAL.
   * @return may be null if there are no files.
   */
  protected FileStatus[] getFiles() throws IOException {
    return FSUtils.listStatus(fs, fullPathLogDir, ourFiles);
  }

  /**
   * Currently, we need to expose the writer's OutputStream to tests so that they can manipulate
   * the default behavior (such as setting the maxRecoveryErrorCount value for example (see
   * {@link TestWALReplay#testReplayEditsWrittenIntoWAL()}). This is done using reflection on the
   * underlying HDFS OutputStream.
   * NOTE: This could be removed once Hadoop1 support is removed.
   * @return null if underlying stream is not ready.
   */
  @VisibleForTesting
  OutputStream getOutputStream() {
    return this.hdfs_out.getWrappedStream();
  }

  @Override
  public byte [][] rollWriter() throws FailedLogCloseException, IOException {
    return rollWriter(false);
  }

  /**
   * retrieve the next path to use for writing.
   * Increments the internal filenum.
   */
  private Path getNewPath() throws IOException {
    this.filenum.set(System.currentTimeMillis());
    Path newPath = getCurrentFileName();
    while (fs.exists(newPath)) {
      this.filenum.incrementAndGet();
      newPath = getCurrentFileName();
    }
    return newPath;
  }

  Path getOldPath() {
    long currentFilenum = this.filenum.get();
    Path oldPath = null;
    if (currentFilenum > 0) {
      // ComputeFilename  will take care of meta wal filename
      oldPath = computeFilename(currentFilenum);
    } // I presume if currentFilenum is <= 0, this is first file and null for oldPath if fine?
    return oldPath;
  }

  /**
   * Tell listeners about pre log roll.
   * @throws IOException 
   */
  private void tellListenersAboutPreLogRoll(final Path oldPath, final Path newPath)
  throws IOException {
    if (!this.listeners.isEmpty()) {
      for (WALActionsListener i : this.listeners) {
        i.preLogRoll(oldPath, newPath);
      }
    }
  }

  /**
   * Tell listeners about post log roll.
   * @throws IOException 
   */
  private void tellListenersAboutPostLogRoll(final Path oldPath, final Path newPath)
  throws IOException {
    if (!this.listeners.isEmpty()) {
      for (WALActionsListener i : this.listeners) {
        i.postLogRoll(oldPath, newPath);
      }
    }
  }

  /**
   * Run a sync after opening to set up the pipeline.
   * @param nextWriter
   * @param startTimeNanos
   */
  private void preemptiveSync(final ProtobufLogWriter nextWriter) {
    long startTimeNanos = System.nanoTime();
    try {
      nextWriter.sync();
      postSync(System.nanoTime() - startTimeNanos, 0);
    } catch (IOException e) {
      // optimization failed, no need to abort here.
      LOG.warn("pre-sync failed but an optimization so keep going", e);
    }
  }

  @Override
  public byte [][] rollWriter(boolean force) throws FailedLogCloseException, IOException {
    rollWriterLock.lock();
    try {
      // Return if nothing to flush.
      if (!force && (this.writer != null && this.numEntries.get() <= 0)) return null;
      byte [][] regionsToFlush = null;
      if (this.closed) {
        LOG.debug("WAL closed. Skipping rolling of writer");
        return regionsToFlush;
      }
      if (!closeBarrier.beginOp()) {
        LOG.debug("WAL closing. Skipping rolling of writer");
        return regionsToFlush;
      }
      TraceScope scope = Trace.startSpan("FSHLog.rollWriter");
      try {
        Path oldPath = getOldPath();
        Path newPath = getNewPath();
        // Any exception from here on is catastrophic, non-recoverable so we currently abort.
        Writer nextWriter = this.createWriterInstance(newPath);
        FSDataOutputStream nextHdfsOut = null;
        if (nextWriter instanceof ProtobufLogWriter) {
          nextHdfsOut = ((ProtobufLogWriter)nextWriter).getStream();
          // If a ProtobufLogWriter, go ahead and try and sync to force setup of pipeline.
          // If this fails, we just keep going.... it is an optimization, not the end of the world.
          preemptiveSync((ProtobufLogWriter)nextWriter);
        }
        tellListenersAboutPreLogRoll(oldPath, newPath);
        // NewPath could be equal to oldPath if replaceWriter fails.
        newPath = replaceWriter(oldPath, newPath, nextWriter, nextHdfsOut);
        tellListenersAboutPostLogRoll(oldPath, newPath);
        // Can we delete any of the old log files?
        if (getNumRolledLogFiles() > 0) {
          cleanOldLogs();
          regionsToFlush = findRegionsToForceFlush();
        }
      } finally {
        closeBarrier.endOp();
        assert scope == NullScope.INSTANCE || !scope.isDetached();
        scope.close();
      }
      return regionsToFlush;
    } finally {
      rollWriterLock.unlock();
    }
  }

  /**
   * This method allows subclasses to inject different writers without having to
   * extend other methods like rollWriter().
   *
   * @return Writer instance
   */
  protected Writer createWriterInstance(final Path path) throws IOException {
    return DefaultWALProvider.createWriter(conf, fs, path, false);
  }

  private long getLowestSeqId(Map<byte[], Long> seqIdMap) {
    long result = HConstants.NO_SEQNUM;
    for (Long seqNum: seqIdMap.values()) {
      if (result == HConstants.NO_SEQNUM || seqNum.longValue() < result) {
        result = seqNum.longValue();
      }
    }
    return result;
  }

  private <T extends Map<byte[], Long>> Map<byte[], Long> copyMapWithLowestSeqId(
      Map<byte[], T> mapToCopy) {
    Map<byte[], Long> copied = Maps.newHashMap();
    for (Map.Entry<byte[], T> entry: mapToCopy.entrySet()) {
      long lowestSeqId = getLowestSeqId(entry.getValue());
      if (lowestSeqId != HConstants.NO_SEQNUM) {
        copied.put(entry.getKey(), lowestSeqId);
      }
    }
    return copied;
  }

  /**
   * Archive old logs that could be archived: a log is eligible for archiving if all its WALEdits
   * have been flushed to hfiles.
   * <p>
   * For each log file, it compares its region to sequenceId map
   * (@link {@link FSHLog#highestRegionSequenceIds} with corresponding region entries in
   * {@link FSHLog#lowestFlushingRegionSequenceIds} and
   * {@link FSHLog#oldestUnflushedRegionSequenceIds}. If all the regions in the map are flushed
   * past of their value, then the wal is eligible for archiving.
   * @throws IOException
   */
  private void cleanOldLogs() throws IOException {
    Map<byte[], Long> lowestFlushingRegionSequenceIdsLocal = null;
    Map<byte[], Long> oldestUnflushedRegionSequenceIdsLocal = null;
    List<Path> logsToArchive = new ArrayList<Path>();
    // make a local copy so as to avoid locking when we iterate over these maps.
    synchronized (regionSequenceIdLock) {
      lowestFlushingRegionSequenceIdsLocal =
          copyMapWithLowestSeqId(this.lowestFlushingStoreSequenceIds);
      oldestUnflushedRegionSequenceIdsLocal =
          copyMapWithLowestSeqId(this.oldestUnflushedStoreSequenceIds);
    }
    for (Map.Entry<Path, Map<byte[], Long>> e : byWalRegionSequenceIds.entrySet()) {
      // iterate over the log file.
      Path log = e.getKey();
      Map<byte[], Long> sequenceNums = e.getValue();
      // iterate over the map for this log file, and tell whether it should be archive or not.
      if (areAllRegionsFlushed(sequenceNums, lowestFlushingRegionSequenceIdsLocal,
          oldestUnflushedRegionSequenceIdsLocal)) {
        logsToArchive.add(log);
        LOG.debug("WAL file ready for archiving " + log);
      }
    }
    for (Path p : logsToArchive) {
      this.totalLogSize.addAndGet(-this.fs.getFileStatus(p).getLen());
      archiveLogFile(p);
      this.byWalRegionSequenceIds.remove(p);
    }
  }

  /**
   * Takes a region:sequenceId map for a WAL file, and checks whether the file can be archived.
   * It compares the region entries present in the passed sequenceNums map with the local copy of
   * {@link #oldestUnflushedRegionSequenceIds} and {@link #lowestFlushingRegionSequenceIds}. If,
   * for all regions, the value is lesser than the minimum of values present in the
   * oldestFlushing/UnflushedSeqNums, then the wal file is eligible for archiving.
   * @param sequenceNums for a WAL, at the time when it was rolled.
   * @param oldestFlushingMap
   * @param oldestUnflushedMap
   * @return true if wal is eligible for archiving, false otherwise.
   */
   static boolean areAllRegionsFlushed(Map<byte[], Long> sequenceNums,
      Map<byte[], Long> oldestFlushingMap, Map<byte[], Long> oldestUnflushedMap) {
    for (Map.Entry<byte[], Long> regionSeqIdEntry : sequenceNums.entrySet()) {
      // find region entries in the flushing/unflushed map. If there is no entry, it meansj
      // a region doesn't have any unflushed entry.
      long oldestFlushing = oldestFlushingMap.containsKey(regionSeqIdEntry.getKey()) ?
          oldestFlushingMap.get(regionSeqIdEntry.getKey()) : Long.MAX_VALUE;
      long oldestUnFlushed = oldestUnflushedMap.containsKey(regionSeqIdEntry.getKey()) ?
          oldestUnflushedMap.get(regionSeqIdEntry.getKey()) : Long.MAX_VALUE;
          // do a minimum to be sure to contain oldest sequence Id
      long minSeqNum = Math.min(oldestFlushing, oldestUnFlushed);
      if (minSeqNum <= regionSeqIdEntry.getValue()) return false;// can't archive
    }
    return true;
  }

  /**
   * Iterates over the given map of regions, and compares their sequence numbers with corresponding
   * entries in {@link #oldestUnflushedRegionSequenceIds}. If the sequence number is greater or
   * equal, the region is eligible to flush, otherwise, there is no benefit to flush (from the
   * perspective of passed regionsSequenceNums map), because the region has already flushed the
   * entries present in the WAL file for which this method is called for (typically, the oldest
   * wal file).
   * @param regionsSequenceNums
   * @return regions which should be flushed (whose sequence numbers are larger than their
   * corresponding un-flushed entries.
   */
  private byte[][] findEligibleMemstoresToFlush(Map<byte[], Long> regionsSequenceNums) {
    List<byte[]> regionsToFlush = null;
    // Keeping the old behavior of iterating unflushedSeqNums under oldestSeqNumsLock.
    synchronized (regionSequenceIdLock) {
      for (Map.Entry<byte[], Long> e: regionsSequenceNums.entrySet()) {
        long unFlushedVal = getEarliestMemstoreSeqNum(e.getKey());
        if (unFlushedVal != HConstants.NO_SEQNUM && unFlushedVal <= e.getValue()) {
          if (regionsToFlush == null)
            regionsToFlush = new ArrayList<byte[]>();
          regionsToFlush.add(e.getKey());
        }
      }
    }
    return regionsToFlush == null ? null : regionsToFlush
        .toArray(new byte[][] { HConstants.EMPTY_BYTE_ARRAY });
  }

  /**
   * If the number of un-archived WAL files is greater than maximum allowed, it checks
   * the first (oldest) WAL file, and returns the regions which should be flushed so that it could
   * be archived.
   * @return regions to flush in order to archive oldest wal file.
   * @throws IOException
   */
  byte[][] findRegionsToForceFlush() throws IOException {
    byte [][] regions = null;
    int logCount = getNumRolledLogFiles();
    if (logCount > this.maxLogs && logCount > 0) {
      Map.Entry<Path, Map<byte[], Long>> firstWALEntry =
        this.byWalRegionSequenceIds.firstEntry();
      regions = findEligibleMemstoresToFlush(firstWALEntry.getValue());
    }
    if (regions != null) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < regions.length; i++) {
        if (i > 0) sb.append(", ");
        sb.append(Bytes.toStringBinary(regions[i]));
      }
      LOG.info("Too many wals: logs=" + logCount + ", maxlogs=" +
         this.maxLogs + "; forcing flush of " + regions.length + " regions(s): " +
         sb.toString());
    }
    return regions;
  }

  /**
   * Cleans up current writer closing it and then puts in place the passed in
   * <code>nextWriter</code>.
   *
   * In the case of creating a new WAL, oldPath will be null.
   *
   * In the case of rolling over from one file to the next, none of the params will be null.
   *
   * In the case of closing out this FSHLog with no further use newPath, nextWriter, and
   * nextHdfsOut will be null.
   *
   * @param oldPath may be null
   * @param newPath may be null
   * @param nextWriter may be null
   * @param nextHdfsOut may be null
   * @return the passed in <code>newPath</code>
   * @throws IOException if there is a problem flushing or closing the underlying FS
   */
  Path replaceWriter(final Path oldPath, final Path newPath, Writer nextWriter,
      final FSDataOutputStream nextHdfsOut)
  throws IOException {
    // Ask the ring buffer writer to pause at a safe point.  Once we do this, the writer
    // thread will eventually pause. An error hereafter needs to release the writer thread
    // regardless -- hence the finally block below.  Note, this method is called from the FSHLog
    // constructor BEFORE the ring buffer is set running so it is null on first time through
    // here; allow for that.
    SyncFuture syncFuture = null;
    SafePointZigZagLatch zigzagLatch = (this.ringBufferEventHandler == null)?
      null: this.ringBufferEventHandler.attainSafePoint();
    TraceScope scope = Trace.startSpan("FSHFile.replaceWriter");
    try {
      // Wait on the safe point to be achieved.  Send in a sync in case nothing has hit the
      // ring buffer between the above notification of writer that we want it to go to
      // 'safe point' and then here where we are waiting on it to attain safe point.  Use
      // 'sendSync' instead of 'sync' because we do not want this thread to block waiting on it
      // to come back.  Cleanup this syncFuture down below after we are ready to run again.
      try {
        if (zigzagLatch != null) {
          Trace.addTimelineAnnotation("awaiting safepoint");
          syncFuture = zigzagLatch.waitSafePoint(publishSyncOnRingBuffer());
        }
      } catch (FailedSyncBeforeLogCloseException e) {
        if (isUnflushedEntries()) throw e;
        // Else, let is pass through to the close.
        LOG.warn("Failed last sync but no outstanding unsync edits so falling through to close; " +
          e.getMessage());
      }

      // It is at the safe point.  Swap out writer from under the blocked writer thread.
      // TODO: This is close is inline with critical section.  Should happen in background?
      try {
        if (this.writer != null) {
          Trace.addTimelineAnnotation("closing writer");
          this.writer.close();
          Trace.addTimelineAnnotation("writer closed");
        }
        this.closeErrorCount.set(0);
      } catch (IOException ioe) {
        int errors = closeErrorCount.incrementAndGet();
        if (!isUnflushedEntries() && (errors <= this.closeErrorsTolerated)) {
          LOG.warn("Riding over failed WAL close of " + oldPath + ", cause=\"" +
            ioe.getMessage() + "\", errors=" + errors +
            "; THIS FILE WAS NOT CLOSED BUT ALL EDITS SYNCED SO SHOULD BE OK");
        } else {
          throw ioe;
        }
      }
      this.writer = nextWriter;
      this.hdfs_out = nextHdfsOut;
      int oldNumEntries = this.numEntries.get();
      this.numEntries.set(0);
      final String newPathString = (null == newPath ? null : FSUtils.getPath(newPath));
      if (oldPath != null) {
        this.byWalRegionSequenceIds.put(oldPath, this.highestRegionSequenceIds);
        this.highestRegionSequenceIds = new HashMap<byte[], Long>();
        long oldFileLen = this.fs.getFileStatus(oldPath).getLen();
        this.totalLogSize.addAndGet(oldFileLen);
        LOG.info("Rolled WAL " + FSUtils.getPath(oldPath) + " with entries=" + oldNumEntries +
          ", filesize=" + StringUtils.byteDesc(oldFileLen) + "; new WAL " +
          newPathString);
      } else {
        LOG.info("New WAL " + newPathString);
      }
    } catch (InterruptedException ie) {
      // Perpetuate the interrupt
      Thread.currentThread().interrupt();
    } catch (IOException e) {
      long count = getUnflushedEntriesCount();
      LOG.error("Failed close of WAL writer " + oldPath + ", unflushedEntries=" + count, e);
      throw new FailedLogCloseException(oldPath + ", unflushedEntries=" + count, e);
    } finally {
      try {
        // Let the writer thread go regardless, whether error or not.
        if (zigzagLatch != null) {
          zigzagLatch.releaseSafePoint();
          // It will be null if we failed our wait on safe point above.
          if (syncFuture != null) blockOnSync(syncFuture);
        }
      } finally {
        scope.close();
      }
    }
    return newPath;
  }

  long getUnflushedEntriesCount() {
    long highestSynced = this.highestSyncedSequence.get();
    return highestSynced > this.highestUnsyncedSequence?
      0: this.highestUnsyncedSequence - highestSynced;
  }

  boolean isUnflushedEntries() {
    return getUnflushedEntriesCount() > 0;
  }

  /*
   * only public so WALSplitter can use.
   * @return archived location of a WAL file with the given path p
   */
  public static Path getWALArchivePath(Path archiveDir, Path p) {
    return new Path(archiveDir, p.getName());
  }

  private void archiveLogFile(final Path p) throws IOException {
    Path newPath = getWALArchivePath(this.fullPathArchiveDir, p);
    // Tell our listeners that a log is going to be archived.
    if (!this.listeners.isEmpty()) {
      for (WALActionsListener i : this.listeners) {
        i.preLogArchive(p, newPath);
      }
    }
    LOG.info("Archiving " + p + " to " + newPath);
    if (!FSUtils.renameAndSetModifyTime(this.fs, p, newPath)) {
      throw new IOException("Unable to rename " + p + " to " + newPath);
    }
    // Tell our listeners that a log has been archived.
    if (!this.listeners.isEmpty()) {
      for (WALActionsListener i : this.listeners) {
        i.postLogArchive(p, newPath);
      }
    }
  }

  /**
   * This is a convenience method that computes a new filename with a given
   * file-number.
   * @param filenum to use
   * @return Path
   */
  protected Path computeFilename(final long filenum) {
    if (filenum < 0) {
      throw new RuntimeException("wal file number can't be < 0");
    }
    String child = logFilePrefix + WAL_FILE_NAME_DELIMITER + filenum + logFileSuffix;
    return new Path(fullPathLogDir, child);
  }

  /**
   * This is a convenience method that computes a new filename with a given
   * using the current WAL file-number
   * @return Path
   */
  public Path getCurrentFileName() {
    return computeFilename(this.filenum.get());
  }

  @Override
  public String toString() {
    return "FSHLog " + logFilePrefix + ":" + logFileSuffix + "(num " + filenum + ")";
  }

/**
 * A log file has a creation timestamp (in ms) in its file name ({@link #filenum}.
 * This helper method returns the creation timestamp from a given log file.
 * It extracts the timestamp assuming the filename is created with the
 * {@link #computeFilename(long filenum)} method.
 * @param fileName
 * @return timestamp, as in the log file name.
 */
  protected long getFileNumFromFileName(Path fileName) {
    if (fileName == null) throw new IllegalArgumentException("file name can't be null");
    if (!ourFiles.accept(fileName)) {
      throw new IllegalArgumentException("The log file " + fileName +
          " doesn't belong to this wal. (" + toString() + ")");
    }
    final String fileNameString = fileName.toString();
    String chompedPath = fileNameString.substring(prefixPathStr.length(),
        (fileNameString.length() - logFileSuffix.length()));
    return Long.parseLong(chompedPath);
  }

  @Override
  public void close() throws IOException {
    shutdown();
    final FileStatus[] files = getFiles();
    if (null != files && 0 != files.length) {
      for (FileStatus file : files) {
        Path p = getWALArchivePath(this.fullPathArchiveDir, file.getPath());
        // Tell our listeners that a log is going to be archived.
        if (!this.listeners.isEmpty()) {
          for (WALActionsListener i : this.listeners) {
            i.preLogArchive(file.getPath(), p);
          }
        }

        if (!FSUtils.renameAndSetModifyTime(fs, file.getPath(), p)) {
          throw new IOException("Unable to rename " + file.getPath() + " to " + p);
        }
        // Tell our listeners that a log was archived.
        if (!this.listeners.isEmpty()) {
          for (WALActionsListener i : this.listeners) {
            i.postLogArchive(file.getPath(), p);
          }
        }
      }
      LOG.debug("Moved " + files.length + " WAL file(s) to " +
        FSUtils.getPath(this.fullPathArchiveDir));
    }
    LOG.info("Closed WAL: " + toString() );
  }

  @Override
  public void shutdown() throws IOException {
    if (shutdown.compareAndSet(false, true)) {
      try {
        // Prevent all further flushing and rolling.
        closeBarrier.stopAndDrainOps();
      } catch (InterruptedException e) {
        LOG.error("Exception while waiting for cache flushes and log rolls", e);
        Thread.currentThread().interrupt();
      }

      // Shutdown the disruptor.  Will stop after all entries have been processed.  Make sure we
      // have stopped incoming appends before calling this else it will not shutdown.  We are
      // conservative below waiting a long time and if not elapsed, then halting.
      if (this.disruptor != null) {
        long timeoutms = conf.getLong("hbase.wal.disruptor.shutdown.timeout.ms", 60000);
        try {
          this.disruptor.shutdown(timeoutms, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
          LOG.warn("Timed out bringing down disruptor after " + timeoutms + "ms; forcing halt " +
            "(It is a problem if this is NOT an ABORT! -- DATALOSS!!!!)");
          this.disruptor.halt();
          this.disruptor.shutdown();
        }
      }
      // With disruptor down, this is safe to let go.
      if (this.appendExecutor !=  null) this.appendExecutor.shutdown();

      // Tell our listeners that the log is closing
      if (!this.listeners.isEmpty()) {
        for (WALActionsListener i : this.listeners) {
          i.logCloseRequested();
        }
      }
      this.closed = true;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Closing WAL writer in " + FSUtils.getPath(fullPathLogDir));
      }
      if (this.writer != null) {
        this.writer.close();
        this.writer = null;
      }
    }
  }

  /**
   * @param now
   * @param encodedRegionName Encoded name of the region as returned by
   * <code>HRegionInfo#getEncodedNameAsBytes()</code>.
   * @param tableName
   * @param clusterIds that have consumed the change
   * @return New log key.
   */
  protected WALKey makeKey(byte[] encodedRegionName, TableName tableName, long seqnum,
      long now, List<UUID> clusterIds, long nonceGroup, long nonce) {
    // we use HLogKey here instead of WALKey directly to support legacy coprocessors.
    return new HLogKey(encodedRegionName, tableName, seqnum, now, clusterIds, nonceGroup, nonce);
  }
  
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="NP_NULL_ON_SOME_PATH_EXCEPTION",
      justification="Will never be null")
  @Override
  public long append(final HTableDescriptor htd, final HRegionInfo hri, final WALKey key,
      final WALEdit edits, final AtomicLong sequenceId, final boolean inMemstore, 
      final List<Cell> memstoreCells) throws IOException {
    if (this.closed) throw new IOException("Cannot append; log is closed");
    // Make a trace scope for the append.  It is closed on other side of the ring buffer by the
    // single consuming thread.  Don't have to worry about it.
    TraceScope scope = Trace.startSpan("FSHLog.append");

    // This is crazy how much it takes to make an edit.  Do we need all this stuff!!!!????  We need
    // all this to make a key and then below to append the edit, we need to carry htd, info,
    // etc. all over the ring buffer.
    FSWALEntry entry = null;
    long sequence = this.disruptor.getRingBuffer().next();
    try {
      RingBufferTruck truck = this.disruptor.getRingBuffer().get(sequence);
      // Construction of FSWALEntry sets a latch.  The latch is thrown just after we stamp the
      // edit with its edit/sequence id.  The below entry.getRegionSequenceId will wait on the
      // latch to be thrown.  TODO: reuse FSWALEntry as we do SyncFuture rather create per append.
      entry = new FSWALEntry(sequence, key, edits, sequenceId, inMemstore, htd, hri, memstoreCells);
      truck.loadPayload(entry, scope.detach());
    } finally {
      this.disruptor.getRingBuffer().publish(sequence);
    }
    return sequence;
  }

  /**
   * Thread to runs the hdfs sync call. This call takes a while to complete.  This is the longest
   * pole adding edits to the WAL and this must complete to be sure all edits persisted.  We run
   * multiple threads sync'ng rather than one that just syncs in series so we have better
   * latencies; otherwise, an edit that arrived just after a sync started, might have to wait
   * almost the length of two sync invocations before it is marked done.
   * <p>When the sync completes, it marks all the passed in futures done.  On the other end of the
   * sync future is a blocked thread, usually a regionserver Handler.  There may be more than one
   * future passed in the case where a few threads arrive at about the same time and all invoke
   * 'sync'.  In this case we'll batch up the invocations and run one filesystem sync only for a
   * batch of Handler sync invocations.  Do not confuse these Handler SyncFutures with the futures
   * an ExecutorService returns when you call submit. We have no use for these in this model. These
   * SyncFutures are 'artificial', something to hold the Handler until the filesystem sync
   * completes.
   */
  private class SyncRunner extends HasThread {
    private volatile long sequence;
    private final BlockingQueue<SyncFuture> syncFutures;
 
    /**
     * UPDATE! 
     * @param syncs the batch of calls to sync that arrived as this thread was starting; when done,
     * we will put the result of the actual hdfs sync call as the result.
     * @param sequence The sequence number on the ring buffer when this thread was set running.
     * If this actual writer sync completes then all appends up this point have been
     * flushed/synced/pushed to datanodes.  If we fail, then the passed in <code>syncs</code>
     * futures will return the exception to their clients; some of the edits may have made it out
     * to data nodes but we will report all that were part of this session as failed.
     */
    SyncRunner(final String name, final int maxHandlersCount) {
      super(name);
      // LinkedBlockingQueue because of
      // http://www.javacodegeeks.com/2010/09/java-best-practices-queue-battle-and.html
      // Could use other blockingqueues here or concurrent queues.
      //
      // We could let the capacity be 'open' but bound it so we get alerted in pathological case
      // where we cannot sync and we have a bunch of threads all backed up waiting on their syncs
      // to come in.  LinkedBlockingQueue actually shrinks when you remove elements so Q should
      // stay neat and tidy in usual case.  Let the max size be three times the maximum handlers.
      // The passed in maxHandlerCount is the user-level handlers which is what we put up most of
      // but HBase has other handlers running too -- opening region handlers which want to write
      // the meta table when succesful (i.e. sync), closing handlers -- etc.  These are usually
      // much fewer in number than the user-space handlers so Q-size should be user handlers plus
      // some space for these other handlers.  Lets multiply by 3 for good-measure.
      this.syncFutures = new LinkedBlockingQueue<SyncFuture>(maxHandlersCount * 3);
    }

    void offer(final long sequence, final SyncFuture [] syncFutures, final int syncFutureCount) {
      // Set sequence first because the add to the queue will wake the thread if sleeping.
      this.sequence = sequence;
      this.syncFutures.addAll(Arrays.asList(syncFutures).subList(0, syncFutureCount));
    }

    /**
     * Release the passed <code>syncFuture</code>
     * @param syncFuture
     * @param currentSequence
     * @param t
     * @return Returns 1.
     */
    private int releaseSyncFuture(final SyncFuture syncFuture, final long currentSequence,
        final Throwable t) {
      if (!syncFuture.done(currentSequence, t)) throw new IllegalStateException();
      // This function releases one sync future only.
      return 1;
    }
 
    /**
     * Release all SyncFutures whose sequence is <= <code>currentSequence</code>.
     * @param currentSequence
     * @param t May be non-null if we are processing SyncFutures because an exception was thrown.
     * @return Count of SyncFutures we let go.
     */
    private int releaseSyncFutures(final long currentSequence, final Throwable t) {
      int syncCount = 0;
      for (SyncFuture syncFuture; (syncFuture = this.syncFutures.peek()) != null;) {
        if (syncFuture.getRingBufferSequence() > currentSequence) break;
        releaseSyncFuture(syncFuture, currentSequence, t);
        if (!this.syncFutures.remove(syncFuture)) {
          throw new IllegalStateException(syncFuture.toString());
        }
        syncCount++;
      }
      return syncCount;
    }

    /**
     * @param sequence The sequence we ran the filesystem sync against.
     * @return Current highest synced sequence.
     */
    private long updateHighestSyncedSequence(long sequence) {
      long currentHighestSyncedSequence;
      // Set the highestSyncedSequence IFF our current sequence id is the 'highest'.
      do {
        currentHighestSyncedSequence = highestSyncedSequence.get();
        if (currentHighestSyncedSequence >= sequence) {
          // Set the sync number to current highwater mark; might be able to let go more
          // queued sync futures
          sequence = currentHighestSyncedSequence;
          break;
        }
      } while (!highestSyncedSequence.compareAndSet(currentHighestSyncedSequence, sequence));
      return sequence;
    }

    public void run() {
      long currentSequence;
      while (!isInterrupted()) {
        int syncCount = 0;
        SyncFuture takeSyncFuture;
        try {
          while (true) {
            // We have to process what we 'take' from the queue
            takeSyncFuture = this.syncFutures.take();
            currentSequence = this.sequence;
            long syncFutureSequence = takeSyncFuture.getRingBufferSequence();
            if (syncFutureSequence > currentSequence) {
              throw new IllegalStateException("currentSequence=" + syncFutureSequence +
                ", syncFutureSequence=" + syncFutureSequence);
            }
            // See if we can process any syncfutures BEFORE we go sync.
            long currentHighestSyncedSequence = highestSyncedSequence.get();
            if (currentSequence < currentHighestSyncedSequence) {
              syncCount += releaseSyncFuture(takeSyncFuture, currentHighestSyncedSequence, null);
              // Done with the 'take'.  Go around again and do a new 'take'.
              continue;
            }
            break;
          }
          // I got something.  Lets run.  Save off current sequence number in case it changes
          // while we run.
          TraceScope scope = Trace.continueSpan(takeSyncFuture.getSpan());
          long start = System.nanoTime();
          Throwable t = null;
          try {
            Trace.addTimelineAnnotation("syncing writer");
            writer.sync();
            Trace.addTimelineAnnotation("writer synced");
            currentSequence = updateHighestSyncedSequence(currentSequence);
          } catch (IOException e) {
            LOG.error("Error syncing, request close of wal ", e);
            t = e;
          } catch (Exception e) {
            LOG.warn("UNEXPECTED", e);
            t = e;
          } finally {
            // reattach the span to the future before releasing.
            takeSyncFuture.setSpan(scope.detach());
            // First release what we 'took' from the queue.
            syncCount += releaseSyncFuture(takeSyncFuture, currentSequence, t);
            // Can we release other syncs?
            syncCount += releaseSyncFutures(currentSequence, t);
            if (t != null) {
              requestLogRoll();
            } else checkLogRoll();
          }
          postSync(System.nanoTime() - start, syncCount);
        } catch (InterruptedException e) {
          // Presume legit interrupt.
          Thread.currentThread().interrupt();
        } catch (Throwable t) {
          LOG.warn("UNEXPECTED, continuing", t);
        }
      }
    }
  }

  /**
   * Schedule a log roll if needed.
   */
  void checkLogRoll() {
    // Will return immediately if we are in the middle of a WAL log roll currently.
    if (!rollWriterLock.tryLock()) return;
    boolean lowReplication;
    try {
      lowReplication = checkLowReplication();
    } finally {
      rollWriterLock.unlock();
    }
    try {
      if (lowReplication || writer != null && writer.getLength() > logrollsize) {
        requestLogRoll(lowReplication);
      }
    } catch (IOException e) {
      LOG.warn("Writer.getLength() failed; continuing", e);
    }
  }

  /*
   * @return true if number of replicas for the WAL is lower than threshold
   */
  private boolean checkLowReplication() {
    boolean logRollNeeded = false;
    // if the number of replicas in HDFS has fallen below the configured
    // value, then roll logs.
    try {
      int numCurrentReplicas = getLogReplication();
      if (numCurrentReplicas != 0 && numCurrentReplicas < this.minTolerableReplication) {
        if (this.lowReplicationRollEnabled) {
          if (this.consecutiveLogRolls.get() < this.lowReplicationRollLimit) {
            LOG.warn("HDFS pipeline error detected. " + "Found "
                + numCurrentReplicas + " replicas but expecting no less than "
                + this.minTolerableReplication + " replicas. "
                + " Requesting close of wal. current pipeline: "
                + Arrays.toString(getPipeLine()));
            logRollNeeded = true;
            // If rollWriter is requested, increase consecutiveLogRolls. Once it
            // is larger than lowReplicationRollLimit, disable the
            // LowReplication-Roller
            this.consecutiveLogRolls.getAndIncrement();
          } else {
            LOG.warn("Too many consecutive RollWriter requests, it's a sign of "
                + "the total number of live datanodes is lower than the tolerable replicas.");
            this.consecutiveLogRolls.set(0);
            this.lowReplicationRollEnabled = false;
          }
        }
      } else if (numCurrentReplicas >= this.minTolerableReplication) {
        if (!this.lowReplicationRollEnabled) {
          // The new writer's log replicas is always the default value.
          // So we should not enable LowReplication-Roller. If numEntries
          // is lower than or equals 1, we consider it as a new writer.
          if (this.numEntries.get() <= 1) {
            return logRollNeeded;
          }
          // Once the live datanode number and the replicas return to normal,
          // enable the LowReplication-Roller.
          this.lowReplicationRollEnabled = true;
          LOG.info("LowReplication-Roller was enabled.");
        }
      }
    } catch (Exception e) {
      LOG.warn("Unable to invoke DFSOutputStream.getNumCurrentReplicas" + e +
        " still proceeding ahead...");
    }
    return logRollNeeded;
  }

  private SyncFuture publishSyncOnRingBuffer() {
    return publishSyncOnRingBuffer(null);
  }

  private SyncFuture publishSyncOnRingBuffer(Span span) {
    long sequence = this.disruptor.getRingBuffer().next();
    SyncFuture syncFuture = getSyncFuture(sequence, span);
    try {
      RingBufferTruck truck = this.disruptor.getRingBuffer().get(sequence);
      truck.loadPayload(syncFuture);
    } finally {
      this.disruptor.getRingBuffer().publish(sequence);
    }
    return syncFuture;
  }

  // Sync all known transactions
  private Span publishSyncThenBlockOnCompletion(Span span) throws IOException {
    return blockOnSync(publishSyncOnRingBuffer(span));
  }

  private Span blockOnSync(final SyncFuture syncFuture) throws IOException {
    // Now we have published the ringbuffer, halt the current thread until we get an answer back.
    try {
      syncFuture.get();
      return syncFuture.getSpan();
    } catch (InterruptedException ie) {
      LOG.warn("Interrupted", ie);
      throw convertInterruptedExceptionToIOException(ie);
    } catch (ExecutionException e) {
      throw ensureIOException(e.getCause());
    }
  }

  private IOException convertInterruptedExceptionToIOException(final InterruptedException ie) {
    Thread.currentThread().interrupt();
    IOException ioe = new InterruptedIOException();
    ioe.initCause(ie);
    return ioe;
  }

  private SyncFuture getSyncFuture(final long sequence, Span span) {
    SyncFuture syncFuture = this.syncFuturesByHandler.get(Thread.currentThread());
    if (syncFuture == null) {
      syncFuture = new SyncFuture();
      this.syncFuturesByHandler.put(Thread.currentThread(), syncFuture);
    }
    return syncFuture.reset(sequence, span);
  }

  private void postSync(final long timeInNanos, final int handlerSyncs) {
    if (timeInNanos > this.slowSyncNs) {
      String msg =
          new StringBuilder().append("Slow sync cost: ")
              .append(timeInNanos / 1000000).append(" ms, current pipeline: ")
              .append(Arrays.toString(getPipeLine())).toString();
      Trace.addTimelineAnnotation(msg);
      LOG.info(msg);
    }
    if (!listeners.isEmpty()) {
      for (WALActionsListener listener : listeners) {
        listener.postSync(timeInNanos, handlerSyncs);
      }
    }
  }

  private long postAppend(final Entry e, final long elapsedTime) {
    long len = 0;
    if (!listeners.isEmpty()) {
      for (Cell cell : e.getEdit().getCells()) {
        len += CellUtil.estimatedSerializedSizeOf(cell);
      }
      for (WALActionsListener listener : listeners) {
        listener.postAppend(len, elapsedTime);
      }
    }
    return len;
  }

  /**
   * Find the 'getNumCurrentReplicas' on the passed <code>os</code> stream.
   * This is used for getting current replicas of a file being written.
   * @return Method or null.
   */
  private Method getGetNumCurrentReplicas(final FSDataOutputStream os) {
    // TODO: Remove all this and use the now publically available
    // HdfsDataOutputStream#getCurrentBlockReplication()
    Method m = null;
    if (os != null) {
      Class<? extends OutputStream> wrappedStreamClass = os.getWrappedStream().getClass();
      try {
        m = wrappedStreamClass.getDeclaredMethod("getNumCurrentReplicas", new Class<?>[] {});
        m.setAccessible(true);
      } catch (NoSuchMethodException e) {
        LOG.info("FileSystem's output stream doesn't support getNumCurrentReplicas; " +
         "HDFS-826 not available; fsOut=" + wrappedStreamClass.getName());
      } catch (SecurityException e) {
        LOG.info("No access to getNumCurrentReplicas on FileSystems's output stream; HDFS-826 " +
          "not available; fsOut=" + wrappedStreamClass.getName(), e);
        m = null; // could happen on setAccessible()
      }
    }
    if (m != null) {
      if (LOG.isTraceEnabled()) LOG.trace("Using getNumCurrentReplicas");
    }
    return m;
  }

  /**
   * This method gets the datanode replication count for the current WAL.
   *
   * If the pipeline isn't started yet or is empty, you will get the default
   * replication factor.  Therefore, if this function returns 0, it means you
   * are not properly running with the HDFS-826 patch.
   * @throws InvocationTargetException
   * @throws IllegalAccessException
   * @throws IllegalArgumentException
   *
   * @throws Exception
   */
  @VisibleForTesting
  int getLogReplication()
  throws IllegalArgumentException, IllegalAccessException, InvocationTargetException {
    final OutputStream stream = getOutputStream();
    if (this.getNumCurrentReplicas != null && stream != null) {
      Object repl = this.getNumCurrentReplicas.invoke(stream, NO_ARGS);
      if (repl instanceof Integer) {
        return ((Integer)repl).intValue();
      }
    }
    return 0;
  }

  @Override
  public void sync() throws IOException {
    TraceScope scope = Trace.startSpan("FSHLog.sync");
    try {
      scope = Trace.continueSpan(publishSyncThenBlockOnCompletion(scope.detach()));
    } finally {
      assert scope == NullScope.INSTANCE || !scope.isDetached();
      scope.close();
    }
  }

  @Override
  public void sync(long txid) throws IOException {
    if (this.highestSyncedSequence.get() >= txid){
      // Already sync'd.
      return;
    }
    TraceScope scope = Trace.startSpan("FSHLog.sync");
    try {
      scope = Trace.continueSpan(publishSyncThenBlockOnCompletion(scope.detach()));
    } finally {
      assert scope == NullScope.INSTANCE || !scope.isDetached();
      scope.close();
    }
  }

  // public only until class moves to o.a.h.h.wal
  public void requestLogRoll() {
    requestLogRoll(false);
  }

  private void requestLogRoll(boolean tooFewReplicas) {
    if (!this.listeners.isEmpty()) {
      for (WALActionsListener i: this.listeners) {
        i.logRollRequested(tooFewReplicas);
      }
    }
  }

  // public only until class moves to o.a.h.h.wal
  /** @return the number of rolled log files */
  public int getNumRolledLogFiles() {
    return byWalRegionSequenceIds.size();
  }

  // public only until class moves to o.a.h.h.wal
  /** @return the number of log files in use */
  public int getNumLogFiles() {
    // +1 for current use log
    return getNumRolledLogFiles() + 1;
  }

  // public only until class moves to o.a.h.h.wal
  /** @return the size of log files in use */
  public long getLogFileSize() {
    return this.totalLogSize.get();
  }

  @Override
  public boolean startCacheFlush(final byte[] encodedRegionName,
      Set<byte[]> flushedFamilyNames) {
    Map<byte[], Long> oldStoreSeqNum = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    if (!closeBarrier.beginOp()) {
      LOG.info("Flush will not be started for " + Bytes.toString(encodedRegionName) +
        " - because the server is closing.");
      return false;
    }
    synchronized (regionSequenceIdLock) {
      ConcurrentMap<byte[], Long> oldestUnflushedStoreSequenceIdsOfRegion =
          oldestUnflushedStoreSequenceIds.get(encodedRegionName);
      if (oldestUnflushedStoreSequenceIdsOfRegion != null) {
        for (byte[] familyName: flushedFamilyNames) {
          Long seqId = oldestUnflushedStoreSequenceIdsOfRegion.remove(familyName);
          if (seqId != null) {
            oldStoreSeqNum.put(familyName, seqId);
          }
        }
        if (!oldStoreSeqNum.isEmpty()) {
          Map<byte[], Long> oldValue = this.lowestFlushingStoreSequenceIds.put(
              encodedRegionName, oldStoreSeqNum);
          assert oldValue == null: "Flushing map not cleaned up for "
              + Bytes.toString(encodedRegionName);
        }
        if (oldestUnflushedStoreSequenceIdsOfRegion.isEmpty()) {
          // Remove it otherwise it will be in oldestUnflushedStoreSequenceIds for ever
          // even if the region is already moved to other server.
          // Do not worry about data racing, we held write lock of region when calling
          // startCacheFlush, so no one can add value to the map we removed.
          oldestUnflushedStoreSequenceIds.remove(encodedRegionName);
        }
      }
    }
    if (oldStoreSeqNum.isEmpty()) {
      // TODO: if we have no oldStoreSeqNum, and WAL is not disabled, presumably either
      // the region is already flushing (which would make this call invalid), or there
      // were no appends after last flush, so why are we starting flush? Maybe we should
      // assert not empty. Less rigorous, but safer, alternative is telling the caller to stop.
      // For now preserve old logic.
      LOG.warn("Couldn't find oldest seqNum for the region we are about to flush: ["
        + Bytes.toString(encodedRegionName) + "]");
    }
    return true;
  }

  @Override
  public void completeCacheFlush(final byte [] encodedRegionName) {
    synchronized (regionSequenceIdLock) {
      this.lowestFlushingStoreSequenceIds.remove(encodedRegionName);
    }
    closeBarrier.endOp();
  }

  private ConcurrentMap<byte[], Long> getOrCreateOldestUnflushedStoreSequenceIdsOfRegion(
      byte[] encodedRegionName) {
    ConcurrentMap<byte[], Long> oldestUnflushedStoreSequenceIdsOfRegion =
        oldestUnflushedStoreSequenceIds.get(encodedRegionName);
    if (oldestUnflushedStoreSequenceIdsOfRegion != null) {
      return oldestUnflushedStoreSequenceIdsOfRegion;
    }
    oldestUnflushedStoreSequenceIdsOfRegion =
        new ConcurrentSkipListMap<byte[], Long>(Bytes.BYTES_COMPARATOR);
    ConcurrentMap<byte[], Long> alreadyPut =
        oldestUnflushedStoreSequenceIds.putIfAbsent(encodedRegionName,
          oldestUnflushedStoreSequenceIdsOfRegion);
    return alreadyPut == null ? oldestUnflushedStoreSequenceIdsOfRegion : alreadyPut;
  }

  @Override
  public void abortCacheFlush(byte[] encodedRegionName) {
    Map<byte[], Long> storeSeqNumsBeforeFlushStarts;
    Map<byte[], Long> currentStoreSeqNums = new TreeMap<byte[], Long>(Bytes.BYTES_COMPARATOR);
    synchronized (regionSequenceIdLock) {
      storeSeqNumsBeforeFlushStarts = this.lowestFlushingStoreSequenceIds.remove(
        encodedRegionName);
      if (storeSeqNumsBeforeFlushStarts != null) {
        ConcurrentMap<byte[], Long> oldestUnflushedStoreSequenceIdsOfRegion =
            getOrCreateOldestUnflushedStoreSequenceIdsOfRegion(encodedRegionName);
        for (Map.Entry<byte[], Long> familyNameAndSeqId: storeSeqNumsBeforeFlushStarts
            .entrySet()) {
          currentStoreSeqNums.put(familyNameAndSeqId.getKey(),
            oldestUnflushedStoreSequenceIdsOfRegion.put(familyNameAndSeqId.getKey(),
              familyNameAndSeqId.getValue()));
        }
      }
    }
    closeBarrier.endOp();
    if (storeSeqNumsBeforeFlushStarts != null) {
      for (Map.Entry<byte[], Long> familyNameAndSeqId : storeSeqNumsBeforeFlushStarts.entrySet()) {
        Long currentSeqNum = currentStoreSeqNums.get(familyNameAndSeqId.getKey());
        if (currentSeqNum != null
            && currentSeqNum.longValue() <= familyNameAndSeqId.getValue().longValue()) {
          String errorStr =
              "Region " + Bytes.toString(encodedRegionName) + " family "
                  + Bytes.toString(familyNameAndSeqId.getKey())
                  + " acquired edits out of order current memstore seq=" + currentSeqNum
                  + ", previous oldest unflushed id=" + familyNameAndSeqId.getValue();
          LOG.error(errorStr);
          Runtime.getRuntime().halt(1);
        }
      }
    }
  }

  @VisibleForTesting
  boolean isLowReplicationRollEnabled() {
      return lowReplicationRollEnabled;
  }

  public static final long FIXED_OVERHEAD = ClassSize.align(
    ClassSize.OBJECT + (5 * ClassSize.REFERENCE) +
    ClassSize.ATOMIC_INTEGER + Bytes.SIZEOF_INT + (3 * Bytes.SIZEOF_LONG));

  private static void split(final Configuration conf, final Path p)
  throws IOException {
    FileSystem fs = FileSystem.get(conf);
    if (!fs.exists(p)) {
      throw new FileNotFoundException(p.toString());
    }
    if (!fs.getFileStatus(p).isDirectory()) {
      throw new IOException(p + " is not a directory");
    }

    final Path baseDir = FSUtils.getRootDir(conf);
    final Path archiveDir = new Path(baseDir, HConstants.HREGION_OLDLOGDIR_NAME);
    WALSplitter.split(baseDir, p, archiveDir, fs, conf, WALFactory.getInstance(conf));
  }


  @Override
  public long getEarliestMemstoreSeqNum(byte[] encodedRegionName) {
    ConcurrentMap<byte[], Long> oldestUnflushedStoreSequenceIdsOfRegion =
        this.oldestUnflushedStoreSequenceIds.get(encodedRegionName);
    return oldestUnflushedStoreSequenceIdsOfRegion != null ?
        getLowestSeqId(oldestUnflushedStoreSequenceIdsOfRegion) : HConstants.NO_SEQNUM;
  }

  @Override
  public long getEarliestMemstoreSeqNum(byte[] encodedRegionName,
      byte[] familyName) {
    ConcurrentMap<byte[], Long> oldestUnflushedStoreSequenceIdsOfRegion =
        this.oldestUnflushedStoreSequenceIds.get(encodedRegionName);
    if (oldestUnflushedStoreSequenceIdsOfRegion != null) {
      Long result = oldestUnflushedStoreSequenceIdsOfRegion.get(familyName);
      return result != null ? result.longValue() : HConstants.NO_SEQNUM;
    } else {
      return HConstants.NO_SEQNUM;
    }
  }

  /**
   * This class is used coordinating two threads holding one thread at a
   * 'safe point' while the orchestrating thread does some work that requires the first thread
   * paused: e.g. holding the WAL writer while its WAL is swapped out from under it by another
   * thread.
   * 
   * <p>Thread A signals Thread B to hold when it gets to a 'safe point'.  Thread A wait until
   * Thread B gets there. When the 'safe point' has been attained, Thread B signals Thread A.
   * Thread B then holds at the 'safe point'.  Thread A on notification that Thread B is paused,
   * goes ahead and does the work it needs to do while Thread B is holding.  When Thread A is done,
   * it flags B and then Thread A and Thread B continue along on their merry way.  Pause and
   * signalling 'zigzags' between the two participating threads.  We use two latches -- one the
   * inverse of the other -- pausing and signaling when states are achieved.
   * 
   * <p>To start up the drama, Thread A creates an instance of this class each time it would do
   * this zigzag dance and passes it to Thread B (these classes use Latches so it is one shot
   * only). Thread B notices the new instance (via reading a volatile reference or how ever) and it
   * starts to work toward the 'safe point'.  Thread A calls {@link #waitSafePoint()} when it
   * cannot proceed until the Thread B 'safe point' is attained. Thread A will be held inside in
   * {@link #waitSafePoint()} until Thread B reaches the 'safe point'.  Once there, Thread B
   * frees Thread A by calling {@link #safePointAttained()}.  Thread A now knows Thread B
   * is at the 'safe point' and that it is holding there (When Thread B calls
   * {@link #safePointAttained()} it blocks here until Thread A calls {@link #releaseSafePoint()}).
   * Thread A proceeds to do what it needs to do while Thread B is paused.  When finished,
   * it lets Thread B lose by calling {@link #releaseSafePoint()} and away go both Threads again.
   */
  static class SafePointZigZagLatch {
    /**
     * Count down this latch when safe point attained.
     */
    private volatile CountDownLatch safePointAttainedLatch = new CountDownLatch(1);
    /**
     * Latch to wait on.  Will be released when we can proceed.
     */
    private volatile CountDownLatch safePointReleasedLatch = new CountDownLatch(1);
 
    /**
     * For Thread A to call when it is ready to wait on the 'safe point' to be attained.
     * Thread A will be held in here until Thread B calls {@link #safePointAttained()}
     * @throws InterruptedException
     * @throws ExecutionException
     * @param syncFuture We need this as barometer on outstanding syncs.  If it comes home with
     * an exception, then something is up w/ our syncing.
     * @return The passed <code>syncFuture</code>
     * @throws FailedSyncBeforeLogCloseException 
     */
    SyncFuture waitSafePoint(final SyncFuture syncFuture)
    throws InterruptedException, FailedSyncBeforeLogCloseException {
      while (true) {
        if (this.safePointAttainedLatch.await(1, TimeUnit.NANOSECONDS)) break;
        if (syncFuture.isThrowable()) {
          throw new FailedSyncBeforeLogCloseException(syncFuture.getThrowable());
        }
      }
      return syncFuture;
    }
 
    /**
     * Called by Thread B when it attains the 'safe point'.  In this method, Thread B signals
     * Thread A it can proceed. Thread B will be held in here until {@link #releaseSafePoint()}
     * is called by Thread A.
     * @throws InterruptedException
     */
    void safePointAttained() throws InterruptedException {
      this.safePointAttainedLatch.countDown();
      this.safePointReleasedLatch.await();
    }

    /**
     * Called by Thread A when it is done with the work it needs to do while Thread B is
     * halted.  This will release the Thread B held in a call to {@link #safePointAttained()}
     */
    void releaseSafePoint() {
      this.safePointReleasedLatch.countDown();
    }

    /**
     * @return True is this is a 'cocked', fresh instance, and not one that has already fired.
     */
    boolean isCocked() {
      return this.safePointAttainedLatch.getCount() > 0 &&
        this.safePointReleasedLatch.getCount() > 0;
    }
  }

  /**
   * Handler that is run by the disruptor ringbuffer consumer. Consumer is a SINGLE
   * 'writer/appender' thread.  Appends edits and starts up sync runs.  Tries its best to batch up
   * syncs.  There is no discernible benefit batching appends so we just append as they come in
   * because it simplifies the below implementation.  See metrics for batching effectiveness
   * (In measurement, at 100 concurrent handlers writing 1k, we are batching > 10 appends and 10
   * handler sync invocations for every actual dfsclient sync call; at 10 concurrent handlers,
   * YMMV).
   * <p>Herein, we have an array into which we store the sync futures as they come in.  When we
   * have a 'batch', we'll then pass what we have collected to a SyncRunner thread to do the
   * filesystem sync.  When it completes, it will then call
   * {@link SyncFuture#done(long, Throwable)} on each of SyncFutures in the batch to release
   * blocked Handler threads.
   * <p>I've tried various effects to try and make latencies low while keeping throughput high.
   * I've tried keeping a single Queue of SyncFutures in this class appending to its tail as the
   * syncs coming and having sync runner threads poll off the head to 'finish' completed
   * SyncFutures.  I've tried linkedlist, and various from concurrent utils whether
   * LinkedBlockingQueue or ArrayBlockingQueue, etc.  The more points of synchronization, the
   * more 'work' (according to 'perf stats') that has to be done; small increases in stall
   * percentages seem to have a big impact on throughput/latencies.  The below model where we have
   * an array into which we stash the syncs and then hand them off to the sync thread seemed like
   * a decent compromise.  See HBASE-8755 for more detail.
   */
  class RingBufferEventHandler implements EventHandler<RingBufferTruck>, LifecycleAware {
    private final SyncRunner [] syncRunners;
    private final SyncFuture [] syncFutures;
    // Had 'interesting' issues when this was non-volatile.  On occasion, we'd not pass all
    // syncFutures to the next sync'ing thread.
    private volatile int syncFuturesCount = 0;
    private volatile SafePointZigZagLatch zigzagLatch;
    /**
     * Object to block on while waiting on safe point.
     */
    private final Object safePointWaiter = new Object();
    private volatile boolean shutdown = false;

    /**
     * Which syncrunner to use next.
     */
    private int syncRunnerIndex;

    RingBufferEventHandler(final int syncRunnerCount, final int maxHandlersCount) {
      this.syncFutures = new SyncFuture[maxHandlersCount];
      this.syncRunners = new SyncRunner[syncRunnerCount];
      for (int i = 0; i < syncRunnerCount; i++) {
        this.syncRunners[i] = new SyncRunner("sync." + i, maxHandlersCount);
      }
    }

    private void cleanupOutstandingSyncsOnException(final long sequence, final Exception e) {
      for (int i = 0; i < this.syncFuturesCount; i++) this.syncFutures[i].done(sequence, e);
      this.syncFuturesCount = 0;
    }

    @Override
    // We can set endOfBatch in the below method if at end of our this.syncFutures array
    public void onEvent(final RingBufferTruck truck, final long sequence, boolean endOfBatch)
    throws Exception {
      // Appends and syncs are coming in order off the ringbuffer.  We depend on this fact.  We'll
      // add appends to dfsclient as they come in.  Batching appends doesn't give any significant
      // benefit on measurement.  Handler sync calls we will batch up.

      try {
        if (truck.hasSyncFuturePayload()) {
          this.syncFutures[this.syncFuturesCount++] = truck.unloadSyncFuturePayload();
          // Force flush of syncs if we are carrying a full complement of syncFutures.
          if (this.syncFuturesCount == this.syncFutures.length) endOfBatch = true;
        } else if (truck.hasFSWALEntryPayload()) {
          TraceScope scope = Trace.continueSpan(truck.unloadSpanPayload());
          try {
            append(truck.unloadFSWALEntryPayload());
          } catch (Exception e) {
            // If append fails, presume any pending syncs will fail too; let all waiting handlers
            // know of the exception.
            cleanupOutstandingSyncsOnException(sequence, e);
            // Return to keep processing.
            return;
          } finally {
            assert scope == NullScope.INSTANCE || !scope.isDetached();
            scope.close(); // append scope is complete
          }
        } else {
          // They can't both be null.  Fail all up to this!!!
          cleanupOutstandingSyncsOnException(sequence,
            new IllegalStateException("Neither append nor sync"));
          // Return to keep processing.
          return;
        }

        // TODO: Check size and if big go ahead and call a sync if we have enough data.

        // If not a batch, return to consume more events from the ring buffer before proceeding;
        // we want to get up a batch of syncs and appends before we go do a filesystem sync.
        if (!endOfBatch || this.syncFuturesCount <= 0) return;

        // Now we have a batch.

        if (LOG.isTraceEnabled()) {
          LOG.trace("Sequence=" + sequence + ", syncCount=" + this.syncFuturesCount);
        }

        // Below expects that the offer 'transfers' responsibility for the outstanding syncs to the
        // syncRunner. We should never get an exception in here. HBASE-11145 was because queue
        // was sized exactly to the count of user handlers but we could have more if we factor in
        // meta handlers doing opens and closes.
        int index = Math.abs(this.syncRunnerIndex++) % this.syncRunners.length;
        try {
          this.syncRunners[index].offer(sequence, this.syncFutures, this.syncFuturesCount);
        } catch (Exception e) {
          cleanupOutstandingSyncsOnException(sequence, e);
          throw e;
        }
        attainSafePoint(sequence);
        this.syncFuturesCount = 0;
      } catch (Throwable t) {
        LOG.error("UNEXPECTED!!! syncFutures.length=" + this.syncFutures.length, t);
      }
    }

    SafePointZigZagLatch attainSafePoint() {
      this.zigzagLatch = new SafePointZigZagLatch();
      return this.zigzagLatch;
    }

    /**
     * Check if we should attain safe point.  If so, go there and then wait till signalled before
     * we proceeding.
     */
    private void attainSafePoint(final long currentSequence) {
      if (this.zigzagLatch == null || !this.zigzagLatch.isCocked()) return;
      // If here, another thread is waiting on us to get to safe point.  Don't leave it hanging.
      try {
        // Wait on outstanding syncers; wait for them to finish syncing (unless we've been
        // shutdown or unless our latch has been thrown because we have been aborted).
        while (!this.shutdown && this.zigzagLatch.isCocked() &&
            highestSyncedSequence.get() < currentSequence) {
          synchronized (this.safePointWaiter) {
            this.safePointWaiter.wait(0, 1);
          }
        }
        // Tell waiting thread we've attained safe point
        this.zigzagLatch.safePointAttained();
      } catch (InterruptedException e) {
        LOG.warn("Interrupted ", e);
        Thread.currentThread().interrupt();
      }
    }

    private void updateOldestUnflushedSequenceIds(byte[] encodedRegionName,
        Set<byte[]> familyNameSet, Long lRegionSequenceId) {
      ConcurrentMap<byte[], Long> oldestUnflushedStoreSequenceIdsOfRegion =
          getOrCreateOldestUnflushedStoreSequenceIdsOfRegion(encodedRegionName);
      for (byte[] familyName : familyNameSet) {
        oldestUnflushedStoreSequenceIdsOfRegion.putIfAbsent(familyName, lRegionSequenceId);
      }
    }

    /**
     * Append to the WAL.  Does all CP and WAL listener calls.
     * @param entry
     * @throws Exception
     */
    void append(final FSWALEntry entry) throws Exception {
      // TODO: WORK ON MAKING THIS APPEND FASTER. DOING WAY TOO MUCH WORK WITH CPs, PBing, etc.
      atHeadOfRingBufferEventHandlerAppend();

      long start = EnvironmentEdgeManager.currentTime();
      byte [] encodedRegionName = entry.getKey().getEncodedRegionName();
      long regionSequenceId = WALKey.NO_SEQUENCE_ID;
      try {
        // We are about to append this edit; update the region-scoped sequence number.  Do it
        // here inside this single appending/writing thread.  Events are ordered on the ringbuffer
        // so region sequenceids will also be in order.
        regionSequenceId = entry.stampRegionSequenceId();
        
        // Edits are empty, there is nothing to append.  Maybe empty when we are looking for a 
        // region sequence id only, a region edit/sequence id that is not associated with an actual 
        // edit. It has to go through all the rigmarole to be sure we have the right ordering.
        if (entry.getEdit().isEmpty()) {
          return;
        }
        
        // Coprocessor hook.
        if (!coprocessorHost.preWALWrite(entry.getHRegionInfo(), entry.getKey(),
            entry.getEdit())) {
          if (entry.getEdit().isReplay()) {
            // Set replication scope null so that this won't be replicated
            entry.getKey().setScopes(null);
          }
        }
        if (!listeners.isEmpty()) {
          for (WALActionsListener i: listeners) {
            // TODO: Why does listener take a table description and CPs take a regioninfo?  Fix.
            i.visitLogEntryBeforeWrite(entry.getHTableDescriptor(), entry.getKey(),
              entry.getEdit());
          }
        }

        writer.append(entry);
        assert highestUnsyncedSequence < entry.getSequence();
        highestUnsyncedSequence = entry.getSequence();
        Long lRegionSequenceId = Long.valueOf(regionSequenceId);
        highestRegionSequenceIds.put(encodedRegionName, lRegionSequenceId);
        if (entry.isInMemstore()) {
          updateOldestUnflushedSequenceIds(encodedRegionName,
              entry.getFamilyNames(), lRegionSequenceId);
        }

        coprocessorHost.postWALWrite(entry.getHRegionInfo(), entry.getKey(), entry.getEdit());
        // Update metrics.
        postAppend(entry, EnvironmentEdgeManager.currentTime() - start);
      } catch (Exception e) {
        LOG.fatal("Could not append. Requesting close of wal", e);
        requestLogRoll();
        throw e;
      }
      numEntries.incrementAndGet();
    }

    @Override
    public void onStart() {
      for (SyncRunner syncRunner: this.syncRunners) syncRunner.start();
    }

    @Override
    public void onShutdown() {
      for (SyncRunner syncRunner: this.syncRunners) syncRunner.interrupt();
    }
  }

  /**
   * Exposed for testing only.  Use to tricks like halt the ring buffer appending.
   */
  @VisibleForTesting
  void atHeadOfRingBufferEventHandlerAppend() {
    // Noop
  }

  private static IOException ensureIOException(final Throwable t) {
    return (t instanceof IOException)? (IOException)t: new IOException(t);
  }

  private static void usage() {
    System.err.println("Usage: FSHLog <ARGS>");
    System.err.println("Arguments:");
    System.err.println(" --dump  Dump textual representation of passed one or more files");
    System.err.println("         For example: " +
      "FSHLog --dump hdfs://example.com:9000/hbase/.logs/MACHINE/LOGFILE");
    System.err.println(" --split Split the passed directory of WAL logs");
    System.err.println("         For example: " +
      "FSHLog --split hdfs://example.com:9000/hbase/.logs/DIR");
  }

  /**
   * Pass one or more log file names and it will either dump out a text version
   * on <code>stdout</code> or split the specified log files.
   *
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      usage();
      System.exit(-1);
    }
    // either dump using the WALPrettyPrinter or split, depending on args
    if (args[0].compareTo("--dump") == 0) {
      WALPrettyPrinter.run(Arrays.copyOfRange(args, 1, args.length));
    } else if (args[0].compareTo("--perf") == 0) {
      LOG.fatal("Please use the WALPerformanceEvaluation tool instead. i.e.:");
      LOG.fatal("\thbase org.apache.hadoop.hbase.wal.WALPerformanceEvaluation --iterations " +
          args[1]);
      System.exit(-1);
    } else if (args[0].compareTo("--split") == 0) {
      Configuration conf = HBaseConfiguration.create();
      for (int i = 1; i < args.length; i++) {
        try {
          Path logPath = new Path(args[i]);
          FSUtils.setFsDefault(conf, logPath);
          split(conf, logPath);
        } catch (IOException t) {
          t.printStackTrace(System.err);
          System.exit(-1);
        }
      }
    } else {
      usage();
      System.exit(-1);
    }
  }
  
  /**
   * Find the 'getPipeline' on the passed <code>os</code> stream.
   * @return Method or null.
   */
  private Method getGetPipeline(final FSDataOutputStream os) {
    Method m = null;
    if (os != null) {
      Class<? extends OutputStream> wrappedStreamClass = os.getWrappedStream()
          .getClass();
      try {
        m = wrappedStreamClass.getDeclaredMethod("getPipeline",
          new Class<?>[] {});
        m.setAccessible(true);
      } catch (NoSuchMethodException e) {
        LOG.info("FileSystem's output stream doesn't support"
            + " getPipeline; not available; fsOut="
            + wrappedStreamClass.getName());
      } catch (SecurityException e) {
        LOG.info(
          "Doesn't have access to getPipeline on "
              + "FileSystems's output stream ; fsOut="
              + wrappedStreamClass.getName(), e);
        m = null; // could happen on setAccessible()
      }
    }
    return m;
  }

  /**
   * This method gets the pipeline for the current WAL.
   */
  @VisibleForTesting
  DatanodeInfo[] getPipeLine() {
    if (this.getPipeLine != null && this.hdfs_out != null) {
      Object repl;
      try {
        repl = this.getPipeLine.invoke(getOutputStream(), NO_ARGS);
        if (repl instanceof DatanodeInfo[]) {
          return ((DatanodeInfo[]) repl);
        }
      } catch (Exception e) {
        LOG.info("Get pipeline failed", e);
      }
    }
    return new DatanodeInfo[0];
  }
}
