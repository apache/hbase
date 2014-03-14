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

package org.apache.hadoop.hbase.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.HDFSBlocksDistribution.HostAndWeight;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotHelper;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotReferenceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingLong;
import org.apache.hadoop.util.StringUtils;

import com.google.common.annotations.VisibleForTesting;

/**
 * TableSnapshotInputFormat allows a MapReduce job to run over a table snapshot. The
 * job bypasses HBase servers, and directly accesses the underlying files
 * (hfile, recovered edits, hlogs, etc) directly to provide maximum performance.
 * The snapshot is not required to be restored or cloned. This also allows to
 * run the mapreduce job from an online or offline hbase cluster. The snapshot
 * files can be exported by using the ExportSnapshot tool, to a pure-hdfs
 * cluster, and this InputFormat can be used to run the mapreduce job directly
 * over the snapshot files.
 * <p>
 * Usage is similar to TableInputFormat.
 * {@link TableMapReduceUtil#initTableSnapshotMapperJob(String, Scan, Class, Class, Class, Job,
 * boolean, Path)} can be used to configure the job.
 * 
 * <pre>
 * {
 *   &#064;code
 *   Job job = new Job(conf);
 *   Scan scan = new Scan();
 *   TableMapReduceUtil.initSnapshotMapperJob(snapshotName, scan,
 *       MyTableMapper.class, MyMapKeyOutput.class,
 *       MyMapOutputValueWritable.class, job, true, tmpDir);
 * }
 * </pre>
 * <p>
 * Internally, this input format restores the snapshot into the given tmp
 * directory. Similar to {@link TableInputFormat} an {@link InputSplit} is created per region.
 * The region is opened for reading from each RecordReader. An internal
 * RegionScanner is used to execute the Scan obtained from the user.
 * <p>
 * <p>
 * HBase owns all the data and snapshot files on the filesystem. Only the HBase user can read from
 * snapshot files and data files. HBase also enforces security because all the requests are handled
 * by the server layer, and the user cannot read from the data files directly. 
 * To read from snapshot files directly from the file system, the user who is running the MR job 
 * must have sufficient permissions to access snapshot and reference files. 
 * This means that to run mapreduce over snapshot files, the MR job has to be run as the HBase 
 * user or the user must have group or other priviledges in the filesystem (See HBASE-8369). 
 * Note that, given other users access to read from snapshot/data files will completely circumvent 
 * the access control enforced by HBase.
 */
public final class TableSnapshotInputFormat extends
    InputFormat<ImmutableBytesWritable, Result> {
  // TODO: Snapshots files are owned in fs by the hbase user. There is no
  // easy way to delegate access.

  private static final String SNAPSHOT_NAME_KEY = "hbase.mr.snapshot.input.name";
  private static final String TABLE_DIR_KEY = "hbase.mr.snapshot.input.table.dir";

  /** See {@link #getBestLocations(Configuration, HDFSBlocksDistribution)} */
  private static final String LOCALITY_CUTOFF_MULTIPLIER = 
      "hbase.tablesnapshotinputformat.locality.cutoff.multiplier";
  private static final float DEFAULT_LOCALITY_CUTOFF_MULTIPLIER = 0.8f;

  /**
   * Snapshot region split.
   */
  public static final class TableSnapshotRegionSplit extends InputSplit implements
      Writable {
    private String regionName;
    private String[] locations;

    /**
     * Constructor for serialization.
     */
    public TableSnapshotRegionSplit() {
    }

    /**
     * Constructor.
     * 
     * @param regionName
     *          Region name
     * @param locationList
     *          List of nodes with the region's HDFS blocks, in descending order
     *          of weight
     */
    public TableSnapshotRegionSplit(final String regionName,
        final List<String> locationList) {
      this.regionName = regionName;

      // only use the top node
      List<String> list = locationList.size() > 1 ? locationList.subList(0, 1)
          : locationList;
      this.locations = list.toArray(new String[list.size()]);
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
      return locations.length;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
      return locations;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      regionName = Text.readString(in);
      int locLength = in.readInt();
      locations = new String[locLength];
      for (int i = 0; i < locLength; i++) {
        locations[i] = Text.readString(in);
      }
    }

    @Override
    public void write(DataOutput out) throws IOException {
      Text.writeString(out, regionName);
      out.writeInt(locations.length);
      for (String l : locations) {
        Text.writeString(out, l);
      }
    }
  }

  /**
   * Snapshot region record reader.
   */
  public static final class TableSnapshotRegionRecordReader extends
      RecordReader<ImmutableBytesWritable, Result> {
    static final Log LOG = LogFactory.getLog(TableSnapshotRegionRecordReader.class);

    // HBASE_COUNTER_GROUP_NAME is the name of mapreduce counter group for HBase
    private static final String HBASE_COUNTER_GROUP_NAME = "HBase Counters";

    private TableSnapshotRegionSplit split;
    private HRegion region;
    private Scan scan;
    private RegionScanner scanner;
    private List<KeyValue> values;
    private Result result = null;
    private ImmutableBytesWritable row = null;
    private boolean more;
    private ScanMetrics scanMetrics = null;
    private TaskAttemptContext context = null;
    private Method getCounter = null;

    @Override
    public void initialize(final InputSplit aSplit,
        final TaskAttemptContext context) throws IOException,
        InterruptedException {
      Configuration conf = context.getConfiguration();
      this.split = (TableSnapshotRegionSplit) aSplit;

      Path rootDir = new Path(conf.get(HConstants.HBASE_DIR));
      FileSystem fs = rootDir.getFileSystem(conf);

      String snapshotName = getSnapshotName(conf);
      Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(
          snapshotName, rootDir);

      // load region descriptor
      String regionName = this.split.regionName;
      Path regionDir = new Path(snapshotDir, regionName);
      HRegionInfo hri = HRegion.loadDotRegionInfoFileContent(fs, regionDir);

      // create scan
      scan = TableMapReduceUtil.convertStringToScan(conf.get(TableInputFormat.SCAN));
      // region is immutable, this should be fine, otherwise we have to set the
      // thread read point...
      scan.setIsolationLevel(IsolationLevel.READ_UNCOMMITTED);
      scan.setCacheBlocks(false);

      // load table descriptor
      HTableDescriptor htd = FSTableDescriptors.getTableDescriptor(fs,
          snapshotDir);
      Path tableDir = new Path(conf.get(TABLE_DIR_KEY));

      // open region from the snapshot directory
      this.region = openRegion(tableDir, fs, conf, hri, htd);

      // create region scanner
      this.scanner = region.getScanner(scan);
      values = new ArrayList<KeyValue>();
      this.more = true;
      this.scanMetrics = new ScanMetrics();

      if (context != null) {
        this.context = context;
        getCounter = retrieveGetCounterWithStringsParams(context);
      }
      region.startRegionOperation();
    }

    private HRegion openRegion(final Path tableDir, final FileSystem fs,
        final Configuration conf, final HRegionInfo hri,
        final HTableDescriptor htd) throws IOException {
      HRegion r = HRegion.newHRegion(tableDir, null, fs, conf, hri, htd, null);
      r.initialize(null);
      return r;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      values.clear();
      // RegionScanner.next() has a different contract than
      // RecordReader.nextKeyValue(). Scanner
      // indicates no value read by returning empty results. Returns boolean
      // indicates if more
      // rows exist AFTER this one
      if (!more) {
        updateCounters();
        return false;
      }
      more = scanner.nextRaw(values, scan.getBatch(), null);
      if (values.isEmpty()) {
        // we are done
        updateCounters();
        return false;
      }
      for (KeyValue kv : values) {
        this.scanMetrics.countOfBytesInResults.inc(kv.getLength());
      }
      this.result = new Result(values);
      if (this.row == null) {
        this.row = new ImmutableBytesWritable();
      }
      this.row.set(result.getRow());

      return true;
    }

    @Override
    public ImmutableBytesWritable getCurrentKey() throws IOException,
        InterruptedException {
      return row;
    }

    @Override
    public Result getCurrentValue() throws IOException, InterruptedException {
      return result;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return 0;
    }

    @Override
    public void close() throws IOException {
      try {
        if (this.scanner != null) {
          this.scanner.close();
        }
      } finally {
        if (region != null) {
          region.closeRegionOperation();
          region.close(true);
        }
      }
    }

    /**
     * If hbase runs on new version of mapreduce, RecordReader has access to
     * counters thus can update counters based on scanMetrics.
     * If hbase runs on old version of mapreduce, it won't be able to get
     * access to counters and TableRecorderReader can't update counter values.
     * @throws IOException
     */
    private void updateCounters() throws IOException {
      // we can get access to counters only if hbase uses new mapreduce APIs
      if (this.getCounter == null) {
        return;
      }

      MetricsTimeVaryingLong[] mlvs =
        scanMetrics.getMetricsTimeVaryingLongArray();

      try {
        for (MetricsTimeVaryingLong mlv : mlvs) {
          Counter ct = (Counter)this.getCounter.invoke(context,
            HBASE_COUNTER_GROUP_NAME, mlv.getName());
          ct.increment(mlv.getCurrentIntervalValue());
        }
      } catch (Exception e) {
        LOG.debug("can't update counter." + StringUtils.stringifyException(e));
      }
    }

   /**
     * In new mapreduce APIs, TaskAttemptContext has two getCounter methods
     * Check if getCounter(String, String) method is available.
     * @return The getCounter method or null if not available.
     * @throws IOException
     */
    private Method retrieveGetCounterWithStringsParams(TaskAttemptContext context)
    throws IOException {
      Method m = null;
      try {
        m = context.getClass().getMethod("getCounter",
          new Class [] {String.class, String.class});
      } catch (SecurityException e) {
        throw new IOException("Failed test for getCounter", e);
      } catch (NoSuchMethodException e) {
        // Ignore
      }
      return m;
    }

  }

  @Override
  public RecordReader<ImmutableBytesWritable, Result> createRecordReader(
      final InputSplit split, final TaskAttemptContext context)
      throws IOException {
    return new TableSnapshotRegionRecordReader();
  }

  @Override
  public List<InputSplit> getSplits(final JobContext job) throws IOException,
      InterruptedException {
    Configuration conf = job.getConfiguration();
    String snapshotName = getSnapshotName(job.getConfiguration());

    Path rootDir = new Path(conf.get(HConstants.HBASE_DIR));
    FileSystem fs = rootDir.getFileSystem(conf);

    Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(
        snapshotName, rootDir);

    Set<String> snapshotRegionNames = SnapshotReferenceUtil
        .getSnapshotRegionNames(fs, snapshotDir);
    if (snapshotRegionNames == null) {
      throw new IllegalArgumentException("Snapshot is empty");
    }

    // load table descriptor
    HTableDescriptor htd = FSTableDescriptors.getTableDescriptor(fs,
        snapshotDir);

    Scan scan = TableMapReduceUtil.convertStringToScan(conf.get(TableInputFormat.SCAN));
    Path tableDir = new Path(conf.get(TABLE_DIR_KEY));

    List<InputSplit> splits = new ArrayList<InputSplit>(
        snapshotRegionNames.size());
    for (String regionName : snapshotRegionNames) {
      // load region descriptor
      Path regionDir = new Path(snapshotDir, regionName);
      HRegionInfo hri = HRegion.loadDotRegionInfoFileContent(fs, regionDir);

      if (keyRangesOverlap(scan.getStartRow(), scan.getStopRow(),
          hri.getStartKey(), hri.getEndKey())) {
        // compute HDFS locations from snapshot files (which will get the locations for
        // referred hfiles)
        List<String> hosts = getBestLocations(conf,
          HRegion.computeHDFSBlocksDistribution(conf, htd, hri.getEncodedName(), tableDir));

        int len = Math.min(3, hosts.size());
        hosts = hosts.subList(0, len);
        splits.add(new TableSnapshotRegionSplit(regionName, hosts));
      }
    }

    return splits;
  }

  private boolean keyRangesOverlap(final byte[] start1, final byte[] end1,
      final byte[] start2, final byte[] end2) {
    return (end2.length == 0 || start1.length == 0 || Bytes.compareTo(start1,
        end2) < 0)
        && (end1.length == 0 || start2.length == 0 || Bytes.compareTo(start2,
            end1) < 0);
  }

  /**
   * This computes the locations to be passed from the InputSplit. MR/Yarn schedulers does not take
   * weights into account, thus will treat every location passed from the input split as equal. We
   * do not want to blindly pass all the locations, since we are creating one split per region, and
   * the region's blocks are all distributed throughout the cluster unless favorite node assignment
   * is used. On the expected stable case, only one location will contain most of the blocks as 
   * local.
   * On the other hand, in favored node assignment, 3 nodes will contain highly local blocks. Here
   * we are doing a simple heuristic, where we will pass all hosts which have at least 80%
   * (hbase.tablesnapshotinputformat.locality.cutoff.multiplier) as much block locality as the top
   * host with the best locality.
   */
  @VisibleForTesting
  List<String> getBestLocations(Configuration conf, HDFSBlocksDistribution blockDistribution) {
    List<String> locations = new ArrayList<String>(3);

    HostAndWeight[] hostAndWeights = blockDistribution.getTopHostsWithWeights();

    if (hostAndWeights.length == 0) {
      return locations;
    }

    HostAndWeight topHost = hostAndWeights[0];
    locations.add(topHost.getHost());

    // Heuristic: filter all hosts which have at least cutoffMultiplier % of block locality
    double cutoffMultiplier
      = conf.getFloat(LOCALITY_CUTOFF_MULTIPLIER, DEFAULT_LOCALITY_CUTOFF_MULTIPLIER);

    double filterWeight = topHost.getWeight() * cutoffMultiplier;

    for (int i = 1; i < hostAndWeights.length; i++) {
      if (hostAndWeights[i].getWeight() >= filterWeight) {
        locations.add(hostAndWeights[i].getHost());
      } else {
        break;
      }
    }

    return locations;
  }

  /**
   * Set job input.
   * 
   * @param job
   *          The job
   * @param snapshotName
   *          The snapshot name
   * @param restoreDir
   *          The directory where the temp table will be created
   * @throws IOException
   *           on error
   */
  public static void setInput(final Job job, final String snapshotName,
      final Path restoreDir) throws IOException {
    Configuration conf = job.getConfiguration();
    conf.set(SNAPSHOT_NAME_KEY, snapshotName);

    Path rootDir = new Path(conf.get(HConstants.HBASE_DIR));
    FileSystem fs = rootDir.getFileSystem(conf);

    Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(
        snapshotName, rootDir);
    SnapshotDescription snapshotDesc = SnapshotDescriptionUtils
        .readSnapshotInfo(fs, snapshotDir);

    // load table descriptor
    HTableDescriptor htd = FSTableDescriptors.getTableDescriptor(fs,
        snapshotDir);

    Path tableDir = new Path(restoreDir, htd.getNameAsString());
    conf.set(TABLE_DIR_KEY, tableDir.toString());

    MonitoredTask status = TaskMonitor.get().createStatus(
        "Restoring  snapshot '" + snapshotName + "' to directory " + tableDir);
    ForeignExceptionDispatcher monitor = new ForeignExceptionDispatcher();

    RestoreSnapshotHelper helper = new RestoreSnapshotHelper(conf, fs,
        snapshotDesc, snapshotDir, htd, tableDir, monitor, status);
    helper.restoreHdfsRegions();
  }

  private static String getSnapshotName(final Configuration conf) {
    String snapshotName = conf.get(SNAPSHOT_NAME_KEY);
    if (snapshotName == null) {
      throw new IllegalArgumentException("Snapshot name must be provided");
    }
    return snapshotName;
  }
}

