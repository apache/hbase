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

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.HDFSBlocksDistribution.HostAndWeight;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.ClientSideRegionScanner;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableSnapshotScanner;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;
import org.apache.hadoop.hbase.protobuf.generated.MapReduceProtos;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.snapshot.ExportSnapshot;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotHelper;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.annotations.VisibleForTesting;

/**
 * TableSnapshotInputFormat allows a MapReduce job to run over a table snapshot. The job
 * bypasses HBase servers, and directly accesses the underlying files (hfile, recovered edits,
 * hlogs, etc) directly to provide maximum performance. The snapshot is not required to be
 * restored to the live cluster or cloned. This also allows to run the mapreduce job from an
 * online or offline hbase cluster. The snapshot files can be exported by using the
 * {@link ExportSnapshot} tool, to a pure-hdfs cluster, and this InputFormat can be used to
 * run the mapreduce job directly over the snapshot files. The snapshot should not be deleted
 * while there are jobs reading from snapshot files.
 * <p>
 * Usage is similar to TableInputFormat, and
 * {@link TableMapReduceUtil#initTableSnapshotMapperJob(String, Scan, Class, Class, Class, Job,
 *   boolean, Path)}
 * can be used to configure the job.
 * <pre>{@code
 * Job job = new Job(conf);
 * Scan scan = new Scan();
 * TableMapReduceUtil.initTableSnapshotMapperJob(snapshotName,
 *      scan, MyTableMapper.class, MyMapKeyOutput.class,
 *      MyMapOutputValueWritable.class, job, true);
 * }
 * </pre>
 * <p>
 * Internally, this input format restores the snapshot into the given tmp directory. Similar to
 * {@link TableInputFormat} an InputSplit is created per region. The region is opened for reading
 * from each RecordReader. An internal RegionScanner is used to execute the {@link Scan} obtained
 * from the user.
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
 * @see TableSnapshotScanner
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class TableSnapshotInputFormat extends InputFormat<ImmutableBytesWritable, Result> {
  // TODO: Snapshots files are owned in fs by the hbase user. There is no
  // easy way to delegate access.

  private static final Log LOG = LogFactory.getLog(TableSnapshotInputFormat.class);

  /** See {@link #getBestLocations(Configuration, HDFSBlocksDistribution)} */
  private static final String LOCALITY_CUTOFF_MULTIPLIER =
      "hbase.tablesnapshotinputformat.locality.cutoff.multiplier";
  private static final float DEFAULT_LOCALITY_CUTOFF_MULTIPLIER = 0.8f;

  private static final String SNAPSHOT_NAME_KEY = "hbase.TableSnapshotInputFormat.snapshot.name";
  private static final String TABLE_DIR_KEY = "hbase.TableSnapshotInputFormat.table.dir";

  @VisibleForTesting
  static class TableSnapshotRegionSplit extends InputSplit implements Writable {
    private HTableDescriptor htd;
    private HRegionInfo regionInfo;
    private String[] locations;

    // constructor for mapreduce framework / Writable
    public TableSnapshotRegionSplit() { }

    TableSnapshotRegionSplit(HTableDescriptor htd, HRegionInfo regionInfo, List<String> locations) {
      this.htd = htd;
      this.regionInfo = regionInfo;
      if (locations == null || locations.isEmpty()) {
        this.locations = new String[0];
      } else {
        this.locations = locations.toArray(new String[locations.size()]);
      }
    }
    @Override
    public long getLength() throws IOException, InterruptedException {
      //TODO: We can obtain the file sizes of the snapshot here.
      return 0;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
      return locations;
    }

    // TODO: We should have ProtobufSerialization in Hadoop, and directly use PB objects instead of
    // doing this wrapping with Writables.
    @Override
    public void write(DataOutput out) throws IOException {
    MapReduceProtos.TableSnapshotRegionSplit.Builder builder =
      MapReduceProtos.TableSnapshotRegionSplit.newBuilder()
        .setTable(htd.convert())
        .setRegion(HRegionInfo.convert(regionInfo));

      for (String location : locations) {
        builder.addLocations(location);
      }

      MapReduceProtos.TableSnapshotRegionSplit split = builder.build();

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      split.writeTo(baos);
      baos.close();
      byte[] buf = baos.toByteArray();
      out.writeInt(buf.length);
      out.write(buf);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
      int len = in.readInt();
      byte[] buf = new byte[len];
      in.readFully(buf);
      MapReduceProtos.TableSnapshotRegionSplit split =
          MapReduceProtos.TableSnapshotRegionSplit.PARSER.parseFrom(buf);
      this.htd = HTableDescriptor.convert(split.getTable());
      this.regionInfo = HRegionInfo.convert(split.getRegion());
      List<String> locationsList = split.getLocationsList();
      this.locations = locationsList.toArray(new String[locationsList.size()]);
    }
  }

  @VisibleForTesting
  static class TableSnapshotRegionRecordReader extends
    RecordReader<ImmutableBytesWritable, Result> {
    private TableSnapshotRegionSplit split;
    private Scan scan;
    private Result result = null;
    private ImmutableBytesWritable row = null;
    private ClientSideRegionScanner scanner;
    private TaskAttemptContext context;
    private Method getCounter;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
        InterruptedException {

      Configuration conf = context.getConfiguration();
      this.split = (TableSnapshotRegionSplit) split;
      HTableDescriptor htd = this.split.htd;
      HRegionInfo hri = this.split.regionInfo;
      FileSystem fs = FSUtils.getCurrentFileSystem(conf);

      Path tmpRootDir = new Path(conf.get(TABLE_DIR_KEY)); // This is the user specified root
      // directory where snapshot was restored

      // create scan
      String scanStr = conf.get(TableInputFormat.SCAN);
      if (scanStr == null) {
        throw new IllegalArgumentException("A Scan is not configured for this job");
      }
      scan = TableMapReduceUtil.convertStringToScan(scanStr);
      // region is immutable, this should be fine,
      // otherwise we have to set the thread read point
      scan.setIsolationLevel(IsolationLevel.READ_UNCOMMITTED);
      // disable caching of data blocks
      scan.setCacheBlocks(false);

      scanner = new ClientSideRegionScanner(conf, fs, tmpRootDir, htd, hri, scan, null);
      this.context = context;
      getCounter = TableRecordReaderImpl.retrieveGetCounterWithStringsParams(context);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      result = scanner.next();
      if (result == null) {
        //we are done
        return false;
      }

      if (this.row == null) {
        this.row = new ImmutableBytesWritable();
      }
      this.row.set(result.getRow());

      ScanMetrics scanMetrics = scanner.getScanMetrics();
      if (scanMetrics != null && context != null) {
        TableRecordReaderImpl.updateCounters(scanMetrics, 0, getCounter, context);
      }

      return true;
    }

    @Override
    public ImmutableBytesWritable getCurrentKey() throws IOException, InterruptedException {
      return row;
    }

    @Override
    public Result getCurrentValue() throws IOException, InterruptedException {
      return result;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return 0; // TODO: use total bytes to estimate
    }

    @Override
    public void close() throws IOException {
      if (this.scanner != null) {
        this.scanner.close();
      }
    }
  }

  @Override
  public RecordReader<ImmutableBytesWritable, Result> createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException {
    return new TableSnapshotRegionRecordReader();
  }

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException, InterruptedException {
    Configuration conf = job.getConfiguration();
    String snapshotName = getSnapshotName(conf);

    Path rootDir = FSUtils.getRootDir(conf);
    FileSystem fs = rootDir.getFileSystem(conf);

    Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);
    SnapshotDescription snapshotDesc = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);
    SnapshotManifest manifest = SnapshotManifest.open(conf, fs, snapshotDir, snapshotDesc);
    List<SnapshotRegionManifest> regionManifests = manifest.getRegionManifests();
    if (regionManifests == null) {
      throw new IllegalArgumentException("Snapshot seems empty");
    }

    // load table descriptor
    HTableDescriptor htd = manifest.getTableDescriptor();

    Scan scan = TableMapReduceUtil.convertStringToScan(conf
      .get(TableInputFormat.SCAN));
    Path tableDir = new Path(conf.get(TABLE_DIR_KEY));

    List<InputSplit> splits = new ArrayList<InputSplit>();
    for (SnapshotRegionManifest regionManifest : regionManifests) {
      // load region descriptor
      HRegionInfo hri = HRegionInfo.convert(regionManifest.getRegionInfo());

      if (CellUtil.overlappingKeys(scan.getStartRow(), scan.getStopRow(),
          hri.getStartKey(), hri.getEndKey())) {
        // compute HDFS locations from snapshot files (which will get the locations for
        // referred hfiles)
        List<String> hosts = getBestLocations(conf,
          HRegion.computeHDFSBlocksDistribution(conf, htd, hri, tableDir));

        int len = Math.min(3, hosts.size());
        hosts = hosts.subList(0, len);
        splits.add(new TableSnapshotRegionSplit(htd, hri, hosts));
      }
    }

    return splits;
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
   * Configures the job to use TableSnapshotInputFormat to read from a snapshot.
   * @param job the job to configure
   * @param snapshotName the name of the snapshot to read from
   * @param restoreDir a temporary directory to restore the snapshot into. Current user should
   * have write permissions to this directory, and this should not be a subdirectory of rootdir.
   * After the job is finished, restoreDir can be deleted.
   * @throws IOException if an error occurs
   */
  public static void setInput(Job job, String snapshotName, Path restoreDir) throws IOException {
    Configuration conf = job.getConfiguration();
    conf.set(SNAPSHOT_NAME_KEY, snapshotName);

    Path rootDir = FSUtils.getRootDir(conf);
    FileSystem fs = rootDir.getFileSystem(conf);

    restoreDir = new Path(restoreDir, UUID.randomUUID().toString());

    // TODO: restore from record readers to parallelize.
    RestoreSnapshotHelper.copySnapshotForScanner(conf, fs, rootDir, restoreDir, snapshotName);

    conf.set(TABLE_DIR_KEY, restoreDir.toString());
  }

  private static String getSnapshotName(Configuration conf) {
    String snapshotName = conf.get(SNAPSHOT_NAME_KEY);
    if (snapshotName == null) {
      throw new IllegalArgumentException("Snapshot name must be provided");
    }
    return snapshotName;
  }
}
