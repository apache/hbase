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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.HDFSBlocksDistribution.HostAndWeight;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.ClientSideRegionScanner;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.MapReduceProtos;
import org.apache.hadoop.hbase.protobuf.generated.MapReduceProtos.TableSnapshotRegionSplit;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotHelper;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * API-agnostic implementation for mapreduce over table snapshots.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class TableSnapshotInputFormatImpl {
  // TODO: Snapshots files are owned in fs by the hbase user. There is no
  // easy way to delegate access.

  private static final String SNAPSHOT_NAME_KEY = "hbase.TableSnapshotInputFormat.snapshot.name";
  // key for specifying the root dir of the restored snapshot
  private static final String RESTORE_DIR_KEY = "hbase.TableSnapshotInputFormat.restore.dir";

  /** See {@link #getBestLocations(Configuration, HDFSBlocksDistribution)} */
  private static final String LOCALITY_CUTOFF_MULTIPLIER = "hbase.tablesnapshotinputformat.locality.cutoff.multiplier";
  private static final float DEFAULT_LOCALITY_CUTOFF_MULTIPLIER = 0.8f;

  /**
   * Implementation class for InputSplit logic common between mapred and mapreduce.
   */
  public static class InputSplit implements Writable {
    private HTableDescriptor htd;
    private HRegionInfo regionInfo;
    private String[] locations;

    // constructor for mapreduce framework / Writable
    public InputSplit() { }

    public InputSplit(HTableDescriptor htd, HRegionInfo regionInfo, List<String> locations) {
      this.htd = htd;
      this.regionInfo = regionInfo;
      if (locations == null || locations.isEmpty()) {
        this.locations = new String[0];
      } else {
        this.locations = locations.toArray(new String[locations.size()]);
      }
    }

    public long getLength() {
      //TODO: We can obtain the file sizes of the snapshot here.
      return 0;
    }

    public String[] getLocations() {
      return locations;
    }

    public HTableDescriptor getTableDescriptor() {
      return htd;
    }

    public HRegionInfo getRegionInfo() {
      return regionInfo;
    }

    // TODO: We should have ProtobufSerialization in Hadoop, and directly use PB objects instead of
    // doing this wrapping with Writables.
    @Override
    public void write(DataOutput out) throws IOException {
      MapReduceProtos.TableSnapshotRegionSplit.Builder builder = MapReduceProtos.TableSnapshotRegionSplit.newBuilder()
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
      TableSnapshotRegionSplit split = TableSnapshotRegionSplit.PARSER.parseFrom(buf);
      this.htd = HTableDescriptor.convert(split.getTable());
      this.regionInfo = HRegionInfo.convert(split.getRegion());
      List<String> locationsList = split.getLocationsList();
      this.locations = locationsList.toArray(new String[locationsList.size()]);
    }
  }

  /**
   * Implementation class for RecordReader logic common between mapred and mapreduce.
   */
  public static class RecordReader {
    InputSplit split;
    private Scan scan;
    private Result result = null;
    private ImmutableBytesWritable row = null;
    private ClientSideRegionScanner scanner;

    public ClientSideRegionScanner getScanner() {
      return scanner;
    }

    public void initialize(InputSplit split, Configuration conf) throws IOException {
      this.split = split;
      HTableDescriptor htd = split.htd;
      HRegionInfo hri = this.split.getRegionInfo();
      FileSystem fs = FSUtils.getCurrentFileSystem(conf);

      Path tmpRootDir = new Path(conf.get(RESTORE_DIR_KEY)); // This is the user specified root
      // directory where snapshot was restored

      // create scan
      // TODO: mapred does not support scan as input API. Work around for now.
      if (conf.get(TableInputFormat.SCAN) != null) {
        scan = TableMapReduceUtil.convertStringToScan(conf.get(TableInputFormat.SCAN));
      } else if (conf.get(org.apache.hadoop.hbase.mapred.TableInputFormat.COLUMN_LIST) != null) {
        String[] columns =
          conf.get(org.apache.hadoop.hbase.mapred.TableInputFormat.COLUMN_LIST).split(" ");
        scan = new Scan();
        for (String col : columns) {
          scan.addFamily(Bytes.toBytes(col));
        }
      } else {
        throw new IllegalArgumentException("A Scan is not configured for this job");
      }

      // region is immutable, this should be fine,
      // otherwise we have to set the thread read point
      scan.setIsolationLevel(IsolationLevel.READ_UNCOMMITTED);
      // disable caching of data blocks
      scan.setCacheBlocks(false);

      scanner = new ClientSideRegionScanner(conf, fs, tmpRootDir, htd, hri, scan, null);
    }

    public boolean nextKeyValue() throws IOException {
      result = scanner.next();
      if (result == null) {
        //we are done
        return false;
      }

      if (this.row == null) {
        this.row = new ImmutableBytesWritable();
      }
      this.row.set(result.getRow());
      return true;
    }

    public ImmutableBytesWritable getCurrentKey() {
      return row;
    }

    public Result getCurrentValue() {
      return result;
    }

    public long getPos() {
      return 0;
    }

    public float getProgress() {
      return 0; // TODO: use total bytes to estimate
    }

    public void close() {
      if (this.scanner != null) {
        this.scanner.close();
      }
    }
  }

  public static List<InputSplit> getSplits(Configuration conf) throws IOException {
    String snapshotName = getSnapshotName(conf);

    Path rootDir = new Path(conf.get(HConstants.HBASE_DIR));
    FileSystem fs = rootDir.getFileSystem(conf);

    Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);
    HBaseProtos.SnapshotDescription snapshotDesc =
      SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);
    SnapshotManifest manifest = SnapshotManifest.open(conf, fs, snapshotDir, snapshotDesc);

    List<SnapshotRegionManifest> regionManifests = manifest.getRegionManifests();

    if (regionManifests == null) {
	throw new IllegalArgumentException("Snapshot seems empty");
    }

    // load table descriptor
    HTableDescriptor htd = manifest.getTableDescriptor();

    // TODO: mapred does not support scan as input API. Work around for now.
    Scan scan = null;
    if (conf.get(TableInputFormat.SCAN) != null) {
      scan = TableMapReduceUtil.convertStringToScan(conf.get(TableInputFormat.SCAN));
    } else if (conf.get(org.apache.hadoop.hbase.mapred.TableInputFormat.COLUMN_LIST) != null) {
      String[] columns =
        conf.get(org.apache.hadoop.hbase.mapred.TableInputFormat.COLUMN_LIST).split(" ");
      scan = new Scan();
      for (String col : columns) {
        scan.addFamily(Bytes.toBytes(col));
      }
    } else {
      throw new IllegalArgumentException("Unable to create scan");
    }
    // the temp dir where the snapshot is restored
    Path restoreDir = new Path(conf.get(RESTORE_DIR_KEY));
    Path tableDir = FSUtils.getTableDir(restoreDir, htd.getTableName());

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
	splits.add(new InputSplit(htd, hri, hosts));
      }
    }

    return splits;
  }

  /**
   * This computes the locations to be passed from the InputSplit. MR/Yarn schedulers does not take
   * weights into account, thus will treat every location passed from the input split as equal. We
   * do not want to blindly pass all the locations, since we are creating one split per region, and
   * the region's blocks are all distributed throughout the cluster unless favorite node assignment
   * is used. On the expected stable case, only one location will contain most of the blocks as local.
   * On the other hand, in favored node assignment, 3 nodes will contain highly local blocks. Here
   * we are doing a simple heuristic, where we will pass all hosts which have at least 80%
   * (hbase.tablesnapshotinputformat.locality.cutoff.multiplier) as much block locality as the top
   * host with the best locality.
   */
  public static List<String> getBestLocations(
      Configuration conf, HDFSBlocksDistribution blockDistribution) {
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

  private static String getSnapshotName(Configuration conf) {
    String snapshotName = conf.get(SNAPSHOT_NAME_KEY);
    if (snapshotName == null) {
      throw new IllegalArgumentException("Snapshot name must be provided");
    }
    return snapshotName;
  }

  /**
   * Configures the job to use TableSnapshotInputFormat to read from a snapshot.
   * @param conf the job to configure
   * @param snapshotName the name of the snapshot to read from
   * @param restoreDir a temporary directory to restore the snapshot into. Current user should
   * have write permissions to this directory, and this should not be a subdirectory of rootdir.
   * After the job is finished, restoreDir can be deleted.
   * @throws IOException if an error occurs
   */
  public static void setInput(Configuration conf, String snapshotName, Path restoreDir)
      throws IOException {
    conf.set(SNAPSHOT_NAME_KEY, snapshotName);

    Path rootDir = new Path(conf.get(HConstants.HBASE_DIR));
    FileSystem fs = rootDir.getFileSystem(conf);

    restoreDir = new Path(restoreDir, UUID.randomUUID().toString());

    // TODO: restore from record readers to parallelize.
    RestoreSnapshotHelper.copySnapshotForScanner(conf, fs, rootDir, restoreDir, snapshotName);

    conf.set(RESTORE_DIR_KEY, restoreDir.toString());
  }
}
