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
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.HDFSBlocksDistribution.HostAndWeight;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.client.ClientSideRegionScanner;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Scan.ReadType;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotHelper;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MapReduceProtos.TableSnapshotRegionSplit;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;

/**
 * Hadoop MR API-agnostic implementation for mapreduce over table snapshots.
 */
@InterfaceAudience.Private
public class TableSnapshotInputFormatImpl {
  // TODO: Snapshots files are owned in fs by the hbase user. There is no
  // easy way to delegate access.

  public static final Logger LOG = LoggerFactory.getLogger(TableSnapshotInputFormatImpl.class);

  private static final String SNAPSHOT_NAME_KEY = "hbase.TableSnapshotInputFormat.snapshot.name";
  // key for specifying the root dir of the restored snapshot
  protected static final String RESTORE_DIR_KEY = "hbase.TableSnapshotInputFormat.restore.dir";

  /** See {@link #getBestLocations(Configuration, HDFSBlocksDistribution, int)} */
  private static final String LOCALITY_CUTOFF_MULTIPLIER =
    "hbase.tablesnapshotinputformat.locality.cutoff.multiplier";
  private static final float DEFAULT_LOCALITY_CUTOFF_MULTIPLIER = 0.8f;

  /**
   * For MapReduce jobs running multiple mappers per region, determines
   * what split algorithm we should be using to find split points for scanners.
   */
  public static final String SPLIT_ALGO = "hbase.mapreduce.split.algorithm";
  /**
   * For MapReduce jobs running multiple mappers per region, determines
   * number of splits to generate per region.
   */
  public static final String NUM_SPLITS_PER_REGION = "hbase.mapreduce.splits.per.region";

  /**
   * Whether to calculate the block location for splits. Default to true.
   * If the computing layer runs outside of HBase cluster, the block locality does not master.
   * Setting this value to false could skip the calculation and save some time.
   *
   * Set access modifier to "public" so that these could be accessed by test classes of
   * both org.apache.hadoop.hbase.mapred
   * and  org.apache.hadoop.hbase.mapreduce.
   */
  public static final String  SNAPSHOT_INPUTFORMAT_LOCALITY_ENABLED_KEY =
      "hbase.TableSnapshotInputFormat.locality.enabled";
  public static final boolean SNAPSHOT_INPUTFORMAT_LOCALITY_ENABLED_DEFAULT = true;

  /**
   * Whether to calculate the Snapshot region location by region location from meta.
   * It is much faster than computing block locations for splits.
   */
  public static final String  SNAPSHOT_INPUTFORMAT_LOCALITY_BY_REGION_LOCATION =
    "hbase.TableSnapshotInputFormat.locality.by.region.location";

  public static final boolean SNAPSHOT_INPUTFORMAT_LOCALITY_BY_REGION_LOCATION_DEFAULT = false;

  /**
   * In some scenario, scan limited rows on each InputSplit for sampling data extraction
   */
  public static final String SNAPSHOT_INPUTFORMAT_ROW_LIMIT_PER_INPUTSPLIT =
      "hbase.TableSnapshotInputFormat.row.limit.per.inputsplit";

  /**
   * Whether to enable scan metrics on Scan, default to true
   */
  public static final String  SNAPSHOT_INPUTFORMAT_SCAN_METRICS_ENABLED =
    "hbase.TableSnapshotInputFormat.scan_metrics.enabled";

  public static final boolean SNAPSHOT_INPUTFORMAT_SCAN_METRICS_ENABLED_DEFAULT = true;

  /**
   * The {@link ReadType} which should be set on the {@link Scan} to read the HBase Snapshot,
   * default STREAM.
   */
  public static final String SNAPSHOT_INPUTFORMAT_SCANNER_READTYPE =
      "hbase.TableSnapshotInputFormat.scanner.readtype";
  public static final ReadType SNAPSHOT_INPUTFORMAT_SCANNER_READTYPE_DEFAULT = ReadType.STREAM;

  /**
   * Implementation class for InputSplit logic common between mapred and mapreduce.
   */
  public static class InputSplit implements Writable {

    private TableDescriptor htd;
    private HRegionInfo regionInfo;
    private String[] locations;
    private String scan;
    private String restoreDir;

    // constructor for mapreduce framework / Writable
    public InputSplit() {}

    public InputSplit(TableDescriptor htd, HRegionInfo regionInfo, List<String> locations,
        Scan scan, Path restoreDir) {
      this.htd = htd;
      this.regionInfo = regionInfo;
      if (locations == null || locations.isEmpty()) {
        this.locations = new String[0];
      } else {
        this.locations = locations.toArray(new String[locations.size()]);
      }
      try {
        this.scan = scan != null ? TableMapReduceUtil.convertScanToString(scan) : "";
      } catch (IOException e) {
        LOG.warn("Failed to convert Scan to String", e);
      }

      this.restoreDir = restoreDir.toString();
    }

    public TableDescriptor getHtd() {
      return htd;
    }

    public String getScan() {
      return scan;
    }

    public String getRestoreDir() {
      return restoreDir;
    }

    public long getLength() {
      //TODO: We can obtain the file sizes of the snapshot here.
      return 0;
    }

    public String[] getLocations() {
      return locations;
    }

    public TableDescriptor getTableDescriptor() {
      return htd;
    }

    public HRegionInfo getRegionInfo() {
      return regionInfo;
    }

    // TODO: We should have ProtobufSerialization in Hadoop, and directly use PB objects instead of
    // doing this wrapping with Writables.
    @Override
    public void write(DataOutput out) throws IOException {
      TableSnapshotRegionSplit.Builder builder = TableSnapshotRegionSplit.newBuilder()
          .setTable(ProtobufUtil.toTableSchema(htd))
          .setRegion(HRegionInfo.convert(regionInfo));

      for (String location : locations) {
        builder.addLocations(location);
      }

      TableSnapshotRegionSplit split = builder.build();

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      split.writeTo(baos);
      baos.close();
      byte[] buf = baos.toByteArray();
      out.writeInt(buf.length);
      out.write(buf);

      Bytes.writeByteArray(out, Bytes.toBytes(scan));
      Bytes.writeByteArray(out, Bytes.toBytes(restoreDir));

    }

    @Override
    public void readFields(DataInput in) throws IOException {
      int len = in.readInt();
      byte[] buf = new byte[len];
      in.readFully(buf);
      TableSnapshotRegionSplit split = TableSnapshotRegionSplit.PARSER.parseFrom(buf);
      this.htd = ProtobufUtil.toTableDescriptor(split.getTable());
      this.regionInfo = HRegionInfo.convert(split.getRegion());
      List<String> locationsList = split.getLocationsList();
      this.locations = locationsList.toArray(new String[locationsList.size()]);

      this.scan = Bytes.toString(Bytes.readByteArray(in));
      this.restoreDir = Bytes.toString(Bytes.readByteArray(in));
    }
  }

  /**
   * Implementation class for RecordReader logic common between mapred and mapreduce.
   */
  public static class RecordReader {
    private InputSplit split;
    private Scan scan;
    private Result result = null;
    private ImmutableBytesWritable row = null;
    private ClientSideRegionScanner scanner;
    private int numOfCompleteRows = 0;
    private int rowLimitPerSplit;

    public ClientSideRegionScanner getScanner() {
      return scanner;
    }

    public void initialize(InputSplit split, Configuration conf) throws IOException {
      this.scan = TableMapReduceUtil.convertStringToScan(split.getScan());
      this.split = split;
      this.rowLimitPerSplit = conf.getInt(SNAPSHOT_INPUTFORMAT_ROW_LIMIT_PER_INPUTSPLIT, 0);
      TableDescriptor htd = split.htd;
      HRegionInfo hri = this.split.getRegionInfo();
      FileSystem fs = CommonFSUtils.getCurrentFileSystem(conf);


      // region is immutable, this should be fine,
      // otherwise we have to set the thread read point
      scan.setIsolationLevel(IsolationLevel.READ_UNCOMMITTED);
      // disable caching of data blocks
      scan.setCacheBlocks(false);

      scanner =
          new ClientSideRegionScanner(conf, fs, new Path(split.restoreDir), htd, hri, scan, null);
    }

    public boolean nextKeyValue() throws IOException {
      result = scanner.next();
      if (result == null) {
        //we are done
        return false;
      }

      if (rowLimitPerSplit > 0 && ++this.numOfCompleteRows > rowLimitPerSplit) {
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

    Path rootDir = CommonFSUtils.getRootDir(conf);
    FileSystem fs = rootDir.getFileSystem(conf);

    SnapshotManifest manifest = getSnapshotManifest(conf, snapshotName, rootDir, fs);

    List<HRegionInfo> regionInfos = getRegionInfosFromManifest(manifest);

    // TODO: mapred does not support scan as input API. Work around for now.
    Scan scan = extractScanFromConf(conf);
    // the temp dir where the snapshot is restored
    Path restoreDir = new Path(conf.get(RESTORE_DIR_KEY));

    RegionSplitter.SplitAlgorithm splitAlgo = getSplitAlgo(conf);

    int numSplits = conf.getInt(NUM_SPLITS_PER_REGION, 1);

    return getSplits(scan, manifest, regionInfos, restoreDir, conf, splitAlgo, numSplits);
  }

  public static RegionSplitter.SplitAlgorithm getSplitAlgo(Configuration conf) throws IOException {
    String splitAlgoClassName = conf.get(SPLIT_ALGO);
    if (splitAlgoClassName == null) {
      return null;
    }
    try {
      return Class.forName(splitAlgoClassName).asSubclass(RegionSplitter.SplitAlgorithm.class)
          .getDeclaredConstructor().newInstance();
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException |
        NoSuchMethodException | InvocationTargetException e) {
      throw new IOException("SplitAlgo class " + splitAlgoClassName + " is not found", e);
    }
  }


  public static List<HRegionInfo> getRegionInfosFromManifest(SnapshotManifest manifest) {
    List<SnapshotRegionManifest> regionManifests = manifest.getRegionManifests();
    if (regionManifests == null) {
      throw new IllegalArgumentException("Snapshot seems empty");
    }

    List<HRegionInfo> regionInfos = Lists.newArrayListWithCapacity(regionManifests.size());

    for (SnapshotRegionManifest regionManifest : regionManifests) {
      HRegionInfo hri = HRegionInfo.convert(regionManifest.getRegionInfo());
      if (hri.isOffline() && (hri.isSplit() || hri.isSplitParent())) {
        continue;
      }
      regionInfos.add(hri);
    }
    return regionInfos;
  }

  public static SnapshotManifest getSnapshotManifest(Configuration conf, String snapshotName,
      Path rootDir, FileSystem fs) throws IOException {
    Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);
    SnapshotDescription snapshotDesc = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);
    return SnapshotManifest.open(conf, fs, snapshotDir, snapshotDesc);
  }

  public static Scan extractScanFromConf(Configuration conf) throws IOException {
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

    if (scan.getReadType() == ReadType.DEFAULT) {
      LOG.info("Provided Scan has DEFAULT ReadType,"
          + " updating STREAM for Snapshot-based InputFormat");
      // Update the "DEFAULT" ReadType to be "STREAM" to try to improve the default case.
      scan.setReadType(conf.getEnum(SNAPSHOT_INPUTFORMAT_SCANNER_READTYPE,
          SNAPSHOT_INPUTFORMAT_SCANNER_READTYPE_DEFAULT));
    }

    return scan;
  }

  public static List<InputSplit> getSplits(Scan scan, SnapshotManifest manifest,
      List<HRegionInfo> regionManifests, Path restoreDir, Configuration conf) throws IOException {
    return getSplits(scan, manifest, regionManifests, restoreDir, conf, null, 1);
  }

  public static List<InputSplit> getSplits(Scan scan, SnapshotManifest manifest,
      List<HRegionInfo> regionManifests, Path restoreDir,
      Configuration conf, RegionSplitter.SplitAlgorithm sa, int numSplits) throws IOException {
    // load table descriptor
    TableDescriptor htd = manifest.getTableDescriptor();

    Path tableDir = CommonFSUtils.getTableDir(restoreDir, htd.getTableName());

    boolean localityEnabled = conf.getBoolean(SNAPSHOT_INPUTFORMAT_LOCALITY_ENABLED_KEY,
                                              SNAPSHOT_INPUTFORMAT_LOCALITY_ENABLED_DEFAULT);

    boolean scanMetricsEnabled = conf.getBoolean(SNAPSHOT_INPUTFORMAT_SCAN_METRICS_ENABLED,
      SNAPSHOT_INPUTFORMAT_SCAN_METRICS_ENABLED_DEFAULT);
    scan.setScanMetricsEnabled(scanMetricsEnabled);

    boolean useRegionLoc = conf.getBoolean(SNAPSHOT_INPUTFORMAT_LOCALITY_BY_REGION_LOCATION,
      SNAPSHOT_INPUTFORMAT_LOCALITY_BY_REGION_LOCATION_DEFAULT);

    Connection connection = null;
    RegionLocator regionLocator = null;
    if (localityEnabled && useRegionLoc) {
      Configuration newConf = new Configuration(conf);
      newConf.setInt("hbase.hconnection.threads.max", 1);
      try {
        connection = ConnectionFactory.createConnection(newConf);
        regionLocator = connection.getRegionLocator(htd.getTableName());

        /* Get all locations for the table and cache it */
        regionLocator.getAllRegionLocations();
      } finally {
        if (connection != null) {
          connection.close();
        }
      }
    }

    List<InputSplit> splits = new ArrayList<>();
    for (HRegionInfo hri : regionManifests) {
      // load region descriptor
      List<String> hosts = null;
      if (localityEnabled) {
        if (regionLocator != null) {
          /* Get Location from the local cache */
          HRegionLocation
            location = regionLocator.getRegionLocation(hri.getStartKey(), false);

          hosts = new ArrayList<>(1);
          hosts.add(location.getHostname());
        } else {
          hosts = calculateLocationsForInputSplit(conf, htd, hri, tableDir);
        }
      }

      if (numSplits > 1) {
        byte[][] sp = sa.split(hri.getStartKey(), hri.getEndKey(), numSplits, true);
        for (int i = 0; i < sp.length - 1; i++) {
          if (PrivateCellUtil.overlappingKeys(scan.getStartRow(), scan.getStopRow(), sp[i],
                  sp[i + 1])) {

            Scan boundedScan = new Scan(scan);
            if (scan.getStartRow().length == 0) {
              boundedScan.withStartRow(sp[i]);
            } else {
              boundedScan.withStartRow(
                Bytes.compareTo(scan.getStartRow(), sp[i]) > 0 ? scan.getStartRow() : sp[i]);
            }

            if (scan.getStopRow().length == 0) {
              boundedScan.withStopRow(sp[i + 1]);
            } else {
              boundedScan.withStopRow(
                Bytes.compareTo(scan.getStopRow(), sp[i + 1]) < 0 ? scan.getStopRow() : sp[i + 1]);
            }

            splits.add(new InputSplit(htd, hri, hosts, boundedScan, restoreDir));
          }
        }
      } else {
        if (PrivateCellUtil.overlappingKeys(scan.getStartRow(), scan.getStopRow(),
            hri.getStartKey(), hri.getEndKey())) {

          splits.add(new InputSplit(htd, hri, hosts, scan, restoreDir));
        }
      }
    }

    return splits;
  }

  /**
   * Compute block locations for snapshot files (which will get the locations for referred hfiles)
   * only when localityEnabled is true.
   */
  private static List<String> calculateLocationsForInputSplit(Configuration conf,
      TableDescriptor htd, HRegionInfo hri, Path tableDir)
      throws IOException {
    return getBestLocations(conf, HRegion.computeHDFSBlocksDistribution(conf, htd, hri, tableDir));
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
   * Return at most numTopsAtMost locations if there are more than that.
   */
  private static List<String> getBestLocations(Configuration conf,
      HDFSBlocksDistribution blockDistribution, int numTopsAtMost) {
    HostAndWeight[] hostAndWeights = blockDistribution.getTopHostsWithWeights();

    if (hostAndWeights.length == 0) { // no matter what numTopsAtMost is
      return null;
    }

    if (numTopsAtMost < 1) { // invalid if numTopsAtMost < 1, correct it to be 1
      numTopsAtMost = 1;
    }
    int top = Math.min(numTopsAtMost, hostAndWeights.length);
    List<String> locations = new ArrayList<>(top);
    HostAndWeight topHost = hostAndWeights[0];
    locations.add(topHost.getHost());

    if (top == 1) { // only care about the top host
      return locations;
    }

    // When top >= 2,
    // do the heuristic: filter all hosts which have at least cutoffMultiplier % of block locality
    double cutoffMultiplier
            = conf.getFloat(LOCALITY_CUTOFF_MULTIPLIER, DEFAULT_LOCALITY_CUTOFF_MULTIPLIER);

    double filterWeight = topHost.getWeight() * cutoffMultiplier;

    for (int i = 1; i <= top - 1; i++) {
      if (hostAndWeights[i].getWeight() >= filterWeight) {
        locations.add(hostAndWeights[i].getHost());
      } else {
        // As hostAndWeights is in descending order,
        // we could break the loop as long as we meet a weight which is less than filterWeight.
        break;
      }
    }

    return locations;
  }

  public static List<String> getBestLocations(Configuration conf,
      HDFSBlocksDistribution blockDistribution) {
    // 3 nodes will contain highly local blocks. So default to 3.
    return getBestLocations(conf, blockDistribution, 3);
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
   * @param conf the job to configuration
   * @param snapshotName the name of the snapshot to read from
   * @param restoreDir a temporary directory to restore the snapshot into. Current user should have
   *          write permissions to this directory, and this should not be a subdirectory of rootdir.
   *          After the job is finished, restoreDir can be deleted.
   * @throws IOException if an error occurs
   */
  public static void setInput(Configuration conf, String snapshotName, Path restoreDir)
      throws IOException {
    setInput(conf, snapshotName, restoreDir, null, 1);
  }

  /**
   * Configures the job to use TableSnapshotInputFormat to read from a snapshot.
   * @param conf the job to configure
   * @param snapshotName the name of the snapshot to read from
   * @param restoreDir a temporary directory to restore the snapshot into. Current user should have
   *          write permissions to this directory, and this should not be a subdirectory of rootdir.
   *          After the job is finished, restoreDir can be deleted.
   * @param numSplitsPerRegion how many input splits to generate per one region
   * @param splitAlgo SplitAlgorithm to be used when generating InputSplits
   * @throws IOException if an error occurs
   */
  public static void setInput(Configuration conf, String snapshotName, Path restoreDir,
                              RegionSplitter.SplitAlgorithm splitAlgo, int numSplitsPerRegion)
          throws IOException {
    conf.set(SNAPSHOT_NAME_KEY, snapshotName);
    if (numSplitsPerRegion < 1) {
      throw new IllegalArgumentException("numSplits must be >= 1, " +
              "illegal numSplits : " + numSplitsPerRegion);
    }
    if (splitAlgo == null && numSplitsPerRegion > 1) {
      throw new IllegalArgumentException("Split algo can't be null when numSplits > 1");
    }
    if (splitAlgo != null) {
      conf.set(SPLIT_ALGO, splitAlgo.getClass().getName());
    }
    conf.setInt(NUM_SPLITS_PER_REGION, numSplitsPerRegion);
    Path rootDir = CommonFSUtils.getRootDir(conf);
    FileSystem fs = rootDir.getFileSystem(conf);

    restoreDir = new Path(restoreDir, UUID.randomUUID().toString());

    RestoreSnapshotHelper.copySnapshotForScanner(conf, fs, rootDir, restoreDir, snapshotName);
    conf.set(RESTORE_DIR_KEY, restoreDir.toString());
  }

  /**
   *  clean restore directory after snapshot scan job
   * @param job the snapshot scan job
   * @param snapshotName the name of the snapshot to read from
   * @throws IOException if an error occurs
   */
  public static void cleanRestoreDir(Job job, String snapshotName) throws IOException {
    Configuration conf = job.getConfiguration();
    Path restoreDir = new Path(conf.get(RESTORE_DIR_KEY));
    FileSystem fs = restoreDir.getFileSystem(conf);
    if (!fs.exists(restoreDir)) {
      LOG.warn("{} doesn't exist on file system, maybe it's already been cleaned", restoreDir);
      return;
    }
    if (!fs.delete(restoreDir, true)) {
      LOG.warn("Failed clean restore dir {} for snapshot {}", restoreDir, snapshotName);
    }
    LOG.debug("Clean restore directory {} for {}", restoreDir,  snapshotName);
  }
}
