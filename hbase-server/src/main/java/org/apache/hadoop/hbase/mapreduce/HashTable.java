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

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.Ordering;

public class HashTable extends Configured implements Tool {

  private static final Log LOG = LogFactory.getLog(HashTable.class);
  
  private static final int DEFAULT_BATCH_SIZE = 8000;
  
  private final static String HASH_BATCH_SIZE_CONF_KEY = "hash.batch.size";
  final static String PARTITIONS_FILE_NAME = "partitions";
  final static String MANIFEST_FILE_NAME = "manifest";
  final static String HASH_DATA_DIR = "hashes";
  final static String OUTPUT_DATA_FILE_PREFIX = "part-r-";
  private final static String TMP_MANIFEST_FILE_NAME = "manifest.tmp";
  
  TableHash tableHash = new TableHash();
  Path destPath;
  
  public HashTable(Configuration conf) {
    super(conf);
  }
  
  public static class TableHash {
    
    Path hashDir;
    
    String tableName;
    String families = null;
    long batchSize = DEFAULT_BATCH_SIZE;
    int numHashFiles = 0;
    byte[] startRow = HConstants.EMPTY_START_ROW;
    byte[] stopRow = HConstants.EMPTY_END_ROW;
    int scanBatch = 0;
    int versions = -1;
    long startTime = 0;
    long endTime = 0;
    
    List<ImmutableBytesWritable> partitions;
    
    public static TableHash read(Configuration conf, Path hashDir) throws IOException {
      TableHash tableHash = new TableHash();
      FileSystem fs = hashDir.getFileSystem(conf);
      tableHash.hashDir = hashDir;
      tableHash.readPropertiesFile(fs, new Path(hashDir, MANIFEST_FILE_NAME));
      tableHash.readPartitionFile(fs, conf, new Path(hashDir, PARTITIONS_FILE_NAME));
      return tableHash;
    }
    
    void writePropertiesFile(FileSystem fs, Path path) throws IOException {
      Properties p = new Properties();
      p.setProperty("table", tableName);
      if (families != null) {
        p.setProperty("columnFamilies", families);
      }
      p.setProperty("targetBatchSize", Long.toString(batchSize));
      p.setProperty("numHashFiles", Integer.toString(numHashFiles));
      if (!isTableStartRow(startRow)) {
        p.setProperty("startRowHex", Bytes.toHex(startRow));
      }
      if (!isTableEndRow(stopRow)) {
        p.setProperty("stopRowHex", Bytes.toHex(stopRow));
      }
      if (scanBatch > 0) {
        p.setProperty("scanBatch", Integer.toString(scanBatch));
      }
      if (versions >= 0) {
        p.setProperty("versions", Integer.toString(versions));
      }
      if (startTime != 0) {
        p.setProperty("startTimestamp", Long.toString(startTime));
      }
      if (endTime != 0) {
        p.setProperty("endTimestamp", Long.toString(endTime));
      }
      
      FSDataOutputStream out = fs.create(path);
      p.store(new OutputStreamWriter(out, Charsets.UTF_8), null);
      out.close();
    }
    
    void readPropertiesFile(FileSystem fs, Path path) throws IOException {
      FSDataInputStream in = fs.open(path);
      Properties p = new Properties();
      p.load(new InputStreamReader(in, Charsets.UTF_8));
      in.close();
      
      tableName = p.getProperty("table");
      families = p.getProperty("columnFamilies");
      batchSize = Long.parseLong(p.getProperty("targetBatchSize"));
      numHashFiles = Integer.parseInt(p.getProperty("numHashFiles"));
      
      String startRowHex = p.getProperty("startRowHex");
      if (startRowHex != null) {
        startRow = Bytes.fromHex(startRowHex);
      }
      String stopRowHex = p.getProperty("stopRowHex");
      if (stopRowHex != null) {
        stopRow = Bytes.fromHex(stopRowHex);
      }
      
      String scanBatchString = p.getProperty("scanBatch");
      if (scanBatchString != null) {
        scanBatch = Integer.parseInt(scanBatchString);
      }
      
      String versionString = p.getProperty("versions");
      if (versionString != null) {
        versions = Integer.parseInt(versionString);
      }
      
      String startTimeString = p.getProperty("startTimestamp");
      if (startTimeString != null) {
        startTime = Long.parseLong(startTimeString);
      }
      
      String endTimeString = p.getProperty("endTimestamp");
      if (endTimeString != null) {
        endTime = Long.parseLong(endTimeString);
      }
    }
    
    Scan initScan() throws IOException {
      Scan scan = new Scan();
      scan.setCacheBlocks(false);
      if (startTime != 0 || endTime != 0) {
        scan.setTimeRange(startTime, endTime == 0 ? HConstants.LATEST_TIMESTAMP : endTime);
      }
      if (scanBatch > 0) {
        scan.setBatch(scanBatch);
      }
      if (versions >= 0) {
        scan.setMaxVersions(versions);
      }
      if (!isTableStartRow(startRow)) {
        scan.setStartRow(startRow);
      }
      if (!isTableEndRow(stopRow)) {
        scan.setStopRow(stopRow);
      }
      if(families != null) {
        for(String fam : families.split(",")) {
          scan.addFamily(Bytes.toBytes(fam));
        }
      }
      return scan;
    }
    
    /**
     * Choose partitions between row ranges to hash to a single output file
     * Selects region boundaries that fall within the scan range, and groups them
     * into the desired number of partitions.
     */
    void selectPartitions(Pair<byte[][], byte[][]> regionStartEndKeys) {
      List<byte[]> startKeys = new ArrayList<byte[]>();
      for (int i = 0; i < regionStartEndKeys.getFirst().length; i++) {
        byte[] regionStartKey = regionStartEndKeys.getFirst()[i];
        byte[] regionEndKey = regionStartEndKeys.getSecond()[i];
        
        // if scan begins after this region, or starts before this region, then drop this region
        // in other words:
        //   IF (scan begins before the end of this region
        //      AND scan ends before the start of this region)
        //   THEN include this region
        if ((isTableStartRow(startRow) || isTableEndRow(regionEndKey)
            || Bytes.compareTo(startRow, regionEndKey) < 0)
          && (isTableEndRow(stopRow) || isTableStartRow(regionStartKey)
            || Bytes.compareTo(stopRow, regionStartKey) > 0)) {
          startKeys.add(regionStartKey);
        }
      }
      
      int numRegions = startKeys.size();
      if (numHashFiles == 0) {
        numHashFiles = numRegions / 100;
      }
      if (numHashFiles == 0) {
        numHashFiles = 1;
      }
      if (numHashFiles > numRegions) {
        // can't partition within regions
        numHashFiles = numRegions;
      }
      
      // choose a subset of start keys to group regions into ranges
      partitions = new ArrayList<ImmutableBytesWritable>(numHashFiles - 1);
      // skip the first start key as it is not a partition between ranges.
      for (long i = 1; i < numHashFiles; i++) {
        int splitIndex = (int) (numRegions * i / numHashFiles);
        partitions.add(new ImmutableBytesWritable(startKeys.get(splitIndex)));
      }
    }
    
    void writePartitionFile(Configuration conf, Path path) throws IOException {
      FileSystem fs = path.getFileSystem(conf);
      @SuppressWarnings("deprecation")
      SequenceFile.Writer writer = SequenceFile.createWriter(
        fs, conf, path, ImmutableBytesWritable.class, NullWritable.class);
      
      for (int i = 0; i < partitions.size(); i++) {
        writer.append(partitions.get(i), NullWritable.get());
      }
      writer.close();
    }
    
    private void readPartitionFile(FileSystem fs, Configuration conf, Path path)
         throws IOException {
      @SuppressWarnings("deprecation")
      SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
      ImmutableBytesWritable key = new ImmutableBytesWritable();
      partitions = new ArrayList<ImmutableBytesWritable>();
      while (reader.next(key)) {
        partitions.add(new ImmutableBytesWritable(key.copyBytes()));
      }
      reader.close();
      
      if (!Ordering.natural().isOrdered(partitions)) {
        throw new IOException("Partitions are not ordered!");
      }
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("tableName=").append(tableName);
      if (families != null) {
        sb.append(", families=").append(families);
      }
      sb.append(", batchSize=").append(batchSize);
      sb.append(", numHashFiles=").append(numHashFiles);
      if (!isTableStartRow(startRow)) {
        sb.append(", startRowHex=").append(Bytes.toHex(startRow));
      }
      if (!isTableEndRow(stopRow)) {
        sb.append(", stopRowHex=").append(Bytes.toHex(stopRow));
      }
      if (scanBatch >= 0) {
        sb.append(", scanBatch=").append(scanBatch);
      }
      if (versions >= 0) {
        sb.append(", versions=").append(versions);
      }
      if (startTime != 0) {
        sb.append("startTime=").append(startTime);
      }
      if (endTime != 0) {
        sb.append("endTime=").append(endTime);
      }
      return sb.toString();
    }
    
    static String getDataFileName(int hashFileIndex) {
      return String.format(HashTable.OUTPUT_DATA_FILE_PREFIX + "%05d", hashFileIndex);
    }
    
    /**
     * Open a TableHash.Reader starting at the first hash at or after the given key.
     * @throws IOException 
     */
    public Reader newReader(Configuration conf, ImmutableBytesWritable startKey)
        throws IOException {
      return new Reader(conf, startKey);
    }
    
    public class Reader implements java.io.Closeable {
      private final Configuration conf;
      
      private int hashFileIndex;
      private MapFile.Reader mapFileReader;
      
      private boolean cachedNext;
      private ImmutableBytesWritable key;
      private ImmutableBytesWritable hash;
      
      Reader(Configuration conf, ImmutableBytesWritable startKey) throws IOException {
        this.conf = conf;
        int partitionIndex = Collections.binarySearch(partitions, startKey);
        if (partitionIndex >= 0) {
          // if the key is equal to a partition, then go the file after that partition
          hashFileIndex = partitionIndex+1;
        } else {
          // if the key is between partitions, then go to the file between those partitions
          hashFileIndex = -1-partitionIndex;
        }
        openHashFile();
        
        // MapFile's don't make it easy to seek() so that the subsequent next() returns
        // the desired key/value pair.  So we cache it for the first call of next().
        hash = new ImmutableBytesWritable();
        key = (ImmutableBytesWritable) mapFileReader.getClosest(startKey, hash);
        if (key == null) {
          cachedNext = false;
          hash = null;
        } else {
          cachedNext = true;
        }
      }
      
      /**
       * Read the next key/hash pair.
       * Returns true if such a pair exists and false when at the end of the data.
       */
      public boolean next() throws IOException {
        if (cachedNext) {
          cachedNext = false;
          return true;
        }
        key = new ImmutableBytesWritable();
        hash = new ImmutableBytesWritable();
        while (true) {
          boolean hasNext = mapFileReader.next(key, hash);
          if (hasNext) {
            return true;
          }
          hashFileIndex++;
          if (hashFileIndex < TableHash.this.numHashFiles) {
            mapFileReader.close();
            openHashFile();
          } else {
            key = null;
            hash = null;
            return false;
          }
        }
      }
      
      /**
       * Get the current key
       * @return the current key or null if there is no current key
       */
      public ImmutableBytesWritable getCurrentKey() {
        return key;
      }
      
      /**
       * Get the current hash
       * @return the current hash or null if there is no current hash
       */
      public ImmutableBytesWritable getCurrentHash() {
        return hash;
      }
      
      private void openHashFile() throws IOException {
        if (mapFileReader != null) {
          mapFileReader.close();
        }
        Path dataDir = new Path(TableHash.this.hashDir, HASH_DATA_DIR);
        Path dataFile = new Path(dataDir, getDataFileName(hashFileIndex));
        mapFileReader = new MapFile.Reader(dataFile, conf);
      }

      @Override
      public void close() throws IOException {
        mapFileReader.close();
      }
    }
  }
  
  static boolean isTableStartRow(byte[] row) {
    return Bytes.equals(HConstants.EMPTY_START_ROW, row);
  }
  
  static boolean isTableEndRow(byte[] row) {
    return Bytes.equals(HConstants.EMPTY_END_ROW, row);
  }
  
  public Job createSubmittableJob(String[] args) throws IOException {
    Path partitionsPath = new Path(destPath, PARTITIONS_FILE_NAME);
    generatePartitions(partitionsPath);
    
    Job job = Job.getInstance(getConf(),
          getConf().get("mapreduce.job.name", "hashTable_" + tableHash.tableName));
    Configuration jobConf = job.getConfiguration();
    jobConf.setLong(HASH_BATCH_SIZE_CONF_KEY, tableHash.batchSize);
    job.setJarByClass(HashTable.class);

    TableMapReduceUtil.initTableMapperJob(tableHash.tableName, tableHash.initScan(),
        HashMapper.class, ImmutableBytesWritable.class, ImmutableBytesWritable.class, job);
    
    // use a TotalOrderPartitioner and reducers to group region output into hash files
    job.setPartitionerClass(TotalOrderPartitioner.class);
    TotalOrderPartitioner.setPartitionFile(jobConf, partitionsPath);
    job.setReducerClass(Reducer.class);  // identity reducer
    job.setNumReduceTasks(tableHash.numHashFiles);
    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(ImmutableBytesWritable.class);
    job.setOutputFormatClass(MapFileOutputFormat.class);
    FileOutputFormat.setOutputPath(job, new Path(destPath, HASH_DATA_DIR));
    
    return job;
  }
  
  private void generatePartitions(Path partitionsPath) throws IOException {
    Pair<byte[][], byte[][]> regionKeys;
    HConnection connection = HConnectionManager.createConnection(getConf());
    try {
      HTable table = (HTable)connection.getTable(TableName.valueOf(tableHash.tableName));
      try {
        regionKeys = table.getStartEndKeys();
      } finally {
        table.close();
      }
    } finally {
      connection.close();
    }
    
    tableHash.selectPartitions(regionKeys);
    LOG.info("Writing " + tableHash.partitions.size() + " partition keys to " + partitionsPath);
    
    tableHash.writePartitionFile(getConf(), partitionsPath);
  }
  
  static class ResultHasher {
    private MessageDigest digest;
    
    private boolean batchStarted = false;
    private ImmutableBytesWritable batchStartKey;
    private ImmutableBytesWritable batchHash;
    private long batchSize = 0;
    
    
    public ResultHasher() {
      try {
        digest = MessageDigest.getInstance("MD5");
      } catch (NoSuchAlgorithmException e) {
        Throwables.propagate(e);
      }
    }
    
    public void startBatch(ImmutableBytesWritable row) {
      if (batchStarted) {
        throw new RuntimeException("Cannot start new batch without finishing existing one.");
      }
      batchStarted = true;
      batchSize = 0;
      batchStartKey = row;
      batchHash = null;
    }
    
    public void hashResult(Result result) {
      if (!batchStarted) {
        throw new RuntimeException("Cannot add to batch that has not been started.");
      }
      for (Cell cell : result.rawCells()) {
        int rowLength = cell.getRowLength();
        int familyLength = cell.getFamilyLength();
        int qualifierLength = cell.getQualifierLength();
        int valueLength = cell.getValueLength();
        digest.update(cell.getRowArray(), cell.getRowOffset(), rowLength);
        digest.update(cell.getFamilyArray(), cell.getFamilyOffset(), familyLength);
        digest.update(cell.getQualifierArray(), cell.getQualifierOffset(), qualifierLength);
        long ts = cell.getTimestamp();
        for (int i = 8; i > 0; i--) {
          digest.update((byte) ts);
          ts >>>= 8;
        }
        digest.update(cell.getValueArray(), cell.getValueOffset(), valueLength);
        
        batchSize += rowLength + familyLength + qualifierLength + 8 + valueLength;
      }
    }
    
    public void finishBatch() {
      if (!batchStarted) {
        throw new RuntimeException("Cannot finish batch that has not started.");
      }
      batchStarted = false;
      batchHash = new ImmutableBytesWritable(digest.digest());
    }

    public boolean isBatchStarted() {
      return batchStarted;
    }

    public ImmutableBytesWritable getBatchStartKey() {
      return batchStartKey;
    }

    public ImmutableBytesWritable getBatchHash() {
      return batchHash;
    }

    public long getBatchSize() {
      return batchSize;
    }
  }
  
  public static class HashMapper
    extends TableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {
    
    private ResultHasher hasher;
    private long targetBatchSize;
    
    private ImmutableBytesWritable currentRow;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      targetBatchSize = context.getConfiguration()
          .getLong(HASH_BATCH_SIZE_CONF_KEY, DEFAULT_BATCH_SIZE);
      hasher = new ResultHasher();
      
      TableSplit split = (TableSplit) context.getInputSplit();
      hasher.startBatch(new ImmutableBytesWritable(split.getStartRow()));
    }
    
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context)
        throws IOException, InterruptedException {
      
      if (currentRow == null || !currentRow.equals(key)) {
        currentRow = new ImmutableBytesWritable(key); // not immutable
        
        if (hasher.getBatchSize() >= targetBatchSize) {
          hasher.finishBatch();
          context.write(hasher.getBatchStartKey(), hasher.getBatchHash());
          hasher.startBatch(currentRow);
        }
      }
      
      hasher.hashResult(value);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      hasher.finishBatch();
      context.write(hasher.getBatchStartKey(), hasher.getBatchHash());
    }
  }
  
  private void writeTempManifestFile() throws IOException {
    Path tempManifestPath = new Path(destPath, TMP_MANIFEST_FILE_NAME);
    FileSystem fs = tempManifestPath.getFileSystem(getConf());
    tableHash.writePropertiesFile(fs, tempManifestPath);
  }
  
  private void completeManifest() throws IOException {
    Path tempManifestPath = new Path(destPath, TMP_MANIFEST_FILE_NAME);
    Path manifestPath = new Path(destPath, MANIFEST_FILE_NAME);
    FileSystem fs = tempManifestPath.getFileSystem(getConf());
    fs.rename(tempManifestPath, manifestPath);
  }
  
  private static final int NUM_ARGS = 2;
  private static void printUsage(final String errorMsg) {
    if (errorMsg != null && errorMsg.length() > 0) {
      System.err.println("ERROR: " + errorMsg);
      System.err.println();
    }
    System.err.println("Usage: HashTable [options] <tablename> <outputpath>");
    System.err.println();
    System.err.println("Options:");
    System.err.println(" batchsize     the target amount of bytes to hash in each batch");
    System.err.println("               rows are added to the batch until this size is reached");
    System.err.println("               (defaults to " + DEFAULT_BATCH_SIZE + " bytes)");
    System.err.println(" numhashfiles  the number of hash files to create");
    System.err.println("               if set to fewer than number of regions then");
    System.err.println("               the job will create this number of reducers");
    System.err.println("               (defaults to 1/100 of regions -- at least 1)");
    System.err.println(" startrow      the start row");
    System.err.println(" stoprow       the stop row");
    System.err.println(" starttime     beginning of the time range (unixtime in millis)");
    System.err.println("               without endtime means from starttime to forever");
    System.err.println(" endtime       end of the time range.  Ignored if no starttime specified.");
    System.err.println(" scanbatch     scanner batch size to support intra row scans");
    System.err.println(" versions      number of cell versions to include");
    System.err.println(" families      comma-separated list of families to include");
    System.err.println();
    System.err.println("Args:");
    System.err.println(" tablename     Name of the table to hash");
    System.err.println(" outputpath    Filesystem path to put the output data");
    System.err.println();
    System.err.println("Examples:");
    System.err.println(" To hash 'TestTable' in 32kB batches for a 1 hour window into 50 files:");
    System.err.println(" $ bin/hbase " +
        "org.apache.hadoop.hbase.mapreduce.HashTable --batchsize=32000 --numhashfiles=50"
        + " --starttime=1265875194289 --endtime=1265878794289 --families=cf2,cf3"
        + " TestTable /hashes/testTable");
  }

  private boolean doCommandLine(final String[] args) {
    if (args.length < NUM_ARGS) {
      printUsage(null);
      return false;
    }
    try {
      
      tableHash.tableName = args[args.length-2];
      destPath = new Path(args[args.length-1]);
      
      for (int i = 0; i < args.length - NUM_ARGS; i++) {
        String cmd = args[i];
        if (cmd.equals("-h") || cmd.startsWith("--h")) {
          printUsage(null);
          return false;
        }
        
        final String batchSizeArgKey = "--batchsize=";
        if (cmd.startsWith(batchSizeArgKey)) {
          tableHash.batchSize = Long.parseLong(cmd.substring(batchSizeArgKey.length()));
          continue;
        }
        
        final String numHashFilesArgKey = "--numhashfiles=";
        if (cmd.startsWith(numHashFilesArgKey)) {
          tableHash.numHashFiles = Integer.parseInt(cmd.substring(numHashFilesArgKey.length()));
          continue;
        }
         
        final String startRowArgKey = "--startrow=";
        if (cmd.startsWith(startRowArgKey)) {
          tableHash.startRow = Bytes.fromHex(cmd.substring(startRowArgKey.length()));
          continue;
        }
        
        final String stopRowArgKey = "--stoprow=";
        if (cmd.startsWith(stopRowArgKey)) {
          tableHash.stopRow = Bytes.fromHex(cmd.substring(stopRowArgKey.length()));
          continue;
        }
        
        final String startTimeArgKey = "--starttime=";
        if (cmd.startsWith(startTimeArgKey)) {
          tableHash.startTime = Long.parseLong(cmd.substring(startTimeArgKey.length()));
          continue;
        }

        final String endTimeArgKey = "--endtime=";
        if (cmd.startsWith(endTimeArgKey)) {
          tableHash.endTime = Long.parseLong(cmd.substring(endTimeArgKey.length()));
          continue;
        }

        final String scanBatchArgKey = "--scanbatch=";
        if (cmd.startsWith(scanBatchArgKey)) {
          tableHash.scanBatch = Integer.parseInt(cmd.substring(scanBatchArgKey.length()));
          continue;
        }

        final String versionsArgKey = "--versions=";
        if (cmd.startsWith(versionsArgKey)) {
          tableHash.versions = Integer.parseInt(cmd.substring(versionsArgKey.length()));
          continue;
        }

        final String familiesArgKey = "--families=";
        if (cmd.startsWith(familiesArgKey)) {
          tableHash.families = cmd.substring(familiesArgKey.length());
          continue;
        }

        printUsage("Invalid argument '" + cmd + "'");
        return false;
      }
      if ((tableHash.startTime != 0 || tableHash.endTime != 0)
          && (tableHash.startTime >= tableHash.endTime)) {
        printUsage("Invalid time range filter: starttime="
            + tableHash.startTime + " >=  endtime=" + tableHash.endTime);
        return false;
      }
      
    } catch (Exception e) {
      e.printStackTrace();
      printUsage("Can't start because " + e.getMessage());
      return false;
    }
    return true;
  }

  /**
   * Main entry point.
   */
  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new HashTable(HBaseConfiguration.create()), args);
    System.exit(ret);
  }

  @Override
  public int run(String[] args) throws Exception {
    String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();
    if (!doCommandLine(otherArgs)) {
      return 1;
    }

    Job job = createSubmittableJob(otherArgs);
    writeTempManifestFile();
    if (!job.waitForCompletion(true)) {
      LOG.info("Map-reduce job failed!");
      return 1;
    }
    completeManifest();
    return 0;
  }

}
