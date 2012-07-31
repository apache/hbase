/*
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
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileDataBlockEncoder;
import org.apache.hadoop.hbase.io.hfile.HFileDataBlockEncoderImpl;
import org.apache.hadoop.hbase.io.hfile.HFilePrettyPrinter;
import org.apache.hadoop.hbase.io.hfile.NoOpDataBlockEncoder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.LoadTestTool;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.apache.hadoop.util.StringUtils;

/**
 * Tests HFile read/write workloads, such as merging HFiles and random reads.
 */
public class HFileReadWriteTest {

  private static final String TABLE_NAME = "MyTable";

  private static enum Workload {
    MERGE("merge", "Merge the specified HFiles", 1, Integer.MAX_VALUE),
    RANDOM_READS("read", "Perform a random read benchmark on the given HFile",
        1, 1);

    private String option;
    private String description;

    public final int minNumInputFiles;
    public final int maxNumInputFiles;

    Workload(String option, String description, int minNumInputFiles,
        int maxNumInputFiles) {
      this.option = option;
      this.description = description;
      this.minNumInputFiles = minNumInputFiles;
      this.maxNumInputFiles = maxNumInputFiles;
    }

    static OptionGroup getOptionGroup() {
      OptionGroup optionGroup = new OptionGroup();
      for (Workload w : values())
        optionGroup.addOption(new Option(w.option, w.description));
      return optionGroup;
    }

    private static String getOptionListStr() {
      StringBuilder sb = new StringBuilder();
      for (Workload w : values()) {
        if (sb.length() > 0)
          sb.append(", ");
        sb.append("-" + w.option);
      }
      return sb.toString();
    }

    static Workload fromCmdLine(CommandLine cmdLine) {
      for (Workload w : values()) {
        if (cmdLine.hasOption(w.option))
          return w;
      }
      LOG.error("No workload specified. Specify one of the options: " +
          getOptionListStr());
      return null;
    }

    public String onlyUsedFor() {
      return ". Only used for the " + this + " workload.";
    }
  }

  private static final String OUTPUT_DIR_OPTION = "output_dir";
  private static final String COMPRESSION_OPTION = "compression";
  private static final String BLOOM_FILTER_OPTION = "bloom";
  private static final String BLOCK_SIZE_OPTION = "block_size";
  private static final String DURATION_OPTION = "duration";
  private static final String NUM_THREADS_OPTION = "num_threads";

  private static final Log LOG = LogFactory.getLog(HFileReadWriteTest.class);

  private Workload workload;
  private FileSystem fs;
  private Configuration conf;
  private CacheConfig cacheConf;
  private List<String> inputFileNames;
  private Path outputDir;
  private int numReadThreads;
  private int durationSec;
  private DataBlockEncoding dataBlockEncoding;
  private boolean encodeInCacheOnly;
  private HFileDataBlockEncoder dataBlockEncoder =
      NoOpDataBlockEncoder.INSTANCE;

  private StoreFile.BloomType bloomType = StoreFile.BloomType.NONE;
  private int blockSize;
  private Compression.Algorithm compression = Compression.Algorithm.NONE;

  private byte[] firstRow, lastRow;

  private AtomicLong numSeeks = new AtomicLong();
  private AtomicLong numKV = new AtomicLong();
  private AtomicLong totalBytes = new AtomicLong();

  private byte[] family;

  private long endTime = Long.MAX_VALUE;

  private SortedSet<String> keysRead = new ConcurrentSkipListSet<String>();
  private List<StoreFile> inputStoreFiles;

  public HFileReadWriteTest() {
    conf = HBaseConfiguration.create();
    cacheConf = new CacheConfig(conf);
  }

  @SuppressWarnings("unchecked")
  public boolean parseOptions(String args[]) {

    Options options = new Options();
    options.addOption(OUTPUT_DIR_OPTION, true, "Output directory" +
        Workload.MERGE.onlyUsedFor());
    options.addOption(COMPRESSION_OPTION, true, " Compression type, one of "
        + Arrays.toString(Compression.Algorithm.values()) +
        Workload.MERGE.onlyUsedFor());
    options.addOption(BLOOM_FILTER_OPTION, true, "Bloom filter type, one of "
        + Arrays.toString(StoreFile.BloomType.values()) +
        Workload.MERGE.onlyUsedFor());
    options.addOption(BLOCK_SIZE_OPTION, true, "HFile block size" +
        Workload.MERGE.onlyUsedFor());
    options.addOption(DURATION_OPTION, true, "The amount of time to run the " +
        "random read workload for" + Workload.RANDOM_READS.onlyUsedFor());
    options.addOption(NUM_THREADS_OPTION, true, "The number of random " +
        "reader threads" + Workload.RANDOM_READS.onlyUsedFor());
    options.addOption(NUM_THREADS_OPTION, true, "The number of random " +
        "reader threads" + Workload.RANDOM_READS.onlyUsedFor());
    options.addOption(LoadTestTool.OPT_DATA_BLOCK_ENCODING, true,
        LoadTestTool.OPT_DATA_BLOCK_ENCODING_USAGE);
    options.addOption(LoadTestTool.OPT_ENCODE_IN_CACHE_ONLY, false,
        LoadTestTool.OPT_ENCODE_IN_CACHE_ONLY_USAGE);
    options.addOptionGroup(Workload.getOptionGroup());

    if (args.length == 0) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(HFileReadWriteTest.class.getSimpleName(),
          options, true);
      return false;
    }

    CommandLineParser parser = new PosixParser();
    CommandLine cmdLine;
    try {
      cmdLine = parser.parse(options, args);
    } catch (ParseException ex) {
      LOG.error(ex);
      return false;
    }

    workload = Workload.fromCmdLine(cmdLine);
    if (workload == null)
      return false;

    inputFileNames = (List<String>) cmdLine.getArgList();

    if (inputFileNames.size() == 0) {
      LOG.error("No input file names specified");
      return false;
    }

    if (inputFileNames.size() < workload.minNumInputFiles) {
      LOG.error("Too few input files: at least " + workload.minNumInputFiles +
          " required");
      return false;
    }

    if (inputFileNames.size() > workload.maxNumInputFiles) {
      LOG.error("Too many input files: at most " + workload.minNumInputFiles +
          " allowed");
      return false;
    }

    if (cmdLine.hasOption(COMPRESSION_OPTION)) {
      compression = Compression.Algorithm.valueOf(
          cmdLine.getOptionValue(COMPRESSION_OPTION));
    }

    if (cmdLine.hasOption(BLOOM_FILTER_OPTION)) {
      bloomType = StoreFile.BloomType.valueOf(cmdLine.getOptionValue(
          BLOOM_FILTER_OPTION));
    }

    encodeInCacheOnly =
        cmdLine.hasOption(LoadTestTool.OPT_ENCODE_IN_CACHE_ONLY);

    if (cmdLine.hasOption(LoadTestTool.OPT_DATA_BLOCK_ENCODING)) {
      dataBlockEncoding = DataBlockEncoding.valueOf(
          cmdLine.getOptionValue(LoadTestTool.OPT_DATA_BLOCK_ENCODING));
      // Optionally encode on disk, always encode in cache.
      dataBlockEncoder = new HFileDataBlockEncoderImpl(
          encodeInCacheOnly ? DataBlockEncoding.NONE : dataBlockEncoding,
          dataBlockEncoding);
    } else {
      if (encodeInCacheOnly) {
        LOG.error("The -" + LoadTestTool.OPT_ENCODE_IN_CACHE_ONLY +
            " option does not make sense without -" +
            LoadTestTool.OPT_DATA_BLOCK_ENCODING);
        return false;
      }
    }

    blockSize = conf.getInt("hfile.min.blocksize.size", 65536);
    if (cmdLine.hasOption(BLOCK_SIZE_OPTION))
      blockSize = Integer.valueOf(cmdLine.getOptionValue(BLOCK_SIZE_OPTION));

    if (workload == Workload.MERGE) {
      String outputDirStr = cmdLine.getOptionValue(OUTPUT_DIR_OPTION);
      if (outputDirStr == null) {
        LOG.error("Output directory is not specified");
        return false;
      }
      outputDir = new Path(outputDirStr);
      // Will be checked for existence in validateConfiguration.
    }

    if (workload == Workload.RANDOM_READS) {
      if (!requireOptions(cmdLine, new String[] { DURATION_OPTION,
          NUM_THREADS_OPTION })) {
        return false;
      }

      durationSec = Integer.parseInt(cmdLine.getOptionValue(DURATION_OPTION));
      numReadThreads = Integer.parseInt(
          cmdLine.getOptionValue(NUM_THREADS_OPTION));
    }

    Collections.sort(inputFileNames);

    return true;
  }

  /** @return true if all the given options are specified */
  private boolean requireOptions(CommandLine cmdLine,
      String[] requiredOptions) {
    for (String option : requiredOptions)
      if (!cmdLine.hasOption(option)) {
        LOG.error("Required option -" + option + " not specified");
        return false;
      }
    return true;
  }

  public boolean validateConfiguration() throws IOException {
    fs = FileSystem.get(conf);

    for (String inputFileName : inputFileNames) {
      Path path = new Path(inputFileName);
      if (!fs.exists(path)) {
        LOG.error("File " + inputFileName + " does not exist");
        return false;
      }

      if (fs.getFileStatus(path).isDir()) {
        LOG.error(inputFileName + " is a directory");
        return false;
      }
    }

    if (outputDir != null &&
        (!fs.exists(outputDir) || !fs.getFileStatus(outputDir).isDir())) {
      LOG.error(outputDir.toString() + " does not exist or is not a " +
          "directory");
      return false;
    }

    return true;
  }

  public void runMergeWorkload() throws IOException {
    long maxKeyCount = prepareForMerge();

    List<StoreFileScanner> scanners =
        StoreFileScanner.getScannersForStoreFiles(inputStoreFiles, false,
            false);

    HColumnDescriptor columnDescriptor = new HColumnDescriptor(
        HFileReadWriteTest.class.getSimpleName());
    columnDescriptor.setBlocksize(blockSize);
    columnDescriptor.setBloomFilterType(bloomType);
    columnDescriptor.setCompressionType(compression);
    columnDescriptor.setDataBlockEncoding(dataBlockEncoding);
    HRegionInfo regionInfo = new HRegionInfo();
    HTableDescriptor htd = new HTableDescriptor(TABLE_NAME);
    HRegion region = new HRegion(outputDir, null, fs, conf, regionInfo, htd,
        null);
    Store store = new Store(outputDir, region, columnDescriptor, fs, conf);

    StoreFile.Writer writer = new StoreFile.WriterBuilder(conf,
        new CacheConfig(conf), fs, blockSize)
            .withOutputDir(outputDir)
            .withCompression(compression)
            .withDataBlockEncoder(dataBlockEncoder)
            .withBloomType(bloomType)
            .withMaxKeyCount(maxKeyCount)
            .withChecksumType(HFile.DEFAULT_CHECKSUM_TYPE)
            .withBytesPerChecksum(HFile.DEFAULT_BYTES_PER_CHECKSUM)
            .build();

    StatisticsPrinter statsPrinter = new StatisticsPrinter();
    statsPrinter.startThread();

    try {
      performMerge(scanners, store, writer);
      writer.close();
    } finally {
      statsPrinter.requestStop();
    }

    Path resultPath = writer.getPath();

    resultPath = tryUsingSimpleOutputPath(resultPath);

    long fileSize = fs.getFileStatus(resultPath).getLen();
    LOG.info("Created " + resultPath + ", size " + fileSize);

    System.out.println();
    System.out.println("HFile information for " + resultPath);
    System.out.println();

    HFilePrettyPrinter hfpp = new HFilePrettyPrinter();
    hfpp.run(new String[] { "-m", "-f", resultPath.toString() });
  }

  private Path tryUsingSimpleOutputPath(Path resultPath) throws IOException {
    if (inputFileNames.size() == 1) {
      // In case of only one input set output to be consistent with the
      // input name.

      Path inputPath = new Path(inputFileNames.get(0));
      Path betterOutputPath = new Path(outputDir,
          inputPath.getName());
      if (!fs.exists(betterOutputPath)) {
        fs.rename(resultPath, betterOutputPath);
        resultPath = betterOutputPath;
      }
    }
    return resultPath;
  }

  private void performMerge(List<StoreFileScanner> scanners, Store store,
      StoreFile.Writer writer) throws IOException {
    InternalScanner scanner = null;
    try {
      Scan scan = new Scan();

      // Include deletes
      scanner = new StoreScanner(store, store.scanInfo, scan, scanners,
          ScanType.MAJOR_COMPACT, Long.MIN_VALUE, Long.MIN_VALUE);

      ArrayList<KeyValue> kvs = new ArrayList<KeyValue>();

      while (scanner.next(kvs) || kvs.size() != 0) {
        numKV.addAndGet(kvs.size());
        for (KeyValue kv : kvs) {
          totalBytes.addAndGet(kv.getLength());
          writer.append(kv);
        }
        kvs.clear();
      }
    } finally {
      if (scanner != null)
        scanner.close();
    }
  }

  /**
   * @return the total key count in the files being merged
   * @throws IOException
   */
  private long prepareForMerge() throws IOException {
    LOG.info("Merging " + inputFileNames);
    LOG.info("Using block size: " + blockSize);
    inputStoreFiles = new ArrayList<StoreFile>();

    long maxKeyCount = 0;
    for (String fileName : inputFileNames) {
      Path filePath = new Path(fileName);

      // Open without caching.
      StoreFile sf = openStoreFile(filePath, false);
      sf.createReader();
      inputStoreFiles.add(sf);

      StoreFile.Reader r = sf.getReader();
      if (r != null) {
        long keyCount = r.getFilterEntries();
        maxKeyCount += keyCount;
        LOG.info("Compacting: " + sf + "; keyCount = " + keyCount
            + "; Bloom Type = " + r.getBloomFilterType().toString()
            + "; Size = " + StringUtils.humanReadableInt(r.length()));
      }
    }
    return maxKeyCount;
  }

  public HFile.Reader[] getHFileReaders() {
    HFile.Reader readers[] = new HFile.Reader[inputStoreFiles.size()];
    for (int i = 0; i < inputStoreFiles.size(); ++i)
      readers[i] = inputStoreFiles.get(i).getReader().getHFileReader();
    return readers;
  }

  private StoreFile openStoreFile(Path filePath, boolean blockCache)
      throws IOException {
    // We are passing the ROWCOL Bloom filter type, but StoreFile will still
    // use the Bloom filter type specified in the HFile.
    return new StoreFile(fs, filePath, conf, cacheConf,
        StoreFile.BloomType.ROWCOL, dataBlockEncoder);
  }

  public static int charToHex(int c) {
    if ('0' <= c && c <= '9')
      return c - '0';
    if ('a' <= c && c <= 'f')
      return 10 + c - 'a';
    return -1;
  }

  public static int hexToChar(int h) {
    h &= 0xff;
    if (0 <= h && h <= 9)
      return '0' + h;
    if (10 <= h && h <= 15)
      return 'a' + h - 10;
    return -1;
  }

  public static byte[] createRandomRow(Random rand, byte[] first, byte[] last)
  {
    int resultLen = Math.max(first.length, last.length);
    int minLen = Math.min(first.length, last.length);
    byte[] result = new byte[resultLen];
    boolean greaterThanFirst = false;
    boolean lessThanLast = false;

    for (int i = 0; i < resultLen; ++i) {
      // Generate random hex characters if both first and last row are hex
      // at this position.
      boolean isHex = i < minLen && charToHex(first[i]) != -1
          && charToHex(last[i]) != -1;

      // If our key is already greater than the first key, we can use
      // arbitrarily low values.
      int low = greaterThanFirst || i >= first.length ? 0 : first[i] & 0xff;

      // If our key is already less than the last key, we can use arbitrarily
      // high values.
      int high = lessThanLast || i >= last.length ? 0xff : last[i] & 0xff;

      // Randomly select the next byte between the lowest and the highest
      // value allowed for this position. Restrict to hex characters if
      // necessary. We are generally biased towards border cases, which is OK
      // for test.

      int r;
      if (isHex) {
        // Use hex chars.
        if (low < '0')
          low = '0';

        if (high > 'f')
          high = 'f';

        int lowHex = charToHex(low);
        int highHex = charToHex(high);
        r = hexToChar(lowHex + rand.nextInt(highHex - lowHex + 1));
      } else {
        r = low + rand.nextInt(high - low + 1);
      }

      if (r > low)
        greaterThanFirst = true;

      if (r < high)
        lessThanLast = true;

      result[i] = (byte) r;
    }

    if (Bytes.compareTo(result, first) < 0) {
      throw new IllegalStateException("Generated key " +
          Bytes.toStringBinary(result) + " is less than the first key " +
          Bytes.toStringBinary(first));
    }

    if (Bytes.compareTo(result, last) > 0) {
      throw new IllegalStateException("Generated key " +
          Bytes.toStringBinary(result) + " is greater than te last key " +
          Bytes.toStringBinary(last));
    }

    return result;
  }

  private static byte[] createRandomQualifier(Random rand) {
    byte[] q = new byte[10 + rand.nextInt(30)];
    rand.nextBytes(q);
    return q;
  }

  private class RandomReader implements Callable<Boolean> {

    private int readerId;
    private StoreFile.Reader reader;
    private boolean pread;

    public RandomReader(int readerId, StoreFile.Reader reader,
        boolean pread)
    {
      this.readerId = readerId;
      this.reader = reader;
      this.pread = pread;
    }

    @Override
    public Boolean call() throws Exception {
      Thread.currentThread().setName("reader " + readerId);
      Random rand = new Random();
      StoreFileScanner scanner = reader.getStoreFileScanner(true, pread);

      while (System.currentTimeMillis() < endTime) {
        byte[] row = createRandomRow(rand, firstRow, lastRow);
        KeyValue kvToSeek = new KeyValue(row, family,
            createRandomQualifier(rand));
        if (rand.nextDouble() < 0.0001) {
          LOG.info("kvToSeek=" + kvToSeek);
        }
        boolean seekResult;
        try {
          seekResult = scanner.seek(kvToSeek);
        } catch (IOException ex) {
          throw new IOException("Seek failed for key " + kvToSeek + ", pread="
              + pread, ex);
        }
        numSeeks.incrementAndGet();
        if (!seekResult) {
          error("Seek returned false for row " + Bytes.toStringBinary(row));
          return false;
        }
        for (int i = 0; i < rand.nextInt(10) + 1; ++i) {
          KeyValue kv = scanner.next();
          numKV.incrementAndGet();
          if (i == 0 && kv == null) {
            error("scanner.next() returned null at the first iteration for " +
                "row " + Bytes.toStringBinary(row));
            return false;
          }
          if (kv == null)
            break;

          String keyHashStr = MD5Hash.getMD5AsHex(kv.getKey());
          keysRead.add(keyHashStr);
          totalBytes.addAndGet(kv.getLength());
        }
      }

      return true;
    }

    private void error(String msg) {
      LOG.error("error in reader " + readerId + " (pread=" + pread + "): "
          + msg);
    }

  }

  private class StatisticsPrinter implements Callable<Boolean> {

    private volatile boolean stopRequested;
    private volatile Thread thread;
    private long totalSeekAndReads, totalPositionalReads;

    /**
     * Run the statistics collector in a separate thread without an executor.
     */
    public void startThread() {
      new Thread() {
        @Override
        public void run() {
          try {
            call();
          } catch (Exception e) {
            LOG.error(e);
          }
        }
      }.start();
    }

    @Override
    public Boolean call() throws Exception {
      LOG.info("Starting statistics printer");
      thread = Thread.currentThread();
      thread.setName(StatisticsPrinter.class.getSimpleName());
      long startTime = System.currentTimeMillis();
      long curTime;
      while ((curTime = System.currentTimeMillis()) < endTime &&
          !stopRequested) {
        long elapsedTime = curTime - startTime;
        printStats(elapsedTime);
        try {
          Thread.sleep(1000 - elapsedTime % 1000);
        } catch (InterruptedException iex) {
          Thread.currentThread().interrupt();
          if (stopRequested)
            break;
        }
      }
      printStats(curTime - startTime);
      LOG.info("Stopping statistics printer");
      return true;
    }

    private void printStats(long elapsedTime) {
      long numSeeksL = numSeeks.get();
      double timeSec = elapsedTime / 1000.0;
      double seekPerSec = numSeeksL / timeSec;
      long kvCount = numKV.get();
      double kvPerSec = kvCount / timeSec;
      long bytes = totalBytes.get();
      double bytesPerSec = bytes / timeSec;

      // readOps and preadOps counters get reset on access, so we have to
      // accumulate them here. HRegion metrics publishing thread should not
      // be running in this tool, so no one else should be resetting these
      // metrics.
      totalSeekAndReads += HFile.getReadOps();
      totalPositionalReads += HFile.getPreadOps();
      long totalBlocksRead = totalSeekAndReads + totalPositionalReads;

      double blkReadPerSec = totalBlocksRead / timeSec;

      double seekReadPerSec = totalSeekAndReads / timeSec;
      double preadPerSec = totalPositionalReads / timeSec;

      boolean isRead = workload == Workload.RANDOM_READS;

      StringBuilder sb = new StringBuilder();
      sb.append("Time: " +  (long) timeSec + " sec");
      if (isRead)
        sb.append(", seek/sec: " + (long) seekPerSec);
      sb.append(", kv/sec: " + (long) kvPerSec);
      sb.append(", bytes/sec: " + (long) bytesPerSec);
      sb.append(", blk/sec: " + (long) blkReadPerSec);
      sb.append(", total KV: " + numKV);
      sb.append(", total bytes: " + totalBytes);
      sb.append(", total blk: " + totalBlocksRead);

      sb.append(", seekRead/sec: " + (long) seekReadPerSec);
      sb.append(", pread/sec: " + (long) preadPerSec);

      if (isRead)
        sb.append(", unique keys: " + (long) keysRead.size());

      LOG.info(sb.toString());
    }

    public void requestStop() {
      stopRequested = true;
      if (thread != null)
        thread.interrupt();
    }

  }

  public boolean runRandomReadWorkload() throws IOException {
    if (inputFileNames.size() != 1) {
      throw new IOException("Need exactly one input file for random reads: " +
          inputFileNames);
    }

    Path inputPath = new Path(inputFileNames.get(0));

    // Make sure we are using caching.
    StoreFile storeFile = openStoreFile(inputPath, true);

    StoreFile.Reader reader = storeFile.createReader();

    LOG.info("First key: " + Bytes.toStringBinary(reader.getFirstKey()));
    LOG.info("Last key: " + Bytes.toStringBinary(reader.getLastKey()));

    KeyValue firstKV = KeyValue.createKeyValueFromKey(reader.getFirstKey());
    firstRow = firstKV.getRow();

    KeyValue lastKV = KeyValue.createKeyValueFromKey(reader.getLastKey());
    lastRow = lastKV.getRow();

    byte[] family = firstKV.getFamily();
    if (!Bytes.equals(family, lastKV.getFamily())) {
      LOG.error("First and last key have different families: "
          + Bytes.toStringBinary(family) + " and "
          + Bytes.toStringBinary(lastKV.getFamily()));
      return false;
    }

    if (Bytes.equals(firstRow, lastRow)) {
      LOG.error("First and last row are the same, cannot run read workload: " +
          "firstRow=" + Bytes.toStringBinary(firstRow) + ", " +
          "lastRow=" + Bytes.toStringBinary(lastRow));
      return false;
    }

    ExecutorService exec = Executors.newFixedThreadPool(numReadThreads + 1);
    int numCompleted = 0;
    int numFailed = 0;
    try {
      ExecutorCompletionService<Boolean> ecs =
          new ExecutorCompletionService<Boolean>(exec);
      endTime = System.currentTimeMillis() + 1000 * durationSec;
      boolean pread = true;
      for (int i = 0; i < numReadThreads; ++i)
        ecs.submit(new RandomReader(i, reader, pread));
      ecs.submit(new StatisticsPrinter());
      Future<Boolean> result;
      while (true) {
        try {
          result = ecs.poll(endTime + 1000 - System.currentTimeMillis(),
              TimeUnit.MILLISECONDS);
          if (result == null)
            break;
          try {
            if (result.get()) {
              ++numCompleted;
            } else {
              ++numFailed;
            }
          } catch (ExecutionException e) {
            LOG.error("Worker thread failure", e.getCause());
            ++numFailed;
          }
        } catch (InterruptedException ex) {
          LOG.error("Interrupted after " + numCompleted +
              " workers completed");
          Thread.currentThread().interrupt();
          continue;
        }

      }
    } finally {
      storeFile.closeReader(true);
      exec.shutdown();

      BlockCache c = cacheConf.getBlockCache();
      if (c != null) {
        c.shutdown();
      }
    }
    LOG.info("Worker threads completed: " + numCompleted);
    LOG.info("Worker threads failed: " + numFailed);
    return true;
  }

  public boolean run() throws IOException {
    LOG.info("Workload: " + workload);
    switch (workload) {
    case MERGE:
      runMergeWorkload();
      break;
    case RANDOM_READS:
      return runRandomReadWorkload();
    default:
      LOG.error("Unknown workload: " + workload);
      return false;
    }

    return true;
  }

  private static void failure() {
    System.exit(1);
  }

  public static void main(String[] args) {
    HFileReadWriteTest app = new HFileReadWriteTest();
    if (!app.parseOptions(args))
      failure();

    try {
      if (!app.validateConfiguration() ||
          !app.run())
        failure();
    } catch (IOException ex) {
      LOG.error(ex);
      failure();
    }
  }

}
