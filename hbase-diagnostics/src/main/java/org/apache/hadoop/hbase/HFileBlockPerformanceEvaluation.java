/*
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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.BlockCompressedSizePredicator;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.EWMABlockSizePredicator;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileBlock;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.io.hfile.PreviousBlockCompressionRatePredicator;
import org.apache.hadoop.hbase.io.hfile.UncompressedBlockSizePredicator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performance evaluation utility for HFile block encoding, compression algorithms, and block size
 * predicators ({@link BlockCompressedSizePredicator} implementations).
 * <p>
 * Tests are parameterized by number of blocks rather than number of rows. A tunable value generator
 * produces cell values that compress to approximately the requested target ratio. The tool sweeps
 * all combinations of compression algorithm, data block encoding, and block size (powers of two
 * between min and max), and for predicator tests, also sweeps all three predicator implementations.
 * </p>
 * <h3>Usage</h3>
 *
 * <pre>
 * HFileBlockPerformanceEvaluation [options]
 *   --blocks N              Number of blocks per test (default: 100)
 *   --compressions LIST     Comma-separated compression algorithms (default: none,gz)
 *   --encodings LIST        Comma-separated data block encodings (default: none,fast_diff)
 *   --min-block-size BYTES  Minimum block size in bytes (default: 8192)
 *   --max-block-size BYTES  Maximum block size in bytes (default: 131072)
 *   --target-ratio FLOAT    Target compression ratio (default: 3.0)
 *   --value-size BYTES      Value size per cell in bytes (default: 1000)
 *   --predicators-only      Run only predicator accuracy tests
 *   --throughput-only       Run only read/write throughput tests
 * </pre>
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
public class HFileBlockPerformanceEvaluation {

  private static final Logger LOG = LoggerFactory.getLogger(HFileBlockPerformanceEvaluation.class);

  static {
    System.setProperty("org.apache.commons.logging.Log",
      "org.apache.commons.logging.impl.SimpleLog");
    System.setProperty(
      "org.apache.commons.logging.simplelog.log.org.apache.hadoop.hbase.io.compress.CodecPool",
      "WARN");
  }

  private static final int ROW_KEY_LENGTH = 16;

  private int numBlocks = 100;
  private int minBlockSize = 8 * 1024;
  private int maxBlockSize = 128 * 1024;
  private double targetRatio = 3.0;
  private int valueSize = 1000;
  private boolean predicatorsOnly = false;
  private boolean throughputOnly = false;

  private List<Compression.Algorithm> compressions = new ArrayList<>();
  private List<DataBlockEncoding> encodings = new ArrayList<>();

  // ---- Value generator ----

  /**
   * Generates byte arrays that compress to approximately the given target ratio. A fraction (1 -
   * 1/ratio) of the bytes are a repeating pattern and the rest are random. The repeating portion
   * compresses nearly to zero while the random portion is incompressible, so the overall ratio
   * approaches the target.
   */
  static class CompressibleValueGenerator {
    private final int valueSize;
    private final int randomBytes;
    private final byte patternByte;

    CompressibleValueGenerator(int valueSize, double targetRatio) {
      this.valueSize = valueSize;
      double incompressibleFraction = 1.0 / targetRatio;
      this.randomBytes = Math.max(1, (int) (valueSize * incompressibleFraction));
      this.patternByte = (byte) 0x42;
    }

    byte[] generate() {
      byte[] value = new byte[valueSize];
      Arrays.fill(value, patternByte);
      byte[] rand = new byte[randomBytes];
      Bytes.random(rand);
      System.arraycopy(rand, 0, value, 0, Math.min(randomBytes, valueSize));
      return value;
    }
  }

  // ---- Cell creation ----

  static byte[] formatRowKey(long i) {
    String v = Long.toString(i);
    StringBuilder sb = new StringBuilder(ROW_KEY_LENGTH);
    for (int pad = ROW_KEY_LENGTH - v.length(); pad > 0; pad--) {
      sb.append('0');
    }
    sb.append(v);
    return Bytes.toBytes(sb.toString());
  }

  static ExtendedCell createCell(long i, byte[] value) {
    return ExtendedCellBuilderFactory.create(CellBuilderType.DEEP_COPY).setRow(formatRowKey(i))
      .setFamily(Bytes.toBytes("cf")).setQualifier(Bytes.toBytes("q"))
      .setTimestamp(EnvironmentEdgeManager.currentTime()).setType(KeyValue.Type.Put.getCode())
      .setValue(value).build();
  }

  // ---- Block size sweep ----

  List<Integer> blockSizeSweep() {
    List<Integer> sizes = new ArrayList<>();
    int size = Integer.highestOneBit(minBlockSize);
    if (size < minBlockSize) {
      size <<= 1;
    }
    while (size <= maxBlockSize) {
      sizes.add(size);
      size <<= 1;
    }
    if (sizes.isEmpty()) {
      sizes.add(minBlockSize);
    }
    return sizes;
  }

  // ---- Predicator classes ----

  @SuppressWarnings("unchecked")
  static final Class<? extends BlockCompressedSizePredicator>[] PREDICATOR_CLASSES =
    new Class[] { UncompressedBlockSizePredicator.class,
      PreviousBlockCompressionRatePredicator.class, EWMABlockSizePredicator.class };

  // ---- Write helper: writes enough cells to fill numBlocks blocks ----

  /**
   * Writes an HFile with the given parameters and returns the path. Writes cells until at least
   * {@code numBlocks} data blocks are produced.
   */
  Path writeHFile(Configuration conf, FileSystem fs, Path dir, Compression.Algorithm compression,
    DataBlockEncoding encoding, int blockSize, String label) throws IOException {
    Path path = new Path(dir, "blockencpe-" + label + ".hfile");
    if (fs.exists(path)) {
      fs.delete(path, false);
    }

    HFileContext context = new HFileContextBuilder().withCompression(compression)
      .withDataBlockEncoding(encoding).withBlockSize(blockSize).build();

    CompressibleValueGenerator valueGen = new CompressibleValueGenerator(valueSize, targetRatio);

    long rowIndex = 0;
    // Estimate rows per block based on uncompressed cell size to avoid writing unnecessary cells.
    // Each cell is roughly ROW_KEY_LENGTH + family + qualifier + value + KV overhead.
    int approxCellSize = ROW_KEY_LENGTH + 2 + 1 + valueSize + KeyValue.FIXED_OVERHEAD;
    long estimatedRowsPerBlock = Math.max(1, blockSize / approxCellSize);
    // For compressed data the predicator will let blocks grow larger, account for the ratio.
    if (compression != Compression.Algorithm.NONE) {
      estimatedRowsPerBlock = (long) (estimatedRowsPerBlock * targetRatio);
    }
    long maxRows = estimatedRowsPerBlock * numBlocks * 3L;

    try (HFile.Writer writer =
      HFile.getWriterFactoryNoCache(conf).withPath(fs, path).withFileContext(context).create()) {
      for (rowIndex = 0; rowIndex < maxRows; rowIndex++) {
        writer.append(createCell(rowIndex, valueGen.generate()));
      }
    }
    return path;
  }

  // ---- Predicator accuracy benchmark ----

  static class PredicatorAccuracyResult {
    final String predicatorName;
    final String compression;
    final String encoding;
    final int blockSize;
    final int actualBlocks;
    final double meanCompressedSize;
    final double stddevCompressedSize;
    final int minCompressedSize;
    final int maxCompressedSize;
    final double meanDeviationPct;

    PredicatorAccuracyResult(String predicatorName, String compression, String encoding,
      int blockSize, int actualBlocks, double meanCompressedSize, double stddevCompressedSize,
      int minCompressedSize, int maxCompressedSize, double meanDeviationPct) {
      this.predicatorName = predicatorName;
      this.compression = compression;
      this.encoding = encoding;
      this.blockSize = blockSize;
      this.actualBlocks = actualBlocks;
      this.meanCompressedSize = meanCompressedSize;
      this.stddevCompressedSize = stddevCompressedSize;
      this.minCompressedSize = minCompressedSize;
      this.maxCompressedSize = maxCompressedSize;
      this.meanDeviationPct = meanDeviationPct;
    }
  }

  List<PredicatorAccuracyResult> runPredicatorAccuracyTests(Configuration baseConf, FileSystem fs,
    Path dir) throws IOException {
    List<PredicatorAccuracyResult> results = new ArrayList<>();
    List<Integer> blockSizes = blockSizeSweep();

    for (Compression.Algorithm compression : compressions) {
      for (DataBlockEncoding encoding : encodings) {
        for (int blockSize : blockSizes) {
          for (Class<? extends BlockCompressedSizePredicator> predClass : PREDICATOR_CLASSES) {
            String predName = predClass.getSimpleName();
            String label = String.format("%s-%s-%d-%s", compression.getName(), encoding.name(),
              blockSize, predName);

            Configuration conf = new Configuration(baseConf);
            conf.setClass(BlockCompressedSizePredicator.BLOCK_COMPRESSED_SIZE_PREDICATOR, predClass,
              BlockCompressedSizePredicator.class);

            LOG.info("Predicator accuracy test: compression={}, encoding={}, blockSize={}, "
              + "predicator={}", compression.getName(), encoding.name(), blockSize, predName);

            long startTime = EnvironmentEdgeManager.currentTime();
            Path hfilePath = writeHFile(conf, fs, dir, compression, encoding, blockSize, label);
            long writeElapsed = EnvironmentEdgeManager.currentTime() - startTime;

            List<Integer> compressedBlockSizes = new ArrayList<>();
            try (HFile.Reader reader =
              HFile.createReader(fs, hfilePath, CacheConfig.DISABLED, true, conf)) {
              try (HFileScanner scanner = reader.getScanner(conf, false, false)) {
                scanner.seekTo();
                HFileBlock prevBlock = null;
                do {
                  Cell cell = scanner.getCell();
                  if (cell == null) {
                    break;
                  }
                  // We iterate cells but track block transitions via the scanner's internal block
                } while (scanner.next());
              }

              // Read block-level info by traversing the data index
              int dataBlockCount = reader.getTrailer().getDataIndexCount();
              // Use the reader's block iterator to get on-disk sizes
              try (HFileScanner scanner = reader.getScanner(conf, false, false)) {
                if (scanner.seekTo()) {
                  HFileBlock block = null;
                  long offset = reader.getTrailer().getFirstDataBlockOffset();
                  long lastOffset = -1;
                  // Read blocks directly
                  while (offset >= 0 && offset != lastOffset) {
                    lastOffset = offset;
                    try {
                      block = reader.readBlock(offset, -1, false, false, false, true, null,
                        reader.getDataBlockEncoding());
                      if (block == null || !block.getBlockType().isData()) {
                        break;
                      }
                      compressedBlockSizes.add(block.getOnDiskSizeWithHeader());
                      long nextOffset = offset + block.getOnDiskSizeWithHeader();
                      block.release();
                      block = null;
                      offset = nextOffset;
                    } catch (Exception e) {
                      break;
                    }
                  }
                }
              }
            }

            // Compute stats
            int actualBlocks = compressedBlockSizes.size();
            double mean = 0;
            int min = Integer.MAX_VALUE;
            int max = Integer.MIN_VALUE;
            for (int s : compressedBlockSizes) {
              mean += s;
              min = Math.min(min, s);
              max = Math.max(max, s);
            }
            if (actualBlocks > 0) {
              mean /= actualBlocks;
            }
            double variance = 0;
            for (int s : compressedBlockSizes) {
              variance += (s - mean) * (s - mean);
            }
            double stddev = actualBlocks > 1 ? Math.sqrt(variance / (actualBlocks - 1)) : 0;
            double meanDeviationPct =
              blockSize > 0 ? Math.abs(mean - blockSize) / blockSize * 100.0 : 0;

            PredicatorAccuracyResult result = new PredicatorAccuracyResult(predName,
              compression.getName(), encoding.name(), blockSize, actualBlocks, mean, stddev,
              actualBlocks > 0 ? min : 0, actualBlocks > 0 ? max : 0, meanDeviationPct);
            results.add(result);

            LOG.info(
              "  predicator={}: blocks={}, meanOnDisk={}, stddev={}, min={}, max={}, "
                + "devFromTarget={}%, writeTime={}ms",
              predName, actualBlocks, String.format("%.0f", mean), String.format("%.0f", stddev),
              actualBlocks > 0 ? min : "N/A", actualBlocks > 0 ? max : "N/A",
              String.format("%.1f", meanDeviationPct), writeElapsed);

            // Cleanup
            fs.delete(hfilePath, false);
          }
        }
      }
    }
    return results;
  }

  // ---- Write throughput benchmark ----

  static class ThroughputResult {
    final String testName;
    final String compression;
    final String encoding;
    final int blockSize;
    final long elapsedMs;
    final long totalBytes;
    final double mbPerSec;
    final int blockCount;

    ThroughputResult(String testName, String compression, String encoding, int blockSize,
      long elapsedMs, long totalBytes, double mbPerSec, int blockCount) {
      this.testName = testName;
      this.compression = compression;
      this.encoding = encoding;
      this.blockSize = blockSize;
      this.elapsedMs = elapsedMs;
      this.totalBytes = totalBytes;
      this.mbPerSec = mbPerSec;
      this.blockCount = blockCount;
    }
  }

  List<ThroughputResult> runWriteBenchmarks(Configuration baseConf, FileSystem fs, Path dir)
    throws IOException {
    List<ThroughputResult> results = new ArrayList<>();
    List<Integer> blockSizes = blockSizeSweep();

    for (Compression.Algorithm compression : compressions) {
      for (DataBlockEncoding encoding : encodings) {
        for (int blockSize : blockSizes) {
          String label =
            String.format("write-%s-%s-%d", compression.getName(), encoding.name(), blockSize);

          Configuration conf = new Configuration(baseConf);
          // Use default predicator for throughput tests
          conf.setClass(BlockCompressedSizePredicator.BLOCK_COMPRESSED_SIZE_PREDICATOR,
            UncompressedBlockSizePredicator.class, BlockCompressedSizePredicator.class);

          LOG.info("Write benchmark: compression={}, encoding={}, blockSize={}",
            compression.getName(), encoding.name(), blockSize);

          long startTime = EnvironmentEdgeManager.currentTime();
          Path hfilePath = writeHFile(conf, fs, dir, compression, encoding, blockSize, label);
          long elapsed = EnvironmentEdgeManager.currentTime() - startTime;

          long fileSize = fs.getFileStatus(hfilePath).getLen();
          int blockCount = 0;
          try (HFile.Reader reader =
            HFile.createReader(fs, hfilePath, CacheConfig.DISABLED, true, conf)) {
            blockCount = reader.getTrailer().getDataIndexCount();
          }

          double mbps = elapsed > 0 ? (fileSize / (1024.0 * 1024.0)) / (elapsed / 1000.0) : 0;

          ThroughputResult result = new ThroughputResult("SequentialWrite", compression.getName(),
            encoding.name(), blockSize, elapsed, fileSize, mbps, blockCount);
          results.add(result);

          LOG.info("  elapsed={}ms, fileSize={}, blocks={}, throughput={} MB/s", elapsed, fileSize,
            blockCount, String.format("%.2f", mbps));

          // Keep file for read benchmarks if needed, otherwise clean up
          if (throughputOnly || !predicatorsOnly) {
            // Will be read by read benchmarks; clean up after read
          } else {
            fs.delete(hfilePath, false);
          }
        }
      }
    }
    return results;
  }

  // ---- Read throughput benchmarks ----

  List<ThroughputResult> runReadBenchmarks(Configuration baseConf, FileSystem fs, Path dir)
    throws IOException {
    List<ThroughputResult> results = new ArrayList<>();
    List<Integer> blockSizes = blockSizeSweep();

    for (Compression.Algorithm compression : compressions) {
      for (DataBlockEncoding encoding : encodings) {
        for (int blockSize : blockSizes) {
          String label =
            String.format("write-%s-%s-%d", compression.getName(), encoding.name(), blockSize);
          Path hfilePath = new Path(dir, "blockencpe-" + label + ".hfile");

          if (!fs.exists(hfilePath)) {
            // Write the file first if it doesn't exist
            Configuration writeConf = new Configuration(baseConf);
            writeConf.setClass(BlockCompressedSizePredicator.BLOCK_COMPRESSED_SIZE_PREDICATOR,
              UncompressedBlockSizePredicator.class, BlockCompressedSizePredicator.class);
            hfilePath = writeHFile(writeConf, fs, dir, compression, encoding, blockSize, label);
          }

          Configuration conf = new Configuration(baseConf);

          // Sequential read
          LOG.info("Sequential read benchmark: compression={}, encoding={}, blockSize={}",
            compression.getName(), encoding.name(), blockSize);

          long totalBytesRead = 0;
          int cellCount = 0;
          long startTime = EnvironmentEdgeManager.currentTime();
          try (HFile.Reader reader =
            HFile.createReader(fs, hfilePath, CacheConfig.DISABLED, true, conf)) {
            try (HFileScanner scanner = reader.getScanner(conf, false, false)) {
              scanner.seekTo();
              do {
                Cell cell = scanner.getCell();
                if (cell == null) {
                  break;
                }
                totalBytesRead += cell.getSerializedSize();
                cellCount++;
              } while (scanner.next());
            }
          }
          long seqElapsed = EnvironmentEdgeManager.currentTime() - startTime;

          double seqMbps =
            seqElapsed > 0 ? (totalBytesRead / (1024.0 * 1024.0)) / (seqElapsed / 1000.0) : 0;

          results.add(new ThroughputResult("SequentialRead", compression.getName(), encoding.name(),
            blockSize, seqElapsed, totalBytesRead, seqMbps, cellCount));

          LOG.info("  sequential: elapsed={}ms, cells={}, throughput={} MB/s", seqElapsed,
            cellCount, String.format("%.2f", seqMbps));

          // Random read
          LOG.info("Random read benchmark: compression={}, encoding={}, blockSize={}",
            compression.getName(), encoding.name(), blockSize);

          int randomReads = numBlocks;
          startTime = EnvironmentEdgeManager.currentTime();
          try (HFile.Reader reader =
            HFile.createReader(fs, hfilePath, CacheConfig.DISABLED, true, conf)) {
            long entryCount = reader.getEntries();
            for (int i = 0; i < randomReads; i++) {
              long randomRow = ThreadLocalRandom.current().nextLong(entryCount);
              byte[] rowKey = formatRowKey(randomRow);
              ExtendedCell seekCell = ExtendedCellBuilderFactory.create(CellBuilderType.DEEP_COPY)
                .setRow(rowKey).setFamily(Bytes.toBytes("cf")).setQualifier(Bytes.toBytes("q"))
                .setTimestamp(Long.MAX_VALUE).setType(KeyValue.Type.Maximum.getCode())
                .setValue(HConstants.EMPTY_BYTE_ARRAY).build();
              try (HFileScanner scanner = reader.getScanner(conf, false, true)) {
                scanner.seekTo(seekCell);
                Cell cell = scanner.getCell();
                if (cell != null) {
                  totalBytesRead += cell.getSerializedSize();
                }
              }
            }
          }
          long randElapsed = EnvironmentEdgeManager.currentTime() - startTime;
          double opsPerSec = randElapsed > 0 ? (randomReads * 1000.0) / randElapsed : 0;

          results.add(new ThroughputResult("RandomRead", compression.getName(), encoding.name(),
            blockSize, randElapsed, randomReads, opsPerSec, randomReads));

          LOG.info("  random: elapsed={}ms, reads={}, throughput={} ops/s", randElapsed,
            randomReads, String.format("%.0f", opsPerSec));

          // Cleanup
          fs.delete(hfilePath, false);
        }
      }
    }
    return results;
  }

  // ---- Summary formatting ----

  static String shortPredicatorName(String className) {
    if (className.contains("Uncompressed")) {
      return "Uncompressed";
    } else if (className.contains("Previous")) {
      return "PrevBlock";
    } else if (className.contains("EWMA")) {
      return "EWMA";
    }
    return className;
  }

  void printPredicatorSummary(List<PredicatorAccuracyResult> results) {
    System.out.println();
    System.out.println(
      "==========================================================================================");
    System.out.println("  PREDICATOR ACCURACY RESULTS");
    System.out.println(
      "==========================================================================================");
    System.out.println();
    System.out.printf("%-14s %-6s %-10s %7s %6s %10s %8s %10s %10s %7s%n", "Predicator", "Compr",
      "Encoding", "BlkSize", "Blocks", "MeanOnDisk", "Stddev", "Min", "Max", "Dev%");
    System.out.printf("%-14s %-6s %-10s %7s %6s %10s %8s %10s %10s %7s%n", "--------------",
      "------", "----------", "-------", "------", "----------", "--------", "----------",
      "----------", "-------");

    String prevGroup = null;
    for (PredicatorAccuracyResult r : results) {
      String group = r.compression + "|" + r.encoding + "|" + r.blockSize;
      if (prevGroup != null && !prevGroup.equals(group)) {
        System.out.println();
      }
      prevGroup = group;
      System.out.printf("%-14s %-6s %-10s %7d %6d %10.0f %8.0f %10d %10d %6.1f%%%n",
        shortPredicatorName(r.predicatorName), r.compression, r.encoding, r.blockSize,
        r.actualBlocks, r.meanCompressedSize, r.stddevCompressedSize, r.minCompressedSize,
        r.maxCompressedSize, r.meanDeviationPct);
    }
    System.out.println();
  }

  void printThroughputSummary(List<ThroughputResult> results) {
    System.out.println();
    System.out.println(
      "==========================================================================================");
    System.out.println("  THROUGHPUT RESULTS");
    System.out.println(
      "==========================================================================================");
    System.out.println();
    System.out.printf("%-16s %-6s %-10s %7s %10s %12s %14s%n", "Test", "Compr", "Encoding",
      "BlkSize", "Elapsed(ms)", "Bytes/Count", "Throughput");
    System.out.printf("%-16s %-6s %-10s %7s %10s %12s %14s%n", "----------------", "------",
      "----------", "-------", "----------", "------------", "--------------");

    String prevTest = null;
    for (ThroughputResult r : results) {
      if (prevTest != null && !prevTest.equals(r.testName)) {
        System.out.println();
      }
      prevTest = r.testName;
      String throughput;
      if ("RandomRead".equals(r.testName)) {
        throughput = String.format("%.0f ops/s", r.mbPerSec);
      } else {
        throughput = String.format("%.2f MB/s", r.mbPerSec);
      }
      System.out.printf("%-16s %-6s %-10s %7d %10d %12d %14s%n", r.testName, r.compression,
        r.encoding, r.blockSize, r.elapsedMs, r.totalBytes, throughput);
    }
    System.out.println();
  }

  // ---- Main driver ----

  void runBenchmarks() throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path dir = fs.makeQualified(new Path("blockenc-pe-" + System.currentTimeMillis()));
    fs.mkdirs(dir);

    LOG.info("HFileBlockPerformanceEvaluation starting");
    LOG.info("  blocks={}, minBlockSize={}, maxBlockSize={}, targetRatio={}, valueSize={}",
      numBlocks, minBlockSize, maxBlockSize, targetRatio, valueSize);
    LOG.info("  compressions={}", compressions);
    LOG.info("  encodings={}", encodings);
    LOG.info("  output dir={}", dir);

    try {
      if (!throughputOnly) {
        List<PredicatorAccuracyResult> predResults = runPredicatorAccuracyTests(conf, fs, dir);
        printPredicatorSummary(predResults);
      }

      if (!predicatorsOnly) {
        List<ThroughputResult> throughputResults = new ArrayList<>();

        List<ThroughputResult> writeResults = runWriteBenchmarks(conf, fs, dir);
        throughputResults.addAll(writeResults);

        List<ThroughputResult> readResults = runReadBenchmarks(conf, fs, dir);
        throughputResults.addAll(readResults);

        printThroughputSummary(throughputResults);
      }

    } finally {
      fs.delete(dir, true);
    }
  }

  // ---- CLI parsing ----

  void parseArgs(String[] args) {
    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "--blocks":
          numBlocks = Integer.parseInt(args[++i]);
          break;
        case "--compressions":
          compressions.clear();
          for (String name : args[++i].split(",")) {
            compressions
              .add(Compression.getCompressionAlgorithmByName(name.trim().toLowerCase(Locale.ROOT)));
          }
          break;
        case "--encodings":
          encodings.clear();
          for (String name : args[++i].split(",")) {
            encodings.add(DataBlockEncoding.valueOf(name.trim().toUpperCase(Locale.ROOT)));
          }
          break;
        case "--min-block-size":
          minBlockSize = Integer.parseInt(args[++i]);
          break;
        case "--max-block-size":
          maxBlockSize = Integer.parseInt(args[++i]);
          break;
        case "--target-ratio":
          targetRatio = Double.parseDouble(args[++i]);
          break;
        case "--value-size":
          valueSize = Integer.parseInt(args[++i]);
          break;
        case "--predicators-only":
          predicatorsOnly = true;
          break;
        case "--throughput-only":
          throughputOnly = true;
          break;
        default:
          LOG.warn("Unknown argument: {}", args[i]);
          printUsage();
          System.exit(1);
      }
    }

    if (compressions.isEmpty()) {
      compressions.add(Compression.Algorithm.NONE);
      compressions.add(Compression.Algorithm.GZ);
    }

    if (encodings.isEmpty()) {
      encodings.add(DataBlockEncoding.NONE);
      encodings.add(DataBlockEncoding.FAST_DIFF);
    }

    if (targetRatio < 1.0) {
      throw new IllegalArgumentException("target-ratio must be >= 1.0, got " + targetRatio);
    }
  }

  static void printUsage() {
    System.err.println("Usage: HFileBlockPerformanceEvaluation [options]");
    System.err.println("  --blocks N              Number of blocks per test (default: 100)");
    System.err.println(
      "  --compressions LIST     Comma-separated compression algorithms (default: none,gz)");
    System.err.println(
      "  --encodings LIST        Comma-separated data block encodings (default: none,fast_diff)");
    System.err.println("  --min-block-size BYTES  Minimum block size (default: 8192)");
    System.err.println("  --max-block-size BYTES  Maximum block size (default: 131072)");
    System.err.println("  --target-ratio FLOAT    Target compression ratio (default: 3.0)");
    System.err.println("  --value-size BYTES      Value size per cell (default: 1000)");
    System.err.println("  --predicators-only      Run only predicator accuracy tests");
    System.err.println("  --throughput-only       Run only read/write throughput tests");
  }

  public static void main(String[] args) throws Exception {
    HFileBlockPerformanceEvaluation eval = new HFileBlockPerformanceEvaluation();
    eval.parseArgs(args);
    eval.runBenchmarks();
  }
}
