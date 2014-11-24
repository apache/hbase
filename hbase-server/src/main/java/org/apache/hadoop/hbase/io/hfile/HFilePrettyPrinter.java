
/*
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
package org.apache.hadoop.hbase.io.hfile;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;

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
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.hfile.HFile.FileInfo;
import org.apache.hadoop.hbase.regionserver.TimeRangeTracker;
import org.apache.hadoop.hbase.util.BloomFilter;
import org.apache.hadoop.hbase.util.BloomFilterFactory;
import org.apache.hadoop.hbase.util.ByteBloomFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricPredicate;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.ConsoleReporter;

/**
 * Implements pretty-printing functionality for {@link HFile}s.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class HFilePrettyPrinter extends Configured implements Tool {

  private static final Log LOG = LogFactory.getLog(HFilePrettyPrinter.class);

  private Options options = new Options();

  private boolean verbose;
  private boolean printValue;
  private boolean printKey;
  private boolean shouldPrintMeta;
  private boolean printBlockIndex;
  private boolean printBlockHeaders;
  private boolean printStats;
  private boolean checkRow;
  private boolean checkFamily;
  private boolean isSeekToRow = false;

  /**
   * The row which the user wants to specify and print all the KeyValues for.
   */
  private byte[] row = null;

  private List<Path> files = new ArrayList<Path>();
  private int count;

  private static final String FOUR_SPACES = "    ";

  public HFilePrettyPrinter() {
    super();
    init();
  }

  public HFilePrettyPrinter(Configuration conf) {
    super(conf);
    init();
  }

  private void init() {
    options.addOption("v", "verbose", false,
        "Verbose output; emits file and meta data delimiters");
    options.addOption("p", "printkv", false, "Print key/value pairs");
    options.addOption("e", "printkey", false, "Print keys");
    options.addOption("m", "printmeta", false, "Print meta data of file");
    options.addOption("b", "printblocks", false, "Print block index meta data");
    options.addOption("h", "printblockheaders", false, "Print block headers for each block.");
    options.addOption("k", "checkrow", false,
        "Enable row order check; looks for out-of-order keys");
    options.addOption("a", "checkfamily", false, "Enable family check");
    options.addOption("w", "seekToRow", true,
      "Seek to this row and print all the kvs for this row only");
    options.addOption("s", "stats", false, "Print statistics");

    OptionGroup files = new OptionGroup();
    files.addOption(new Option("f", "file", true,
      "File to scan. Pass full-path; e.g. hdfs://a:9000/hbase/hbase:meta/12/34"));
    files.addOption(new Option("r", "region", true,
      "Region to scan. Pass region name; e.g. 'hbase:meta,,1'"));
    options.addOptionGroup(files);
  }

  public boolean parseOptions(String args[]) throws ParseException,
      IOException {
    if (args.length == 0) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("HFile", options, true);
      return false;
    }
    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args);

    verbose = cmd.hasOption("v");
    printValue = cmd.hasOption("p");
    printKey = cmd.hasOption("e") || printValue;
    shouldPrintMeta = cmd.hasOption("m");
    printBlockIndex = cmd.hasOption("b");
    printBlockHeaders = cmd.hasOption("h");
    printStats = cmd.hasOption("s");
    checkRow = cmd.hasOption("k");
    checkFamily = cmd.hasOption("a");

    if (cmd.hasOption("f")) {
      files.add(new Path(cmd.getOptionValue("f")));
    }

    if (cmd.hasOption("w")) {
      String key = cmd.getOptionValue("w");
      if (key != null && key.length() != 0) {
        row = key.getBytes();
        isSeekToRow = true;
      } else {
        System.err.println("Invalid row is specified.");
        System.exit(-1);
      }
    }

    if (cmd.hasOption("r")) {
      String regionName = cmd.getOptionValue("r");
      byte[] rn = Bytes.toBytes(regionName);
      byte[][] hri = HRegionInfo.parseRegionName(rn);
      Path rootDir = FSUtils.getRootDir(getConf());
      Path tableDir = FSUtils.getTableDir(rootDir, TableName.valueOf(hri[0]));
      String enc = HRegionInfo.encodeRegionName(rn);
      Path regionDir = new Path(tableDir, enc);
      if (verbose)
        System.out.println("region dir -> " + regionDir);
      List<Path> regionFiles = HFile.getStoreFiles(FileSystem.get(getConf()),
          regionDir);
      if (verbose)
        System.out.println("Number of region files found -> "
            + regionFiles.size());
      if (verbose) {
        int i = 1;
        for (Path p : regionFiles) {
          if (verbose)
            System.out.println("Found file[" + i++ + "] -> " + p);
        }
      }
      files.addAll(regionFiles);
    }

    return true;
  }

  /**
   * Runs the command-line pretty-printer, and returns the desired command
   * exit code (zero for success, non-zero for failure).
   */
  public int run(String[] args) {
    if (getConf() == null) {
      throw new RuntimeException("A Configuration instance must be provided.");
    }
    try {
      FSUtils.setFsDefault(getConf(), FSUtils.getRootDir(getConf()));
      if (!parseOptions(args))
        return 1;
    } catch (IOException ex) {
      LOG.error("Error parsing command-line options", ex);
      return 1;
    } catch (ParseException ex) {
      LOG.error("Error parsing command-line options", ex);
      return 1;
    }

    // iterate over all files found
    for (Path fileName : files) {
      try {
        processFile(fileName);
      } catch (IOException ex) {
        LOG.error("Error reading " + fileName, ex);
      }
    }

    if (verbose || printKey) {
      System.out.println("Scanned kv count -> " + count);
    }

    return 0;
  }

  private void processFile(Path file) throws IOException {
    if (verbose)
      System.out.println("Scanning -> " + file);
    FileSystem fs = file.getFileSystem(getConf());
    if (!fs.exists(file)) {
      System.err.println("ERROR, file doesnt exist: " + file);
    }

    HFile.Reader reader = HFile.createReader(fs, file, new CacheConfig(getConf()), getConf());

    Map<byte[], byte[]> fileInfo = reader.loadFileInfo();

    KeyValueStatsCollector fileStats = null;

    if (verbose || printKey || checkRow || checkFamily || printStats) {
      // scan over file and read key/value's and check if requested
      HFileScanner scanner = reader.getScanner(false, false, false);
      fileStats = new KeyValueStatsCollector();
      boolean shouldScanKeysValues = false;
      if (this.isSeekToRow) {
        // seek to the first kv on this row
        shouldScanKeysValues = 
          (scanner.seekTo(KeyValueUtil.createFirstOnRow(this.row).getKey()) != -1);
      } else {
        shouldScanKeysValues = scanner.seekTo();
      }
      if (shouldScanKeysValues)
        scanKeysValues(file, fileStats, scanner, row);
    }

    // print meta data
    if (shouldPrintMeta) {
      printMeta(reader, fileInfo);
    }

    if (printBlockIndex) {
      System.out.println("Block Index:");
      System.out.println(reader.getDataBlockIndexReader());
    }

    if (printBlockHeaders) {
      System.out.println("Block Headers:");
      /*
       * TODO: this same/similar block iteration logic is used in HFileBlock#blockRange and
       * TestLazyDataBlockDecompression. Refactor?
       */
      FSDataInputStreamWrapper fsdis = new FSDataInputStreamWrapper(fs, file);
      long fileSize = fs.getFileStatus(file).getLen();
      FixedFileTrailer trailer =
        FixedFileTrailer.readFromStream(fsdis.getStream(false), fileSize);
      long offset = trailer.getFirstDataBlockOffset(),
        max = trailer.getLastDataBlockOffset();
      HFileBlock block;
      while (offset <= max) {
        block = reader.readBlock(offset, -1, /* cacheBlock */ false, /* pread */ false,
          /* isCompaction */ false, /* updateCacheMetrics */ false, null, null);
        offset += block.getOnDiskSizeWithHeader();
        System.out.println(block);
      }
    }

    if (printStats) {
      fileStats.finish();
      System.out.println("Stats:\n" + fileStats);
    }

    reader.close();
  }

  private void scanKeysValues(Path file, KeyValueStatsCollector fileStats,
      HFileScanner scanner,  byte[] row) throws IOException {
    Cell pCell = null;
    do {
      Cell cell = scanner.getKeyValue();
      if (row != null && row.length != 0) {
        int result = CellComparator.compareRows(cell.getRowArray(), cell.getRowOffset(),
            cell.getRowLength(), row, 0, row.length);
        if (result > 0) {
          break;
        } else if (result < 0) {
          continue;
        }
      }
      // collect stats
      if (printStats) {
        fileStats.collect(cell);
      }
      // dump key value
      if (printKey) {
        System.out.print("K: " + cell);
        if (printValue) {
          System.out.print(" V: "
              + Bytes.toStringBinary(cell.getValueArray(), cell.getValueOffset(),
                  cell.getValueLength()));
          int i = 0;
          List<Tag> tags = Tag.asList(cell.getTagsArray(), cell.getTagsOffset(),
              cell.getTagsLength());
          for (Tag tag : tags) {
            System.out.print(String.format(" T[%d]: %s", i++,
                Bytes.toStringBinary(tag.getBuffer(), tag.getTagOffset(), tag.getTagLength())));
          }
        }
        System.out.println();
      }
      // check if rows are in order
      if (checkRow && pCell != null) {
        if (CellComparator.compareRows(pCell, cell) > 0) {
          System.err.println("WARNING, previous row is greater then"
              + " current row\n\tfilename -> " + file + "\n\tprevious -> "
              + CellUtil.getCellKey(pCell) + "\n\tcurrent  -> "
              + CellUtil.getCellKey(cell));
        }
      }
      // check if families are consistent
      if (checkFamily) {
        String fam = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(),
            cell.getFamilyLength());
        if (!file.toString().contains(fam)) {
          System.err.println("WARNING, filename does not match kv family,"
              + "\n\tfilename -> " + file + "\n\tkeyvalue -> "
              + CellUtil.getCellKey(cell));
        }
        if (pCell != null && CellComparator.compareFamilies(pCell, cell) != 0) {
          System.err.println("WARNING, previous kv has different family"
              + " compared to current key\n\tfilename -> " + file
              + "\n\tprevious -> " + CellUtil.getCellKey(pCell)
              + "\n\tcurrent  -> " + CellUtil.getCellKey(cell));
        }
      }
      pCell = cell;
      ++count;
    } while (scanner.next());
  }

  /**
   * Format a string of the form "k1=v1, k2=v2, ..." into separate lines
   * with a four-space indentation.
   */
  private static String asSeparateLines(String keyValueStr) {
    return keyValueStr.replaceAll(", ([a-zA-Z]+=)",
                                  ",\n" + FOUR_SPACES + "$1");
  }

  private void printMeta(HFile.Reader reader, Map<byte[], byte[]> fileInfo)
      throws IOException {
    System.out.println("Block index size as per heapsize: "
        + reader.indexSize());
    System.out.println(asSeparateLines(reader.toString()));
    System.out.println("Trailer:\n    "
        + asSeparateLines(reader.getTrailer().toString()));
    System.out.println("Fileinfo:");
    for (Map.Entry<byte[], byte[]> e : fileInfo.entrySet()) {
      System.out.print(FOUR_SPACES + Bytes.toString(e.getKey()) + " = ");
      if (Bytes.compareTo(e.getKey(), Bytes.toBytes("MAX_SEQ_ID_KEY")) == 0) {
        long seqid = Bytes.toLong(e.getValue());
        System.out.println(seqid);
      } else if (Bytes.compareTo(e.getKey(), Bytes.toBytes("TIMERANGE")) == 0) {
        TimeRangeTracker timeRangeTracker = new TimeRangeTracker();
        Writables.copyWritable(e.getValue(), timeRangeTracker);
        System.out.println(timeRangeTracker.getMinimumTimestamp() + "...."
            + timeRangeTracker.getMaximumTimestamp());
      } else if (Bytes.compareTo(e.getKey(), FileInfo.AVG_KEY_LEN) == 0
          || Bytes.compareTo(e.getKey(), FileInfo.AVG_VALUE_LEN) == 0) {
        System.out.println(Bytes.toInt(e.getValue()));
      } else {
        System.out.println(Bytes.toStringBinary(e.getValue()));
      }
    }

    try {
      System.out.println("Mid-key: " + Bytes.toStringBinary(reader.midkey()));
    } catch (Exception e) {
      System.out.println ("Unable to retrieve the midkey");
    }

    // Printing general bloom information
    DataInput bloomMeta = reader.getGeneralBloomFilterMetadata();
    BloomFilter bloomFilter = null;
    if (bloomMeta != null)
      bloomFilter = BloomFilterFactory.createFromMeta(bloomMeta, reader);

    System.out.println("Bloom filter:");
    if (bloomFilter != null) {
      System.out.println(FOUR_SPACES + bloomFilter.toString().replaceAll(
          ByteBloomFilter.STATS_RECORD_SEP, "\n" + FOUR_SPACES));
    } else {
      System.out.println(FOUR_SPACES + "Not present");
    }

    // Printing delete bloom information
    bloomMeta = reader.getDeleteBloomFilterMetadata();
    bloomFilter = null;
    if (bloomMeta != null)
      bloomFilter = BloomFilterFactory.createFromMeta(bloomMeta, reader);

    System.out.println("Delete Family Bloom filter:");
    if (bloomFilter != null) {
      System.out.println(FOUR_SPACES
          + bloomFilter.toString().replaceAll(ByteBloomFilter.STATS_RECORD_SEP,
              "\n" + FOUR_SPACES));
    } else {
      System.out.println(FOUR_SPACES + "Not present");
    }
  }

  private static class KeyValueStatsCollector {
    private final MetricsRegistry metricsRegistry = new MetricsRegistry();
    private final ByteArrayOutputStream metricsOutput = new ByteArrayOutputStream();
    private final SimpleReporter simpleReporter = new SimpleReporter(metricsRegistry, new PrintStream(metricsOutput));
    Histogram keyLen = metricsRegistry.newHistogram(HFilePrettyPrinter.class, "Key length");
    Histogram valLen = metricsRegistry.newHistogram(HFilePrettyPrinter.class, "Val length");
    Histogram rowSizeBytes = metricsRegistry.newHistogram(HFilePrettyPrinter.class, "Row size (bytes)");
    Histogram rowSizeCols = metricsRegistry.newHistogram(HFilePrettyPrinter.class, "Row size (columns)");

    long curRowBytes = 0;
    long curRowCols = 0;

    byte[] biggestRow = null;

    private Cell prevCell = null;
    private long maxRowBytes = 0;
    private long curRowKeyLength;

    public void collect(Cell cell) {
      valLen.update(cell.getValueLength());
      if (prevCell != null &&
          KeyValue.COMPARATOR.compareRows(prevCell, cell) != 0) {
        // new row
        collectRow();
      }
      curRowBytes += KeyValueUtil.length(cell);
      curRowKeyLength = KeyValueUtil.keyLength(cell);
      curRowCols++;
      prevCell = cell;
    }

    private void collectRow() {
      rowSizeBytes.update(curRowBytes);
      rowSizeCols.update(curRowCols);
      keyLen.update(curRowKeyLength);

      if (curRowBytes > maxRowBytes && prevCell != null) {
        biggestRow = prevCell.getRow();
        maxRowBytes = curRowBytes;
      }

      curRowBytes = 0;
      curRowCols = 0;
    }

    public void finish() {
      if (curRowCols > 0) {
        collectRow();
      }
    }

    @Override
    public String toString() {
      if (prevCell == null)
        return "no data available for statistics";

      // Dump the metrics to the output stream
      simpleReporter.shutdown();
      simpleReporter.run();
      metricsRegistry.shutdown();

      return
              metricsOutput.toString() +
                      "Key of biggest row: " + Bytes.toStringBinary(biggestRow);
    }
  }

  private static class SimpleReporter extends ConsoleReporter {
    private final PrintStream out;

    public SimpleReporter(MetricsRegistry metricsRegistry, PrintStream out) {
      super(metricsRegistry, out, MetricPredicate.ALL);
      this.out = out;
    }

    @Override
    public void run() {
      for (Map.Entry<String, SortedMap<MetricName, Metric>> entry : getMetricsRegistry().groupedMetrics(
              MetricPredicate.ALL).entrySet()) {
        try {
          for (Map.Entry<MetricName, Metric> subEntry : entry.getValue().entrySet()) {
            out.print("   " + subEntry.getKey().getName());
            out.println(':');

            subEntry.getValue().processWith(this, subEntry.getKey(), out);
          }
        } catch (Exception e) {
          e.printStackTrace(out);
        }
      }
    }

    @Override
    public void processHistogram(MetricName name, Histogram histogram, PrintStream stream) {
      super.processHistogram(name, histogram, stream);
      stream.printf(Locale.getDefault(), "             count = %d%n", histogram.count());
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    // no need for a block cache
    conf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0);
    int ret = ToolRunner.run(conf, new HFilePrettyPrinter(), args);
    System.exit(ret);
  }
}
