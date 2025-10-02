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
package org.apache.hadoop.hbase.io.hfile;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformReservoir;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.IOException;
import java.io.PrintStream;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.atomic.LongAdder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.TimeRangeTracker;
import org.apache.hadoop.hbase.util.BloomFilter;
import org.apache.hadoop.hbase.util.BloomFilterFactory;
import org.apache.hadoop.hbase.util.BloomFilterUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLineParser;
import org.apache.hbase.thirdparty.org.apache.commons.cli.HelpFormatter;
import org.apache.hbase.thirdparty.org.apache.commons.cli.Option;
import org.apache.hbase.thirdparty.org.apache.commons.cli.OptionGroup;
import org.apache.hbase.thirdparty.org.apache.commons.cli.Options;
import org.apache.hbase.thirdparty.org.apache.commons.cli.ParseException;
import org.apache.hbase.thirdparty.org.apache.commons.cli.PosixParser;

/**
 * Implements pretty-printing functionality for {@link HFile}s.
 * <p>
 * This tool supports all HFile versions (v2, v3, and v4) with version-specific enhancements:
 * <ul>
 * <li><b>HFile v2:</b> Basic file inspection, metadata, block headers, and key/value display</li>
 * <li><b>HFile v3:</b> All v2 features plus tag support and encryption metadata</li>
 * <li><b>HFile v4:</b> All v3 features plus multi-tenant support, tenant section display, and
 * enhanced metadata for tenant isolation</li>
 * </ul>
 * <p>
 * Key improvements for HFile v4 multi-tenant support:
 * <ul>
 * <li>Version-aware block index handling (graceful fallback for v4)</li>
 * <li>Enhanced block header display with tenant-aware error handling</li>
 * <li>Tenant-specific information display with -t option</li>
 * <li>Tenant boundary detection in key/value output</li>
 * <li>V4-specific trailer field display (multi-tenant flags, tenant prefix length)</li>
 * <li>Tenant isolation considerations (suppressed last key)</li>
 * </ul>
 * <p>
 * Usage examples:
 *
 * <pre>
 * # Basic metadata for any HFile version
 * hbase hfile -m -f /path/to/hfile
 *
 * # Key/value pairs with tenant information (v4 files)
 * hbase hfile -p -v -t -f /path/to/v4/hfile
 *
 * # Block analysis (works across all versions)
 * hbase hfile -b -h -f /path/to/hfile
 * </pre>
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
@InterfaceStability.Evolving
public class HFilePrettyPrinter extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory.getLogger(HFilePrettyPrinter.class);

  private Options options = new Options();

  private boolean verbose;
  private boolean printValue;
  private boolean printKey;
  private boolean shouldPrintMeta;
  private boolean printBlockIndex;
  private boolean printBlockHeaders;
  private boolean printStats;
  private boolean printStatRanges;
  private boolean checkRow;
  private boolean checkFamily;
  private boolean isSeekToRow = false;
  private boolean checkMobIntegrity = false;
  private boolean printTenantInfo = false;
  private Map<String, List<Path>> mobFileLocations;
  private static final int FOUND_MOB_FILES_CACHE_CAPACITY = 50;
  private static final int MISSING_MOB_FILES_CACHE_CAPACITY = 20;
  private PrintStream out = System.out;
  private PrintStream err = System.err;

  // Configurable block display limits
  private int maxBlocksToShow;
  private static final int DEFAULT_MAX_BLOCKS = 50;

  /**
   * The row which the user wants to specify and print all the KeyValues for.
   */
  private byte[] row = null;

  private List<Path> files = new ArrayList<>();
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
    options.addOption("v", "verbose", false, "Verbose output; emits file and meta data delimiters");
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
    options.addOption("d", "details", false,
      "Print detailed statistics, including counts by range");
    options.addOption("i", "checkMobIntegrity", false,
      "Print all cells whose mob files are missing");
    options.addOption("t", "tenantinfo", false,
      "Print tenant information for multi-tenant HFiles (v4+)");
    options.addOption("l", "blocklimit", true, "Maximum number of blocks to display (default: 50)");

    OptionGroup files = new OptionGroup();
    files.addOption(new Option("f", "file", true,
      "File to scan. Pass full-path; e.g. hdfs://a:9000/hbase/hbase:meta/12/34"));
    files.addOption(
      new Option("r", "region", true, "Region to scan. Pass region name; e.g. 'hbase:meta,,1'"));
    options.addOptionGroup(files);
  }

  public void setPrintStreams(PrintStream out, PrintStream err) {
    this.out = out;
    this.err = err;
  }

  public boolean parseOptions(String args[]) throws ParseException, IOException {
    if (args.length == 0) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("hfile", options, true);
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
    printStatRanges = cmd.hasOption("d");
    printStats = cmd.hasOption("s") || printStatRanges;
    checkRow = cmd.hasOption("k");
    checkFamily = cmd.hasOption("a");
    checkMobIntegrity = cmd.hasOption("i");
    printTenantInfo = cmd.hasOption("t");

    if (cmd.hasOption("l")) {
      try {
        int limit = Integer.parseInt(cmd.getOptionValue("l"));
        if (limit > 0) {
          maxBlocksToShow = limit;
        } else {
          err.println("Invalid block limit: " + limit + ". Must be a positive number.");
          System.exit(-1);
        }
      } catch (NumberFormatException e) {
        err.println("Invalid block limit format: " + cmd.getOptionValue("l"));
        System.exit(-1);
      }
    }

    if (cmd.hasOption("f")) {
      files.add(new Path(cmd.getOptionValue("f")));
    }

    if (cmd.hasOption("w")) {
      String key = cmd.getOptionValue("w");
      if (key != null && key.length() != 0) {
        row = Bytes.toBytesBinary(key);
        isSeekToRow = true;
      } else {
        err.println("Invalid row is specified.");
        System.exit(-1);
      }
    }

    if (cmd.hasOption("r")) {
      String regionName = cmd.getOptionValue("r");
      byte[] rn = Bytes.toBytes(regionName);
      byte[][] hri = RegionInfo.parseRegionName(rn);
      Path rootDir = CommonFSUtils.getRootDir(getConf());
      Path tableDir = CommonFSUtils.getTableDir(rootDir, TableName.valueOf(hri[0]));
      String enc = RegionInfo.encodeRegionName(rn);
      Path regionDir = new Path(tableDir, enc);
      if (verbose) out.println("region dir -> " + regionDir);
      List<Path> regionFiles = HFile.getStoreFiles(FileSystem.get(getConf()), regionDir);
      if (verbose) out.println("Number of region files found -> " + regionFiles.size());
      if (verbose) {
        int i = 1;
        for (Path p : regionFiles) {
          if (verbose) out.println("Found file[" + i++ + "] -> " + p);
        }
      }
      files.addAll(regionFiles);
    }

    if (checkMobIntegrity) {
      if (verbose) {
        System.out.println("checkMobIntegrity is enabled");
      }
      mobFileLocations = new HashMap<>();
    }

    cmd.getArgList().forEach((file) -> files.add(new Path(file)));

    return true;
  }

  /**
   * Runs the command-line pretty-printer, and returns the desired command exit code (zero for
   * success, non-zero for failure).
   */
  @Override
  public int run(String[] args) {
    if (getConf() == null) {
      throw new RuntimeException("A Configuration instance must be provided.");
    }
    try {
      CommonFSUtils.setFsDefault(getConf(), CommonFSUtils.getRootDir(getConf()));
      if (!parseOptions(args)) {
        return 1;
      }
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
        int exitCode = processFile(fileName, false);
        if (exitCode != 0) {
          return exitCode;
        }
      } catch (IOException ex) {
        LOG.error("Error reading " + fileName, ex);
        return -2;
      }
    }

    if (verbose || printKey) {
      out.println("Scanned kv count -> " + count);
    }

    return 0;
  }

  // HBASE-22561 introduces boolean checkRootDir for WebUI specificly
  public int processFile(Path file, boolean checkRootDir) throws IOException {
    if (verbose) {
      out.println("Scanning -> " + file);
    }

    if (checkRootDir) {
      Path rootPath = CommonFSUtils.getRootDir(getConf());
      String rootString = rootPath + Path.SEPARATOR;
      if (!file.toString().startsWith(rootString)) {
        // First we see if fully-qualified URI matches the root dir. It might
        // also be an absolute path in the same filesystem, so we prepend the FS
        // of the root dir and see if that fully-qualified URI matches.
        FileSystem rootFS = rootPath.getFileSystem(getConf());
        String qualifiedFile = rootFS.getUri().toString() + file.toString();
        if (!qualifiedFile.startsWith(rootString)) {
          err.println(
            "ERROR, file (" + file + ") is not in HBase's root directory (" + rootString + ")");
          return -2;
        }
      }
    }

    FileSystem fs = file.getFileSystem(getConf());
    if (!fs.exists(file)) {
      err.println("ERROR, file doesnt exist: " + file);
      return -2;
    }

    HFile.Reader reader = HFile.createReader(fs, file, CacheConfig.DISABLED, true, getConf());

    Map<byte[], byte[]> fileInfo = reader.getHFileInfo();
    FixedFileTrailer trailer = reader.getTrailer();
    int majorVersion = trailer.getMajorVersion();
    boolean isV4 = majorVersion == HFile.MIN_FORMAT_VERSION_WITH_MULTI_TENANT;

    KeyValueStatsCollector fileStats = null;

    if (verbose || printKey || checkRow || checkFamily || printStats || checkMobIntegrity) {
      // scan over file and read key/value's and check if requested
      HFileScanner scanner = reader.getScanner(getConf(), false, false, false);
      fileStats = new KeyValueStatsCollector();
      boolean shouldScanKeysValues;
      if (this.isSeekToRow && !Bytes.equals(row, reader.getFirstRowKey().orElse(null))) {
        // seek to the first kv on this row
        shouldScanKeysValues = (scanner.seekTo(PrivateCellUtil.createFirstOnRow(this.row)) != -1);
      } else {
        shouldScanKeysValues = scanner.seekTo();
      }
      if (shouldScanKeysValues) {
        scanKeysValues(file, fileStats, scanner, row);
      }
    }

    // print meta data
    if (shouldPrintMeta) {
      printMeta(reader, fileInfo, isV4);
    }

    // print tenant information for v4 files
    if (printTenantInfo && isV4) {
      printTenantInformation(reader);
    }

    if (printBlockIndex) {
      printBlockIndex(reader, isV4);
    }

    if (printBlockHeaders) {
      printBlockHeaders(reader, file, fs, isV4);
    }

    if (printStats) {
      fileStats.finish(printStatRanges);
      out.println("Stats:\n" + fileStats);
    }

    reader.close();
    return 0;
  }

  /**
   * Get the effective block limit based on user configuration.
   * @return the effective block limit to use
   */
  private int getEffectiveBlockLimit() {
    // If user specified a custom limit (> 0), use it
    if (maxBlocksToShow > 0) {
      return maxBlocksToShow;
    }
    // Otherwise use default
    return DEFAULT_MAX_BLOCKS;
  }

  private void scanKeysValues(Path file, KeyValueStatsCollector fileStats, HFileScanner scanner,
    byte[] row) throws IOException {
    Cell pCell = null;
    FileSystem fs = FileSystem.get(getConf());
    Set<String> foundMobFiles = new LinkedHashSet<>(FOUND_MOB_FILES_CACHE_CAPACITY);
    Set<String> missingMobFiles = new LinkedHashSet<>(MISSING_MOB_FILES_CACHE_CAPACITY);

    // Check if this is a v4 file for enhanced output
    boolean isV4 = false;
    String currentTenantId = null;
    try {
      HFile.Reader reader = scanner.getReader();
      if (
        reader != null
          && reader.getTrailer().getMajorVersion() == HFile.MIN_FORMAT_VERSION_WITH_MULTI_TENANT
      ) {
        isV4 = true;
        if (verbose) {
          out.println("Scanning HFile v4 - tenant boundaries may be shown");
        }
      }
    } catch (Exception e) {
      // Continue without tenant-specific processing
    }

    do {
      ExtendedCell cell = scanner.getCell();
      if (row != null && row.length != 0) {
        int result = CellComparator.getInstance().compareRows(cell, row, 0, row.length);
        if (result > 0) {
          break;
        } else if (result < 0) {
          continue;
        }
      }

      // For multi-tenant v4 files, try to extract tenant information
      if (isV4 && printKey) {
        String extractedTenantId = extractTenantIdFromCell(cell, scanner.getReader());
        if (extractedTenantId != null && !extractedTenantId.equals(currentTenantId)) {
          if (currentTenantId != null) {
            out.println("--- End of tenant section: " + currentTenantId + " ---");
          }
          currentTenantId = extractedTenantId;
          out.println("--- Start of tenant section: " + currentTenantId + " ---");
        }
      }

      // collect stats
      if (printStats) {
        fileStats.collect(cell, printStatRanges);
      }
      // dump key value
      if (printKey) {
        out.print("K: " + cell);
        if (printValue) {
          out.print(" V: " + Bytes.toStringBinary(cell.getValueArray(), cell.getValueOffset(),
            cell.getValueLength()));
          int i = 0;
          List<Tag> tags = PrivateCellUtil.getTags(cell);
          for (Tag tag : tags) {
            out.print(String.format(" T[%d]: %s", i++, tag.toString()));
          }
        }
        // Show tenant ID if available and verbose mode is on
        if (isV4 && verbose && currentTenantId != null) {
          out.print(" [Tenant: " + currentTenantId + "]");
        }
        out.println();
      }
      // check if rows are in order
      if (checkRow && pCell != null) {
        if (CellComparator.getInstance().compareRows(pCell, cell) > 0) {
          err.println("WARNING, previous row is greater then" + " current row\n\tfilename -> "
            + file + "\n\tprevious -> " + CellUtil.getCellKeyAsString(pCell) + "\n\tcurrent  -> "
            + CellUtil.getCellKeyAsString(cell));
        }
      }
      // check if families are consistent
      if (checkFamily) {
        String fam =
          Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
        if (!file.toString().contains(fam)) {
          err.println("WARNING, filename does not match kv family," + "\n\tfilename -> " + file
            + "\n\tkeyvalue -> " + CellUtil.getCellKeyAsString(cell));
        }
        if (pCell != null && CellComparator.getInstance().compareFamilies(pCell, cell) != 0) {
          err.println(
            "WARNING, previous kv has different family" + " compared to current key\n\tfilename -> "
              + file + "\n\tprevious -> " + CellUtil.getCellKeyAsString(pCell) + "\n\tcurrent  -> "
              + CellUtil.getCellKeyAsString(cell));
        }
      }
      // check if mob files are missing.
      if (checkMobIntegrity && MobUtils.isMobReferenceCell(cell)) {
        Optional<TableName> tn = MobUtils.getTableName(cell);
        if (!tn.isPresent()) {
          System.err.println(
            "ERROR, wrong tag format in mob reference cell " + CellUtil.getCellKeyAsString(cell));
        } else if (!MobUtils.hasValidMobRefCellValue(cell)) {
          System.err.println(
            "ERROR, wrong value format in mob reference cell " + CellUtil.getCellKeyAsString(cell));
        } else {
          String mobFileName = MobUtils.getMobFileName(cell);
          boolean exist = mobFileExists(fs, tn.get(), mobFileName,
            Bytes.toString(CellUtil.cloneFamily(cell)), foundMobFiles, missingMobFiles);
          if (!exist) {
            // report error
            System.err.println("ERROR, the mob file [" + mobFileName
              + "] is missing referenced by cell " + CellUtil.getCellKeyAsString(cell));
          }
        }
      }
      pCell = cell;
      ++count;
    } while (scanner.next());

    // Close final tenant section if we were tracking it
    if (isV4 && printKey && currentTenantId != null) {
      out.println("--- End of tenant section: " + currentTenantId + " ---");
    }
  }

  /**
   * Enhanced tenant ID extraction that uses trailer information when available.
   */
  private String extractTenantIdFromCell(ExtendedCell cell, HFile.Reader reader) {
    try {
      FixedFileTrailer trailer = reader.getTrailer();
      int tenantPrefixLength = 4; // fallback default

      // For v4 files, always try to get the actual tenant prefix length from trailer
      if (trailer.getMajorVersion() == HFile.MIN_FORMAT_VERSION_WITH_MULTI_TENANT) {
        tenantPrefixLength = trailer.getTenantPrefixLength();
      }

      byte[] rowKey = CellUtil.cloneRow(cell);
      if (rowKey.length >= tenantPrefixLength) {
        return Bytes.toStringBinary(rowKey, 0, tenantPrefixLength);
      } else {
        // Row key is shorter than expected tenant prefix
        if (verbose && rowKey.length > 0) {
          err.println("Warning: Row key length (" + rowKey.length
            + ") is shorter than tenant prefix length (" + tenantPrefixLength + ")");
        }
        return rowKey.length > 0 ? Bytes.toStringBinary(rowKey) : null;
      }
    } catch (Exception e) {
      if (verbose) {
        err.println("Warning: Error extracting tenant ID from cell: " + e.getMessage());
      }
    }
    return null;
  }

  /**
   * Checks whether the referenced mob file exists.
   */
  private boolean mobFileExists(FileSystem fs, TableName tn, String mobFileName, String family,
    Set<String> foundMobFiles, Set<String> missingMobFiles) throws IOException {
    if (foundMobFiles.contains(mobFileName)) {
      return true;
    }
    if (missingMobFiles.contains(mobFileName)) {
      return false;
    }
    String tableName = tn.getNameAsString();
    List<Path> locations = mobFileLocations.get(tableName);
    if (locations == null) {
      locations = new ArrayList<>(2);
      locations.add(MobUtils.getMobFamilyPath(getConf(), tn, family));
      locations.add(HFileArchiveUtil.getStoreArchivePath(getConf(), tn,
        MobUtils.getMobRegionInfo(tn).getEncodedName(), family));
      mobFileLocations.put(tn.getNameAsString(), locations);
    }
    boolean exist = false;
    for (Path location : locations) {
      Path mobFilePath = new Path(location, mobFileName);
      if (fs.exists(mobFilePath)) {
        exist = true;
        break;
      }
    }
    if (exist) {
      evictMobFilesIfNecessary(foundMobFiles, FOUND_MOB_FILES_CACHE_CAPACITY);
      foundMobFiles.add(mobFileName);
    } else {
      evictMobFilesIfNecessary(missingMobFiles, MISSING_MOB_FILES_CACHE_CAPACITY);
      missingMobFiles.add(mobFileName);
    }
    return exist;
  }

  /**
   * Evicts the cached mob files if the set is larger than the limit.
   */
  private void evictMobFilesIfNecessary(Set<String> mobFileNames, int limit) {
    if (mobFileNames.size() < limit) {
      return;
    }
    int index = 0;
    int evict = limit / 2;
    Iterator<String> fileNamesItr = mobFileNames.iterator();
    while (index < evict && fileNamesItr.hasNext()) {
      fileNamesItr.next();
      fileNamesItr.remove();
      index++;
    }
  }

  /**
   * Format a string of the form "k1=v1, k2=v2, ..." into separate lines with a four-space
   * indentation.
   */
  private static String asSeparateLines(String keyValueStr) {
    return keyValueStr.replaceAll(", ([a-zA-Z]+=)", ",\n" + FOUR_SPACES + "$1");
  }

  private void printMeta(HFile.Reader reader, Map<byte[], byte[]> fileInfo, boolean isV4)
    throws IOException {
    out.println("Block index size as per heapsize: " + reader.indexSize());
    out.println(asSeparateLines(reader.toString()));

    FixedFileTrailer trailer = reader.getTrailer();
    out.println("Trailer:\n    " + asSeparateLines(trailer.toString()));

    // Print v4-specific trailer information if available
    if (isV4) {
      printV4SpecificTrailerInfo(trailer);
    }

    out.println("Fileinfo:");
    for (Map.Entry<byte[], byte[]> e : fileInfo.entrySet()) {
      out.print(FOUR_SPACES + Bytes.toString(e.getKey()) + " = ");
      if (
        Bytes.equals(e.getKey(), HStoreFile.MAX_SEQ_ID_KEY)
          || Bytes.equals(e.getKey(), HStoreFile.DELETE_FAMILY_COUNT)
          || Bytes.equals(e.getKey(), HStoreFile.EARLIEST_PUT_TS)
          || Bytes.equals(e.getKey(), HFileWriterImpl.MAX_MEMSTORE_TS_KEY)
          || Bytes.equals(e.getKey(), HFileInfo.CREATE_TIME_TS)
          || Bytes.equals(e.getKey(), HStoreFile.BULKLOAD_TIME_KEY)
      ) {
        out.println(Bytes.toLong(e.getValue()));
      } else if (Bytes.equals(e.getKey(), HStoreFile.TIMERANGE_KEY)) {
        TimeRangeTracker timeRangeTracker = TimeRangeTracker.parseFrom(e.getValue());
        out.println(timeRangeTracker.getMin() + "...." + timeRangeTracker.getMax());
      } else if (
        Bytes.equals(e.getKey(), HFileInfo.AVG_KEY_LEN)
          || Bytes.equals(e.getKey(), HFileInfo.AVG_VALUE_LEN)
          || Bytes.equals(e.getKey(), HFileWriterImpl.KEY_VALUE_VERSION)
          || Bytes.equals(e.getKey(), HFileInfo.MAX_TAGS_LEN)
          || Bytes.equals(e.getKey(), Bytes.toBytes(MultiTenantHFileWriter.FILEINFO_SECTION_COUNT))
          || Bytes.equals(e.getKey(),
            Bytes.toBytes(MultiTenantHFileWriter.FILEINFO_TENANT_INDEX_LEVELS))
          || Bytes.equals(e.getKey(),
            Bytes.toBytes(MultiTenantHFileWriter.FILEINFO_TENANT_INDEX_MAX_CHUNK))
      ) {
        out.println(Bytes.toInt(e.getValue()));
      } else if (
        Bytes.equals(e.getKey(), HStoreFile.MAJOR_COMPACTION_KEY)
          || Bytes.equals(e.getKey(), HFileInfo.TAGS_COMPRESSED)
          || Bytes.equals(e.getKey(), HStoreFile.EXCLUDE_FROM_MINOR_COMPACTION_KEY)
          || Bytes.equals(e.getKey(), HStoreFile.HISTORICAL_KEY)
      ) {
        out.println(Bytes.toBoolean(e.getValue()));
      } else if (Bytes.equals(e.getKey(), HFileInfo.LASTKEY)) {
        out.println(new KeyValue.KeyOnlyKeyValue(e.getValue()).toString());
      } else {
        out.println(Bytes.toStringBinary(e.getValue()));
      }
    }

    // For v4 files, also print section-level trailers and FileInfo
    if (isV4 && reader instanceof AbstractMultiTenantReader) {
      printSectionTrailers((AbstractMultiTenantReader) reader);
      printSectionFileInfo((AbstractMultiTenantReader) reader);
    }

    // Mid-key handling for different versions
    try {
      out.println("Mid-key: " + reader.midKey().map(CellUtil::getCellKeyAsString));
    } catch (Exception e) {
      out.println("Unable to retrieve the midkey");
    }

    // Printing general bloom information
    DataInput bloomMeta = reader.getGeneralBloomFilterMetadata();
    BloomFilter bloomFilter = null;
    if (bloomMeta != null) bloomFilter = BloomFilterFactory.createFromMeta(bloomMeta, reader);

    out.println("Bloom filter:");
    if (bloomFilter != null) {
      out.println(FOUR_SPACES
        + bloomFilter.toString().replaceAll(BloomFilterUtil.STATS_RECORD_SEP, "\n" + FOUR_SPACES));
    } else {
      out.println(FOUR_SPACES + "Not present");
    }

    // Printing delete bloom information
    bloomMeta = reader.getDeleteBloomFilterMetadata();
    bloomFilter = null;
    if (bloomMeta != null) bloomFilter = BloomFilterFactory.createFromMeta(bloomMeta, reader);

    out.println("Delete Family Bloom filter:");
    if (bloomFilter != null) {
      out.println(FOUR_SPACES
        + bloomFilter.toString().replaceAll(BloomFilterUtil.STATS_RECORD_SEP, "\n" + FOUR_SPACES));
    } else {
      out.println(FOUR_SPACES + "Not present");
    }

    // For v4 files, also print section-level bloom filter information
    if (isV4 && reader instanceof AbstractMultiTenantReader) {
      printSectionBloomFilters((AbstractMultiTenantReader) reader);
    }
  }

  /**
   * Print trailer information for each section in a multi-tenant HFile v4. Each section is
   * essentially an HFile v3 with its own trailer.
   * @param mtReader the multi-tenant reader to get section information from
   */
  private void printSectionTrailers(AbstractMultiTenantReader mtReader) {
    byte[][] tenantSectionIds = mtReader.getAllTenantSectionIds();

    if (tenantSectionIds == null || tenantSectionIds.length == 0) {
      out.println("Section-level Trailers: No sections found");
      return;
    }

    out.println("Section-level Trailers:");

    for (int i = 0; i < tenantSectionIds.length; i++) {
      byte[] sectionId = tenantSectionIds[i];
      out.println(
        FOUR_SPACES + "--- Section " + i + ": " + Bytes.toStringBinary(sectionId) + " ---");

      try (AbstractMultiTenantReader.SectionReaderLease lease =
        mtReader.getSectionReader(sectionId)) {
        if (lease != null) {
          HFileReaderImpl sectionHFileReader = lease.getReader();
          if (sectionHFileReader != null) {
            FixedFileTrailer sectionTrailer = sectionHFileReader.getTrailer();
            if (sectionTrailer != null) {
              out.println(FOUR_SPACES + FOUR_SPACES + "Section Trailer:");
              String trailerStr = sectionTrailer.toString();
              String[] lines = trailerStr.split("\n");
              for (String line : lines) {
                out.println(FOUR_SPACES + FOUR_SPACES + FOUR_SPACES + line);
              }
            } else {
              out.println(FOUR_SPACES + FOUR_SPACES + "Section trailer not available");
            }
          } else {
            out.println(FOUR_SPACES + FOUR_SPACES + "Section reader not initialized");
          }
        } else {
          out.println(FOUR_SPACES + FOUR_SPACES + "Could not create section reader");
        }
      } catch (IllegalArgumentException | IOException sectionException) {
        out.println(FOUR_SPACES + FOUR_SPACES + "Error accessing section trailer: "
          + sectionException.getMessage());
      }

      if (i < tenantSectionIds.length - 1) {
        out.println(); // Add spacing between sections
      }
    }
  }

  /**
   * Print FileInfo for each section in a multi-tenant HFile v4. Each section is essentially an
   * HFile v3 with its own FileInfo block.
   * @param mtReader the multi-tenant reader to get section information from
   */
  private void printSectionFileInfo(AbstractMultiTenantReader mtReader) {
    byte[][] tenantSectionIds = mtReader.getAllTenantSectionIds();

    if (tenantSectionIds == null || tenantSectionIds.length == 0) {
      out.println("Section-level FileInfo: No sections found");
      return;
    }

    out.println("Section-level FileInfo:");

    for (int i = 0; i < tenantSectionIds.length; i++) {
      byte[] sectionId = tenantSectionIds[i];
      out.println(
        FOUR_SPACES + "--- Section " + i + ": " + Bytes.toStringBinary(sectionId) + " ---");

      try (AbstractMultiTenantReader.SectionReaderLease lease =
        mtReader.getSectionReader(sectionId)) {
        if (lease != null) {
          HFileReaderImpl sectionHFileReader = lease.getReader();
          if (sectionHFileReader != null) {
            Map<byte[], byte[]> sectionFileInfo = sectionHFileReader.getHFileInfo();
            if (sectionFileInfo != null && !sectionFileInfo.isEmpty()) {
              out.println(FOUR_SPACES + FOUR_SPACES + "Section FileInfo:");
              for (Map.Entry<byte[], byte[]> e : sectionFileInfo.entrySet()) {
                out.print(
                  FOUR_SPACES + FOUR_SPACES + FOUR_SPACES + Bytes.toString(e.getKey()) + " = ");
                if (
                  Bytes.equals(e.getKey(), HStoreFile.MAX_SEQ_ID_KEY)
                    || Bytes.equals(e.getKey(), HStoreFile.DELETE_FAMILY_COUNT)
                    || Bytes.equals(e.getKey(), HStoreFile.EARLIEST_PUT_TS)
                    || Bytes.equals(e.getKey(), HFileWriterImpl.MAX_MEMSTORE_TS_KEY)
                    || Bytes.equals(e.getKey(), HFileInfo.CREATE_TIME_TS)
                    || Bytes.equals(e.getKey(), HStoreFile.BULKLOAD_TIME_KEY)
                ) {
                  out.println(Bytes.toLong(e.getValue()));
                } else if (Bytes.equals(e.getKey(), HStoreFile.TIMERANGE_KEY)) {
                  TimeRangeTracker timeRangeTracker = TimeRangeTracker.parseFrom(e.getValue());
                  out.println(timeRangeTracker.getMin() + "...." + timeRangeTracker.getMax());
                } else if (
                  Bytes.equals(e.getKey(), HFileInfo.AVG_KEY_LEN)
                    || Bytes.equals(e.getKey(), HFileInfo.AVG_VALUE_LEN)
                    || Bytes.equals(e.getKey(), HFileWriterImpl.KEY_VALUE_VERSION)
                    || Bytes.equals(e.getKey(), HFileInfo.MAX_TAGS_LEN)
                ) {
                  out.println(Bytes.toInt(e.getValue()));
                } else if (
                  Bytes.equals(e.getKey(), HStoreFile.MAJOR_COMPACTION_KEY)
                    || Bytes.equals(e.getKey(), HFileInfo.TAGS_COMPRESSED)
                    || Bytes.equals(e.getKey(), HStoreFile.EXCLUDE_FROM_MINOR_COMPACTION_KEY)
                    || Bytes.equals(e.getKey(), HStoreFile.HISTORICAL_KEY)
                ) {
                  out.println(Bytes.toBoolean(e.getValue()));
                } else if (Bytes.equals(e.getKey(), HFileInfo.LASTKEY)) {
                  out.println(new KeyValue.KeyOnlyKeyValue(e.getValue()).toString());
                } else {
                  out.println(Bytes.toStringBinary(e.getValue()));
                }
              }
            } else {
              out.println(FOUR_SPACES + FOUR_SPACES + "Section FileInfo not available or empty");
            }
          } else {
            out.println(FOUR_SPACES + FOUR_SPACES + "Section reader not initialized");
          }
        } else {
          out.println(FOUR_SPACES + FOUR_SPACES + "Could not create section reader");
        }
      } catch (IllegalArgumentException | IOException sectionException) {
        out.println(FOUR_SPACES + FOUR_SPACES + "Error accessing section FileInfo: "
          + sectionException.getMessage());
      }

      if (i < tenantSectionIds.length - 1) {
        out.println(); // Add spacing between sections
      }
    }
  }

  /**
   * Print bloom filter information for each section in a multi-tenant HFile v4.
   * @param mtReader the multi-tenant reader to get section information from
   */
  private void printSectionBloomFilters(AbstractMultiTenantReader mtReader) {
    byte[][] tenantSectionIds = mtReader.getAllTenantSectionIds();

    if (tenantSectionIds == null || tenantSectionIds.length == 0) {
      out.println("Section-level Bloom filters: No sections found");
      return;
    }

    out.println("Section-level Bloom filters:");

    for (int i = 0; i < tenantSectionIds.length; i++) {
      byte[] sectionId = tenantSectionIds[i];
      out.println(
        FOUR_SPACES + "--- Section " + i + ": " + Bytes.toStringBinary(sectionId) + " ---");

      try (AbstractMultiTenantReader.SectionReaderLease lease =
        mtReader.getSectionReader(sectionId)) {
        if (lease != null) {
          HFileReaderImpl sectionHFileReader = lease.getReader();
          if (sectionHFileReader != null) {

            // Print general bloom filter for this section
            DataInput bloomMeta = sectionHFileReader.getGeneralBloomFilterMetadata();
            BloomFilter bloomFilter = null;
            if (bloomMeta != null) {
              bloomFilter = BloomFilterFactory.createFromMeta(bloomMeta, sectionHFileReader);
            }

            out.println(FOUR_SPACES + FOUR_SPACES + FOUR_SPACES + "General Bloom filter:");
            if (bloomFilter != null) {
              String bloomDetails = bloomFilter.toString().replaceAll(
                BloomFilterUtil.STATS_RECORD_SEP, "\n" + FOUR_SPACES + FOUR_SPACES + FOUR_SPACES);
              out.println(FOUR_SPACES + FOUR_SPACES + FOUR_SPACES + bloomDetails);
            } else {
              out.println(FOUR_SPACES + FOUR_SPACES + FOUR_SPACES + "Not present");
            }

            // Print delete bloom filter for this section
            bloomMeta = sectionHFileReader.getDeleteBloomFilterMetadata();
            bloomFilter = null;
            if (bloomMeta != null) {
              bloomFilter = BloomFilterFactory.createFromMeta(bloomMeta, sectionHFileReader);
            }

            out.println(FOUR_SPACES + FOUR_SPACES + "Delete Family Bloom filter:");
            if (bloomFilter != null) {
              String bloomDetails = bloomFilter.toString().replaceAll(
                BloomFilterUtil.STATS_RECORD_SEP, "\n" + FOUR_SPACES + FOUR_SPACES + FOUR_SPACES);
              out.println(FOUR_SPACES + FOUR_SPACES + FOUR_SPACES + bloomDetails);
            } else {
              out.println(FOUR_SPACES + FOUR_SPACES + FOUR_SPACES + "Not present");
            }

          } else {
            out.println(FOUR_SPACES + FOUR_SPACES + "Section reader not initialized");
          }
        } else {
          out.println(FOUR_SPACES + FOUR_SPACES + "Could not create section reader");
        }
      } catch (IllegalArgumentException | IOException sectionException) {
        out.println(FOUR_SPACES + FOUR_SPACES + "Error accessing section bloom filters: "
          + sectionException.getMessage());
      }

      if (i < tenantSectionIds.length - 1) {
        out.println(); // Add spacing between sections
      }
    }
  }

  private void printV4SpecificTrailerInfo(FixedFileTrailer trailer) {
    out.println("HFile v4 Specific Information:");
    try {
      // Access v4-specific trailer fields directly (no reflection needed)
      boolean isMultiTenant = trailer.isMultiTenant();
      out.println(FOUR_SPACES + "Multi-tenant enabled: " + isMultiTenant);

      if (isMultiTenant) {
        int tenantPrefixLength = trailer.getTenantPrefixLength();
        out.println(FOUR_SPACES + "Tenant prefix length: " + tenantPrefixLength);
      }

    } catch (Exception e) {
      out.println(
        FOUR_SPACES + "Unable to retrieve v4-specific trailer information: " + e.getMessage());
    }
  }

  private void printTenantInformation(HFile.Reader reader) throws IOException {
    out.println("Tenant Information:");

    FixedFileTrailer trailer = reader.getTrailer();
    if (trailer.getMajorVersion() == HFile.MIN_FORMAT_VERSION_WITH_MULTI_TENANT) {
      // Check if this is actually a multi-tenant file in the trailer
      try {
        // Access multi-tenant specific fields directly from trailer (no reflection needed)
        boolean isMultiTenant = trailer.isMultiTenant();

        if (isMultiTenant) {
          out.println(FOUR_SPACES + "Multi-tenant: true");

          int tenantPrefixLength = trailer.getTenantPrefixLength();
          out.println(FOUR_SPACES + "Tenant prefix length: " + tenantPrefixLength);

          // Try to access tenant section information if available
          if (reader instanceof AbstractMultiTenantReader) {
            AbstractMultiTenantReader mtReader = (AbstractMultiTenantReader) reader;
            out.println(FOUR_SPACES + "Reader type: " + reader.getClass().getSimpleName());
            try {
              byte[][] tenantSectionIds = mtReader.getAllTenantSectionIds();
              if (tenantSectionIds != null && tenantSectionIds.length > 0) {
                out.println(FOUR_SPACES + "Number of tenant sections: " + tenantSectionIds.length);
                for (int i = 0; i < Math.min(tenantSectionIds.length, 10); i++) {
                  out.println(FOUR_SPACES + "Tenant section " + i + ": "
                    + Bytes.toStringBinary(tenantSectionIds[i]));
                }
                if (tenantSectionIds.length > 10) {
                  out.println(
                    FOUR_SPACES + "... and " + (tenantSectionIds.length - 10) + " more sections");
                }
              }
            } catch (Exception e) {
              out.println(
                FOUR_SPACES + "Unable to retrieve tenant section information: " + e.getMessage());
            }
          }
        } else {
          out.println(FOUR_SPACES + "Multi-tenant: false (HFile v4 format but single tenant)");
        }
      } catch (Exception e) {
        out.println(FOUR_SPACES + "Unable to retrieve multi-tenant information: " + e.getMessage());
      }
    } else {
      out.println(
        FOUR_SPACES + "Not a multi-tenant HFile (version " + trailer.getMajorVersion() + ")");
    }
  }

  private void printBlockIndex(HFile.Reader reader, boolean isV4) throws IOException {
    out.println("Block Index:");

    if (isV4) {
      // For v4 files, show block index for each tenant section
      if (reader instanceof AbstractMultiTenantReader) {
        AbstractMultiTenantReader mtReader = (AbstractMultiTenantReader) reader;
        byte[][] tenantSectionIds = mtReader.getAllTenantSectionIds();

        if (tenantSectionIds != null && tenantSectionIds.length > 0) {
          out.println(
            FOUR_SPACES + "HFile v4 contains " + tenantSectionIds.length + " tenant sections:");

          for (int i = 0; i < tenantSectionIds.length; i++) {
            byte[] sectionId = tenantSectionIds[i];
            out.println(FOUR_SPACES + "--- Tenant Section " + i + ": "
              + Bytes.toStringBinary(sectionId) + " ---");

            try {
              // Always show basic section information first
              java.util.Map<String, Object> sectionInfo = mtReader.getSectionInfo(sectionId);
              if (sectionInfo != null && (Boolean) sectionInfo.get("exists")) {
                out.println(
                  FOUR_SPACES + FOUR_SPACES + "Section offset: " + sectionInfo.get("offset"));
                out.println(FOUR_SPACES + FOUR_SPACES + "Section size: " + sectionInfo.get("size")
                  + " bytes");
              } else {
                out.println(FOUR_SPACES + FOUR_SPACES + "Section metadata not available");
                continue;
              }

              // Get the actual block index from the section reader
              try (AbstractMultiTenantReader.SectionReaderLease lease =
                mtReader.getSectionReader(sectionId)) {
                if (lease != null) {
                  HFileReaderImpl sectionHFileReader = lease.getReader();
                  if (sectionHFileReader != null) {
                    HFileBlockIndex.CellBasedKeyBlockIndexReader indexReader =
                      sectionHFileReader.getDataBlockIndexReader();
                    if (indexReader != null) {
                      out.println(FOUR_SPACES + FOUR_SPACES + "Block index details:");
                      String indexDetails = indexReader.toString();
                      // Indent the index details for better readability
                      String[] lines = indexDetails.split("\n");
                      for (String line : lines) {
                        out.println(FOUR_SPACES + FOUR_SPACES + FOUR_SPACES + line);
                      }
                    } else {
                      out.println(
                        FOUR_SPACES + FOUR_SPACES + "Block index not available for this section");
                    }
                  } else {
                    out.println(FOUR_SPACES + FOUR_SPACES + "Section reader not initialized");
                  }
                } else {
                  out.println(FOUR_SPACES + FOUR_SPACES + "Could not create section reader");
                }
              } catch (Exception sectionException) {
                out.println(FOUR_SPACES + FOUR_SPACES + "Error accessing section reader: "
                  + sectionException.getMessage());
              }

            } catch (Exception e) {
              out.println(
                FOUR_SPACES + FOUR_SPACES + "Error reading section block index: " + e.getMessage());
            }

            if (i < tenantSectionIds.length - 1) {
              out.println(); // Add spacing between sections
            }
          }
        } else {
          out.println(FOUR_SPACES + "No tenant sections found in HFile v4");
        }
      } else {
        out.println(FOUR_SPACES + "Reader is not a multi-tenant reader for v4 file");
      }
    } else {
      // For v2/v3 files, use standard approach
      HFileBlockIndex.CellBasedKeyBlockIndexReader indexReader = reader.getDataBlockIndexReader();
      if (indexReader != null) {
        out.println(indexReader);
      } else {
        out.println(FOUR_SPACES + "Block index not available");
      }
    }
  }

  private void printBlockHeaders(HFile.Reader reader, Path file, FileSystem fs, boolean isV4)
    throws IOException {
    out.println("Block Headers:");

    if (isV4) {
      // For v4 files, show block headers for each tenant section
      if (reader instanceof AbstractMultiTenantReader) {
        AbstractMultiTenantReader mtReader = (AbstractMultiTenantReader) reader;
        byte[][] tenantSectionIds = mtReader.getAllTenantSectionIds();

        if (tenantSectionIds != null && tenantSectionIds.length > 0) {
          out.println(
            FOUR_SPACES + "HFile v4 contains " + tenantSectionIds.length + " tenant sections:");

          for (int i = 0; i < tenantSectionIds.length; i++) {
            byte[] sectionId = tenantSectionIds[i];
            out.println(FOUR_SPACES + "--- Tenant Section " + i + ": "
              + Bytes.toStringBinary(sectionId) + " ---");

            try {
              // Always show basic section information first
              java.util.Map<String, Object> sectionInfo = mtReader.getSectionInfo(sectionId);
              if (sectionInfo != null && (Boolean) sectionInfo.get("exists")) {
                out.println(
                  FOUR_SPACES + FOUR_SPACES + "Section offset: " + sectionInfo.get("offset"));
                out.println(FOUR_SPACES + FOUR_SPACES + "Section size: " + sectionInfo.get("size")
                  + " bytes");
              } else {
                out.println(FOUR_SPACES + FOUR_SPACES + "Section metadata not available");
                continue;
              }

              // Get the actual block headers from the section reader
              try (AbstractMultiTenantReader.SectionReaderLease lease =
                mtReader.getSectionReader(sectionId)) {
                if (lease != null) {
                  HFileReaderImpl sectionHFileReader = lease.getReader();
                  if (sectionHFileReader != null) {
                    out.println(FOUR_SPACES + FOUR_SPACES + "Block headers:");
                    // Create a section-specific path for block header reading
                    // Use the original file path since block reading handles section offsets
                    // internally
                    printSectionBlockHeaders(sectionHFileReader, file, fs,
                      FOUR_SPACES + FOUR_SPACES + FOUR_SPACES);
                  } else {
                    out.println(FOUR_SPACES + FOUR_SPACES + "Section reader not initialized");
                  }
                } else {
                  out.println(FOUR_SPACES + FOUR_SPACES + "Could not create section reader");
                }
              } catch (Exception sectionException) {
                out.println(FOUR_SPACES + FOUR_SPACES + "Error accessing section reader: "
                  + sectionException.getMessage());
              }

            } catch (Exception e) {
              out.println(FOUR_SPACES + FOUR_SPACES + "Error reading section block headers: "
                + e.getMessage());
            }

            if (i < tenantSectionIds.length - 1) {
              out.println(); // Add spacing between sections
            }
          }
        } else {
          out.println(FOUR_SPACES + "No tenant sections found in HFile v4");
        }
      } else {
        out.println(FOUR_SPACES + "Reader is not a multi-tenant reader for v4 file");
      }
    } else {
      // For v2/v3 files, use standard approach
      printStandardBlockHeaders(reader, file, fs);
    }
  }

  /**
   * Print block headers using the standard approach for v2/v3 files.
   */
  private void printStandardBlockHeaders(HFile.Reader reader, Path file, FileSystem fs)
    throws IOException {
    try {
      FSDataInputStreamWrapper fsdis = new FSDataInputStreamWrapper(fs, file);
      long fileSize = fs.getFileStatus(file).getLen();
      FixedFileTrailer trailer = FixedFileTrailer.readFromStream(fsdis.getStream(false), fileSize);
      long offset = trailer.getFirstDataBlockOffset();
      long max = trailer.getLastDataBlockOffset();

      if (offset > max || offset < 0 || max < 0) {
        out.println(FOUR_SPACES + "Invalid block offset range: " + offset + " to " + max);
        return;
      }

      int blockCount = 0;
      final int effectiveLimit = getEffectiveBlockLimit();

      HFileBlock block;
      while (offset <= max && blockCount < effectiveLimit) {
        try {
          block = reader.readBlock(offset, -1, /* cacheBlock */ false, /* pread */ false,
            /* isCompaction */ false, /* updateCacheMetrics */ false, null, null);

          if (block == null) {
            out.println(FOUR_SPACES + "Warning: null block at offset " + offset);
            break;
          }

          out.println(block);
          offset += block.getOnDiskSizeWithHeader();
          blockCount++;

        } catch (Exception e) {
          out.println(
            FOUR_SPACES + "Error reading block at offset " + offset + ": " + e.getMessage());
          // For non-v4 files, try to continue with next logical offset
          offset += 64; // Skip ahead and try again
          if (offset > max) break;
        }
      }

      if (blockCount >= effectiveLimit) {
        out.println(FOUR_SPACES + "... (truncated after " + effectiveLimit + " blocks)");
      }

      out.println(FOUR_SPACES + "Total blocks shown: " + blockCount);

    } catch (Exception e) {
      out.println(FOUR_SPACES + "Unable to read block headers: " + e.getMessage());
    }
  }

  /**
   * Print block headers for a specific section reader with custom indentation.
   * @param sectionReader the section reader to get block headers from
   * @param file          the original file path (for context)
   * @param fs            the file system
   * @param indent        the indentation string to use for output
   * @throws IOException if an error occurs reading block headers
   */
  private void printSectionBlockHeaders(HFileReaderImpl sectionReader, Path file, FileSystem fs,
    String indent) throws IOException {
    try {
      FixedFileTrailer sectionTrailer = sectionReader.getTrailer();
      long firstDataBlockOffset = sectionTrailer.getFirstDataBlockOffset();
      long lastDataBlockOffset = sectionTrailer.getLastDataBlockOffset();

      if (firstDataBlockOffset == -1 || lastDataBlockOffset == -1) {
        out.println(indent + "No data blocks in this section");
        return;
      }

      if (
        firstDataBlockOffset > lastDataBlockOffset || firstDataBlockOffset < 0
          || lastDataBlockOffset < 0
      ) {
        out.println(indent + "Invalid block offset range: " + firstDataBlockOffset + " to "
          + lastDataBlockOffset);
        return;
      }

      int blockCount = 0;
      final int effectiveLimit = getEffectiveBlockLimit();
      long offset = firstDataBlockOffset;

      while (offset <= lastDataBlockOffset && blockCount < effectiveLimit) {
        try {
          HFileBlock block =
            sectionReader.readBlock(offset, -1, /* cacheBlock */ false, /* pread */ false,
              /* isCompaction */ false, /* updateCacheMetrics */ false, null, null);

          if (block == null) {
            out.println(indent + "Warning: null block at offset " + offset);
            break;
          }

          // Print block header with proper indentation
          String blockHeader = block.toString();
          String[] lines = blockHeader.split("\n");
          for (String line : lines) {
            out.println(indent + line);
          }

          offset += block.getOnDiskSizeWithHeader();
          blockCount++;

        } catch (Exception e) {
          out.println(indent + "Error reading block at offset " + offset + ": " + e.getMessage());
          // Try to continue with next logical offset
          offset += 64; // Skip ahead and try again
          if (offset > lastDataBlockOffset) break;
        }
      }

      if (blockCount >= effectiveLimit) {
        out.println(indent + "... (truncated after " + effectiveLimit + " blocks)");
      }

      out.println(indent + "Total blocks shown: " + blockCount);

    } catch (Exception e) {
      out.println(indent + "Unable to read section block headers: " + e.getMessage());
    }
  }

  // Default reservoir is exponentially decaying, but we're doing a point-in-time analysis
  // of a store file. It doesn't make sense to prefer keys later in the store file.
  private static final MetricRegistry.MetricSupplier<Histogram> UNIFORM_RESERVOIR =
    () -> new Histogram(new UniformReservoir());

  // Useful ranges for viewing distribution of small to large keys, values, and rows.
  // we only print ranges which actually have values, so more here doesn't add much overhead
  private static final long[] RANGES = new long[] { 1, 3, 10, 50, 100, 500, 1_000, 5_000, 10_000,
    50_000, 100_000, 500_000, 750_000, 1_000_000, 5_000_000, 10_000_000, 50_000_000, 100_000_000 };

  /**
   * Holds a Histogram and supporting min/max and range buckets for analyzing distribution of key
   * bytes, value bytes, row bytes, and row columns. Supports adding values, getting the histogram,
   * and getting counts per range.
   */
  static class KeyValueStats {
    private final Histogram histogram;
    private final String name;
    private long max = Long.MIN_VALUE;
    private long min = Long.MAX_VALUE;
    private boolean collectRanges = false;
    private final LongAdder[] rangeCounts;

    KeyValueStats(MetricRegistry metricRegistry, String statName) {
      this.histogram =
        metricRegistry.histogram(name(HFilePrettyPrinter.class, statName), UNIFORM_RESERVOIR);
      this.name = statName;
      this.rangeCounts = new LongAdder[RANGES.length];
      for (int i = 0; i < rangeCounts.length; i++) {
        rangeCounts[i] = new LongAdder();
      }
    }

    void update(long value, boolean collectRanges) {
      histogram.update(value);
      min = Math.min(value, min);
      max = Math.max(value, max);

      if (collectRanges) {
        this.collectRanges = true;
        int result = Arrays.binarySearch(RANGES, value);
        int idx = result >= 0 ? result : Math.abs(result) - 1;
        rangeCounts[idx].increment();
      }
    }

    Histogram getHistogram() {
      return histogram;
    }

    String getName() {
      return name;
    }

    long getMax() {
      return max;
    }

    long getMin() {
      return min;
    }

    long[] getRanges() {
      return RANGES;
    }

    long getCountAtOrBelow(long range) {
      long count = 0;
      for (int i = 0; i < RANGES.length; i++) {
        if (RANGES[i] <= range) {
          count += rangeCounts[i].sum();
        } else {
          break;
        }
      }
      return count;
    }

    boolean hasRangeCounts() {
      return collectRanges;
    }
  }

  private static class KeyValueStatsCollector {
    private final MetricRegistry metricsRegistry = new MetricRegistry();
    private final ByteArrayOutputStream metricsOutput = new ByteArrayOutputStream();

    KeyValueStats keyLen = new KeyValueStats(metricsRegistry, "Key length");
    KeyValueStats valLen = new KeyValueStats(metricsRegistry, "Val length");
    KeyValueStats rowSizeBytes = new KeyValueStats(metricsRegistry, "Row size (bytes)");
    KeyValueStats rowSizeCols = new KeyValueStats(metricsRegistry, "Row size (columns)");

    private final SimpleReporter simpleReporter =
      SimpleReporter.newBuilder().outputTo(new PrintStream(metricsOutput)).addStats(keyLen)
        .addStats(valLen).addStats(rowSizeBytes).addStats(rowSizeCols).build();

    long curRowBytes = 0;
    long curRowCols = 0;

    byte[] biggestRow = null;

    private Cell prevCell = null;
    private long maxRowBytes = 0;
    private long curRowKeyLength;

    public void collect(Cell cell, boolean printStatRanges) {
      valLen.update(cell.getValueLength(), printStatRanges);
      if (prevCell != null && CellComparator.getInstance().compareRows(prevCell, cell) != 0) {
        // new row
        collectRow(printStatRanges);
      }
      curRowBytes += cell.getSerializedSize();
      curRowKeyLength = KeyValueUtil.keyLength(cell);
      curRowCols++;
      prevCell = cell;
    }

    private void collectRow(boolean printStatRanges) {
      rowSizeBytes.update(curRowBytes, printStatRanges);
      rowSizeCols.update(curRowCols, printStatRanges);
      keyLen.update(curRowKeyLength, printStatRanges);

      if (curRowBytes > maxRowBytes && prevCell != null) {
        biggestRow = CellUtil.cloneRow(prevCell);
        maxRowBytes = curRowBytes;
      }

      curRowBytes = 0;
      curRowCols = 0;
    }

    public void finish(boolean printStatRanges) {
      if (curRowCols > 0) {
        collectRow(printStatRanges);
      }
    }

    @Override
    public String toString() {
      if (prevCell == null) return "no data available for statistics";

      // Dump the metrics to the output stream
      simpleReporter.report();

      return metricsOutput.toString() + "Key of biggest row: " + Bytes.toStringBinary(biggestRow);
    }
  }

  /**
   * Simple reporter which collects registered histograms for printing to an output stream in
   * {@link #report()}.
   */
  private static final class SimpleReporter {
    /**
     * Returns a new {@link Builder} for {@link SimpleReporter}.
     * @return a {@link Builder} instance for a {@link SimpleReporter}
     */
    public static Builder newBuilder() {
      return new Builder();
    }

    /**
     * A builder for {@link SimpleReporter} instances. Defaults to using the default locale and time
     * zone, writing to {@code System.out}.
     */
    public static class Builder {
      private final List<KeyValueStats> stats = new ArrayList<>();
      private PrintStream output;
      private Locale locale;
      private TimeZone timeZone;

      private Builder() {
        this.output = System.out;
        this.locale = Locale.getDefault();
        this.timeZone = TimeZone.getDefault();
      }

      /**
       * Write to the given {@link PrintStream}.
       * @param output a {@link PrintStream} instance.
       * @return {@code this}
       */
      public Builder outputTo(PrintStream output) {
        this.output = output;
        return this;
      }

      /**
       * Add the given {@link KeyValueStats} to be reported
       * @param stat the stat to be reported
       * @return {@code this}
       */
      public Builder addStats(KeyValueStats stat) {
        this.stats.add(stat);
        return this;
      }

      /**
       * Builds a {@link ConsoleReporter} with the given properties.
       * @return a {@link ConsoleReporter}
       */
      public SimpleReporter build() {
        return new SimpleReporter(output, stats, locale, timeZone);
      }
    }

    private final PrintStream output;
    private final List<KeyValueStats> stats;
    private final Locale locale;
    private final DateFormat dateFormat;

    private SimpleReporter(PrintStream output, List<KeyValueStats> stats, Locale locale,
      TimeZone timeZone) {
      this.output = output;
      this.stats = stats;
      this.locale = locale;
      this.dateFormat = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.MEDIUM, locale);
      dateFormat.setTimeZone(timeZone);
    }

    public void report() {
      // we know we only have histograms
      if (!stats.isEmpty()) {
        for (KeyValueStats stat : stats) {
          output.print("   " + stat.getName());
          output.println(':');
          printHistogram(stat);
        }
        output.println();
      }

      output.println();
      output.flush();
    }

    private void printHistogram(KeyValueStats stats) {
      Histogram histogram = stats.getHistogram();
      Snapshot snapshot = histogram.getSnapshot();

      output.printf(locale, "               min = %d%n", stats.getMin());
      output.printf(locale, "               max = %d%n", stats.getMax());
      output.printf(locale, "              mean = %2.2f%n", snapshot.getMean());
      output.printf(locale, "            stddev = %2.2f%n", snapshot.getStdDev());
      output.printf(locale, "            median = %2.2f%n", snapshot.getMedian());
      output.printf(locale, "              75%% <= %2.2f%n", snapshot.get75thPercentile());
      output.printf(locale, "              95%% <= %2.2f%n", snapshot.get95thPercentile());
      output.printf(locale, "              98%% <= %2.2f%n", snapshot.get98thPercentile());
      output.printf(locale, "              99%% <= %2.2f%n", snapshot.get99thPercentile());
      output.printf(locale, "            99.9%% <= %2.2f%n", snapshot.get999thPercentile());
      output.printf(locale, "             count = %d%n", histogram.getCount());

      // if printStatRanges was enabled with -d arg, below we'll create an approximate histogram
      // of counts based on the configured ranges in RANGES. Each range of sizes (i.e. <= 50, <=
      // 100, etc) will have a count printed if any values were seen in that range. If no values
      // were seen for a range, that range will be excluded to keep the output small.
      if (stats.hasRangeCounts()) {
        output.printf(locale, "           (range <= count):%n");
        long lastVal = 0;
        long lastRange = 0;
        for (long range : stats.getRanges()) {
          long val = stats.getCountAtOrBelow(range);
          if (val - lastVal > 0) {
            // print the last zero value before this one, to give context
            if (lastVal == 0 && lastRange != 0) {
              printRangeCount(lastRange, lastVal);
            }
            printRangeCount(range, val - lastVal);
          }
          lastVal = val;
          lastRange = range;
        }
        if (histogram.getCount() - lastVal > 0) {
          // print any remaining that might have been outside our buckets
          printRangeCount(Long.MAX_VALUE, histogram.getCount() - lastVal);
        }
      }
    }

    private void printRangeCount(long range, long countAtOrBelow) {
      String rangeString = range == Long.MAX_VALUE ? "inf" : Long.toString(range);
      output.printf(locale, "%17s <= %d%n", rangeString, countAtOrBelow);
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
