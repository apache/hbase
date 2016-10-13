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

package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException;
import org.apache.hadoop.hbase.fs.MasterStorage;
import org.apache.hadoop.hbase.fs.StorageIdentifier;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.hbase.fs.RegionStorage;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionContext;
import org.apache.hadoop.hbase.regionserver.throttle.NoLimitThroughputController;
import org.apache.hadoop.hbase.util.Bytes;

/*
 * The CompactionTool allows to execute a compaction specifying a:
 * <ul>
 *  <li>table folder (all regions and families will be compacted)
 *  <li>region folder (all families in the region will be compacted)
 *  <li>family folder (the store files will be compacted)
 * </ul>
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
public class CompactionTool extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(CompactionTool.class);

  private final static String CONF_TMP_DIR = "hbase.tmp.dir";
  private final static String CONF_COMPACT_ONCE = "hbase.compactiontool.compact.once";
  private final static String CONF_COMPACT_MAJOR = "hbase.compactiontool.compact.major";
  private final static String CONF_DELETE_COMPACTED = "hbase.compactiontool.delete";
  private final static String CONF_COMPLETE_COMPACTION = "hbase.hstore.compaction.complete";

  public final static char COMPACTION_ENTRY_DELIMITER = '/';

  /**
   * All input entries are converted in table{@value #COMPACTION_ENTRY_DELIMITER}region{@value
   * #COMPACTION_ENTRY_DELIMITER}columnfamily format (e.g. table1{@value
   * #COMPACTION_ENTRY_DELIMITER}b441cd3b56238c01a0ee4a445e8544c3{@value
   * #COMPACTION_ENTRY_DELIMITER}cf1
   */
  Set<String> toCompactEntries = new HashSet<>();

  /**
   * Class responsible to execute the Compaction on the specified path.
   * The path can be a table, region or family directory.
   */
  private static class CompactionWorker {
    private final boolean keepCompactedFiles;
    private final boolean deleteCompacted;
    private final Configuration conf;
    private final MasterStorage<? extends StorageIdentifier> ms;

    public CompactionWorker(final MasterStorage<? extends StorageIdentifier> ms,
                            final Configuration conf) {
      this.conf = conf;
      this.keepCompactedFiles = !conf.getBoolean(CONF_COMPLETE_COMPACTION, true);
      this.deleteCompacted = conf.getBoolean(CONF_DELETE_COMPACTED, false);
      this.ms = ms;
    }

    /**
     * Execute the compaction on the specified table{@value
     * #COMPACTION_ENTRY_DELIMITER}region{@value #COMPACTION_ENTRY_DELIMITER}cf.
     *
     * @param entry "table{@value #COMPACTION_ENTRY_DELIMITER}region{@value
     * #COMPACTION_ENTRY_DELIMITER}cf" entry on which to run compaction.
     * @param compactOnce Execute just a single step of compaction.
     * @param major Request major compaction.
     */
    public void compact(final String entry, final boolean compactOnce, final boolean major)
        throws IOException {
      // Validations
      String[] details = entry.split(String.valueOf(COMPACTION_ENTRY_DELIMITER));
      if (details == null || details.length != 3) {
        throw new IOException("Entry '" + entry + "' does not have valid details: table" +
            COMPACTION_ENTRY_DELIMITER + "region" + COMPACTION_ENTRY_DELIMITER + "columnfamily.");
      }

      compactStoreFiles(TableName.valueOf(details[0]), details[1], details[2], compactOnce, major);
    }

    /**
     * Execute the actual compaction job.
     * If the compact once flag is not specified, execute the compaction until
     * no more compactions are needed. Uses the Configuration settings provided.
     */
    private void compactStoreFiles(TableName tableName, String regionName, String familyName,
                                   final boolean compactOnce, final boolean major)
        throws IOException {
      HStore store = getStore(conf, ms, tableName, regionName, familyName);

      LOG.info("Compact table='" + tableName + "' region='" + regionName + "' family='" +
          familyName + "'");

      if (major) {
        store.triggerMajorCompaction();
      }
      do {
        CompactionContext compaction = store.requestCompaction(Store.PRIORITY_USER, null);
        if (compaction == null) break;
        List<StoreFile> storeFiles =
            store.compact(compaction, NoLimitThroughputController.INSTANCE);
        if (storeFiles != null && !storeFiles.isEmpty()) {
          if (keepCompactedFiles && deleteCompacted) {
            // TODO currently removeFiles archives the files, check if we can delete them
            RegionStorage rs = store.getRegionStorage();
            rs.removeStoreFiles(store.getColumnFamilyName(), storeFiles);
          }
        }
      } while (store.needsCompaction() && !compactOnce);
    }

    private static HStore getStore(final Configuration conf,
        final MasterStorage<? extends StorageIdentifier> ms, TableName tableName, String regionName,
        String familyName) throws IOException {
      HTableDescriptor htd = ms.getTableDescriptor(tableName);
      HRegionInfo hri = ms.getRegion(tableName, regionName);
      RegionStorage rs = ms.getRegionStorage(hri);
      HRegion region = new HRegion(rs, htd, null, null);
      return new HStore(region, htd.getFamily(Bytes.toBytes(familyName)), conf);
    }
  }

  /**
   * Execute compaction, from this client, one path at the time.
   */
  private int doClient(final MasterStorage<? extends StorageIdentifier> ms,
                       final boolean compactOnce, final boolean major) throws IOException {
    CompactionWorker worker = new CompactionWorker(ms, getConf());
    for (String entry: toCompactEntries) {
      worker.compact(entry, compactOnce, major);
    }
    return 0;
  }

  /**
   * Validates the input args and polulates table{@value
   * #COMPACTION_ENTRY_DELIMITER}region{@value #COMPACTION_ENTRY_DELIMITER}columnfamily entries
   */
  private void validateInput(MasterStorage<? extends StorageIdentifier> ms, TableName tableName,
      Set<String> regionNames, Set<String> columnFamilies) throws IOException {
    HTableDescriptor htd = ms.getTableDescriptor(tableName);
    Collection<HColumnDescriptor> families = htd.getFamilies();

    if (columnFamilies.isEmpty()) {
      for (HColumnDescriptor column: families) {
        columnFamilies.add(column.getNameAsString());
      }
    } else {
      for (String familyName: columnFamilies) {
        if (!htd.hasFamily(Bytes.toBytes(familyName))) {
          throw new IllegalArgumentIOException("Column family '" + familyName + "' not found!");
        }
      }
    }

    Collection<HRegionInfo> regions = ms.getRegions(tableName);
    if (regionNames.isEmpty()) {
      for(HRegionInfo hri: regions) {
        regionNames.add(hri.getEncodedName());
      }
    } else {
      for (String regionName: regionNames) {
        if (ms.getRegion(tableName, regionName) == null) {
          throw new IllegalArgumentIOException("Region '" + regionName + "' not found!");
        }
      }
    }

    for (String regionName: regionNames) {
      for (String column: columnFamilies) {
        toCompactEntries.add(tableName.toString() + COMPACTION_ENTRY_DELIMITER +
            regionName + COMPACTION_ENTRY_DELIMITER + column);
      }
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    TableName tableName = null;
    Set<String> regionNames = new HashSet<>();
    Set<String> columnFamilies = new HashSet<>();

    boolean compactOnce = false;
    boolean major = false;

    Configuration conf = getConf();
    MasterStorage<? extends StorageIdentifier> ms = MasterStorage.open(getConf(), false);

    try {
      for (int i = 0; i < args.length; ++i) {
        String opt = args[i];
        if (opt.equals("-compactOnce")) {
          compactOnce = true;
        } else if (opt.equals("-major")) {
          major = true;
        } else if (!opt.startsWith("-")) {
          if (tableName != null) {
            printUsage("Incorrect usage! table '" + tableName.getNameAsString() + "' already " +
                "specified.");
            return 1;
          }
          tableName = TableName.valueOf(opt);
        } else {
          if (i == (args.length - 1)) {
            printUsage("Incorrect usage! Option '" + opt + "' needs a value.");
            return 1;
          }

          if (opt.equals("-regions")) {
            Collections.addAll(regionNames, args[++i].split(","));
          } else if (opt.equals("-columnFamilies")) {
            Collections.addAll(columnFamilies, args[++i].split(","));
          } else {
            printUsage();
            return 1;
          }
        }
      }
      validateInput(ms, tableName, regionNames, columnFamilies);
    } catch (Exception e) {
      printUsage(e.getMessage());
      return 1;
    }

    if (toCompactEntries.isEmpty()) {
      printUsage("Nothing to compact!");
      return 1;
    }

    // Execute compaction!
    return doClient(ms, compactOnce, major);
  }

  private void printUsage() {
    printUsage(null);
  }

  private void printUsage(final String message) {
    if (message != null && message.length() > 0) {
      System.err.println(message);
    }
    System.err.println("Usage: java " + this.getClass().getName() + " \\");
    System.err.println("  [-compactOnce] [-major] [-regions r1,r2...] [-columnFamilies cf1,cf2...]"
        + " <table> [-D<property=value>]*");
    System.err.println();
    System.err.println("Options:");
    System.err.println(" compactOnce    Execute just one compaction step. (default: while needed)");
    System.err.println(" major          Trigger major compaction.");
    System.err.println();
    System.err.println(" table          Compact specified table");
    System.err.println(" regions        Compact specified regions of a table");
    System.err.println(" columnFamilies Compact specified column families of regions or a table");
    System.err.println();
    System.err.println("Note: -D properties will be applied to the conf used. ");
    System.err.println("For example: ");
    System.err.println(" To preserve input files, pass -D"+CONF_COMPLETE_COMPACTION+"=false");
    System.err.println(" To stop delete of compacted file, pass -D"+CONF_DELETE_COMPACTED+"=false");
    System.err.println(" To set tmp dir, pass -D"+CONF_TMP_DIR+"=ALTERNATE_DIR");
    System.err.println();
    System.err.println("Examples:");
    System.err.println(" To compact the full 'TestTable':");
    System.err.println(" $ bin/hbase " + this.getClass().getName() + " TestTable");
    System.err.println();
    System.err.println(" To compact a region:");
    System.err.println(" $ bin/hbase " + this.getClass().getName() +
        " -regions 907ecc9390fe0c3fd04e02 TesTable");
    System.err.println();
    System.err.println(" To compact column family 'x' of table 'TestTable' across all regions:");
    System.err.println(" $ bin/hbase " + this.getClass().getName() + " -columnFamilies x " +
        "TestTable");
    System.err.println();
    System.err.println(" To compact column family 'x' of a specific region:");
    System.err.println(" $ bin/hbase " + this.getClass().getName() + " -columnFamilies x -regions "
        + " 907ecc9390fe0c3fd04e02 TestTable");
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(HBaseConfiguration.create(), new CompactionTool(), args));
  }
}
