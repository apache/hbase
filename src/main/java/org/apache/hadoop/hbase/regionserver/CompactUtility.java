/**
 *  Copyright The Apache Software Foundation Licensed to the Apache Software Foundation (ASF)
 *  under one or more contributor license agreements. See the NOTICE file distributed with this work
 *  for additional information regarding copyright ownership. The ASF licenses this file to you under
 *  the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in
 *  writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 *  language governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A compaction Utility for the Store files of a region Server.
 * This utility exposes a hook to compact specific store files.
 * It provides a command line interface to
 * compact files without the requirement of running a region server
 */

public class CompactUtility {

  static final Log LOG = LogFactory.getLog(CompactUtility.class);
  private String table;
  private HColumnDescriptor cFamily;
  private List<Path> filesCompacting;
  private Configuration conf;
  private HRegion hRegion;


  public CompactUtility(String table, HColumnDescriptor cFamily,
                        byte[] startKey, long regionId,
                        List<Path> filesCompacting, Configuration conf)
          throws IOException {
    this.conf = conf;
    this.table = table;
    this.cFamily = cFamily;
    this.filesCompacting = filesCompacting;
    FileSystem fs = FileSystem.get(conf);
    HTableDescriptor htd = new HTableDescriptor(this.table);
    htd.addFamily(this.cFamily);

    HRegionInfo hri = new HRegionInfo(htd, startKey, null, false, regionId);
    Path tableDir = HTableDescriptor.getTableDir(
            new Path(HConstants.HBASE_DIR), this.table.getBytes());
    Path regionDir = HRegion.getRegionDir(tableDir, hri.getEncodedName());
    Path rootDir = fs.makeQualified(
            new Path(conf.get(HConstants.HBASE_DIR)));
    HLog log = new HLog(fs, new Path(regionDir, HConstants.HREGION_LOGDIR_NAME),
            new Path(regionDir, HConstants.HREGION_OLDLOGDIR_NAME), conf, null);
    this.hRegion = HRegion.openHRegion(hri, rootDir, log, this.conf);
  }

  public void compact() throws Exception {
    Store store = hRegion.getStore(this.cFamily.getName());
    List<StoreFile> storeFiles = new ArrayList<StoreFile>(
            store.getStorefiles());
    if (storeFiles == null) {
      throw new Exception("No Store Files To Compact");
    }
    if (filesCompacting != null) {
      for (Iterator<StoreFile> sFile = storeFiles.iterator(); sFile.hasNext(); ) {
        if (!filesCompacting.contains(sFile.next().getPath())) {
          sFile.remove();
        }
      }
    }
    long maxId = StoreFile.getMaxSequenceIdInList(storeFiles, true);
    StoreFile.Writer writer = store.compactStores(storeFiles, false, maxId);
    StoreFile result = store.completeCompaction(storeFiles, writer);
    store.close();
    hRegion.close();
    if (result == null) {
      throw new Exception("Compaction Failed");
    }
  }

  private static void printHelp(Options opt) {
    new HelpFormatter().printHelp(
      "Compact Utility < -t tableName -c ColumnFamilyName -r regionID" +
      " [List of store file Paths]", opt);
  }

  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption("t", "table name", true,
            "Table for which the files hold data");
    options.addOption("c", "Column Family", true,
            "Column Family of the table");
    options.addOption("r", "Region Name", true,
            "Region Name for which we need to compact the files");
    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args);
    if (!cmd.hasOption("t") || !cmd.hasOption("c") || !cmd.hasOption("r")) {
      printHelp(options);
      throw new IOException("Incomplete arguments");
    }
    String table = cmd.getOptionValue("t");
    String regionName = cmd.getOptionValue("r");

    byte[] startKey = null;
    long regionId = -1;
    try {
      // the regionName is of format "<tableName>,<startKey>,<regionId.md5hash>"
      String[] splits = regionName.split(",");
      startKey = Bytes.toBytes(splits[1]);
      regionId = Long.valueOf(splits[2].split("\\.")[0]);
    } catch (Exception e) {
      System.out.println("Invalid region name specified. The expected format" +
        " is " + "<tableName>,<startKey>,<regionId.md5hash>");
      System.exit(-1);
    }

    HColumnDescriptor cFamily = new HColumnDescriptor(cmd.getOptionValue("c"));

    String[] files = cmd.getArgs();
    List<Path> filePaths = new ArrayList<Path>();
    for (String file : files) {
      filePaths.add(new Path(file));
    }
    Configuration conf = HBaseConfiguration.create();
    SchemaMetrics.configureGlobally(conf);
    CompactUtility compactUtility = new CompactUtility(
            table, cFamily, startKey, regionId, filePaths, conf);
    compactUtility.compact();
  }
}

