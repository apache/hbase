/**
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

package org.apache.hadoop.hbase.util;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Preconditions;

/**
 * Utility that can merge any two regions in the same table: adjacent,
 * overlapping or disjoint.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
public class Merge extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(Merge.class);
  private Path rootdir;
  private volatile MetaUtils utils;
  private TableName tableName;               // Name of table
  private volatile byte [] region1;        // Name of region 1
  private volatile byte [] region2;        // Name of region 2
  private volatile HRegionInfo mergeInfo;

  /** default constructor */
  public Merge() {
    super();
  }

  /**
   * @param conf configuration
   */
  public Merge(Configuration conf) {
    this.mergeInfo = null;
    setConf(conf);
  }

  @Override
  public int run(String[] args) throws Exception {
    if (parseArgs(args) != 0) {
      return -1;
    }

    // Verify file system is up.
    FileSystem fs = FileSystem.get(getConf());              // get DFS handle
    LOG.info("Verifying that file system is available...");
    try {
      FSUtils.checkFileSystemAvailable(fs);
    } catch (IOException e) {
      LOG.fatal("File system is not available", e);
      return -1;
    }

    // Verify HBase is down
    LOG.info("Verifying that HBase is not running...");
    try {
      HBaseAdmin.checkHBaseAvailable(getConf());
      LOG.fatal("HBase cluster must be off-line, and is not. Aborting.");
      return -1;
    } catch (ZooKeeperConnectionException zkce) {
      // If no zk, presume no master.
    } catch (MasterNotRunningException e) {
      // Expected. Ignore.
    }

    // Initialize MetaUtils and and get the root of the HBase installation

    this.utils = new MetaUtils(getConf());
    this.rootdir = FSUtils.getRootDir(getConf());
    try {
      mergeTwoRegions();
      return 0;
    } catch (IOException e) {
      LOG.fatal("Merge failed", e);
      return -1;

    } finally {
      if (this.utils != null) {
        this.utils.shutdown();
      }
    }
  }

  /** @return HRegionInfo for merge result */
  HRegionInfo getMergedHRegionInfo() {
    return this.mergeInfo;
  }

  /*
   * Merges two regions from a user table.
   */
  private void mergeTwoRegions() throws IOException {
    LOG.info("Merging regions " + Bytes.toStringBinary(this.region1) + " and " +
        Bytes.toStringBinary(this.region2) + " in table " + this.tableName);
    HRegion meta = this.utils.getMetaRegion();
    Get get = new Get(region1);
    get.addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
    Result result1 =  meta.get(get);
    Preconditions.checkState(!result1.isEmpty(),
        "First region cells can not be null");
    HRegionInfo info1 = HRegionInfo.getHRegionInfo(result1);
    if (info1 == null) {
      throw new NullPointerException("info1 is null using key " +
          Bytes.toStringBinary(region1) + " in " + meta);
    }
    get = new Get(region2);
    get.addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
    Result result2 =  meta.get(get);
    Preconditions.checkState(!result2.isEmpty(),
        "Second region cells can not be null");
    HRegionInfo info2 = HRegionInfo.getHRegionInfo(result2);
    if (info2 == null) {
      throw new NullPointerException("info2 is null using key " + meta);
    }
    HTableDescriptor htd = FSTableDescriptors.getTableDescriptorFromFs(FileSystem.get(getConf()),
      this.rootdir, this.tableName);
    HRegion merged = merge(htd, meta, info1, info2);

    LOG.info("Adding " + merged.getRegionInfo() + " to " +
        meta.getRegionInfo());

    HRegion.addRegionToMETA(meta, merged);
    merged.close();
  }

  /*
   * Actually merge two regions and update their info in the meta region(s)
   * Returns HRegion object for newly merged region
   */
  private HRegion merge(final HTableDescriptor htd, HRegion meta,
                        HRegionInfo info1, HRegionInfo info2)
  throws IOException {
    if (info1 == null) {
      throw new IOException("Could not find " + Bytes.toStringBinary(region1) + " in " +
          Bytes.toStringBinary(meta.getRegionInfo().getRegionName()));
    }
    if (info2 == null) {
      throw new IOException("Could not find " + Bytes.toStringBinary(region2) + " in " +
          Bytes.toStringBinary(meta.getRegionInfo().getRegionName()));
    }
    HRegion merged = null;
    HRegion r1 = HRegion.openHRegion(info1, htd, utils.getLog(info1), getConf());
    try {
      HRegion r2 = HRegion.openHRegion(info2, htd, utils.getLog(info2), getConf());
      try {
        merged = HRegion.merge(r1, r2);
      } finally {
        if (!r2.isClosed()) {
          r2.close();
        }
      }
    } finally {
      if (!r1.isClosed()) {
        r1.close();
      }
    }

    // Remove the old regions from meta.
    // HRegion.merge has already deleted their files

    removeRegionFromMeta(meta, info1);
    removeRegionFromMeta(meta, info2);

    this.mergeInfo = merged.getRegionInfo();
    return merged;
  }

  /*
   * Removes a region's meta information from the passed <code>meta</code>
   * region.
   *
   * @param meta hbase:meta HRegion to be updated
   * @param regioninfo HRegionInfo of region to remove from <code>meta</code>
   *
   * @throws IOException
   */
  private void removeRegionFromMeta(HRegion meta, HRegionInfo regioninfo)
  throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Removing region: " + regioninfo + " from " + meta);
    }

    Delete delete  = new Delete(regioninfo.getRegionName(),
        System.currentTimeMillis());
    meta.delete(delete);
  }

  /*
   * Parse given arguments including generic arguments and assign table name and regions names.
   *
   * @param args the arguments to parse
   *
   * @throws IOException
   */
  private int parseArgs(String[] args) throws IOException {
    GenericOptionsParser parser =
      new GenericOptionsParser(getConf(), args);

    String[] remainingArgs = parser.getRemainingArgs();
    if (remainingArgs.length != 3) {
      usage();
      return -1;
    }
    tableName = TableName.valueOf(remainingArgs[0]);

    region1 = Bytes.toBytesBinary(remainingArgs[1]);
    region2 = Bytes.toBytesBinary(remainingArgs[2]);
    int status = 0;
    if (notInTable(tableName, region1) || notInTable(tableName, region2)) {
      status = -1;
    } else if (Bytes.equals(region1, region2)) {
      LOG.error("Can't merge a region with itself");
      status = -1;
    }
    return status;
  }

  private boolean notInTable(final TableName tn, final byte [] rn) {
    if (WritableComparator.compareBytes(tn.getName(), 0, tn.getName().length,
        rn, 0, tn.getName().length) != 0) {
      LOG.error("Region " + Bytes.toStringBinary(rn) + " does not belong to table " +
        tn);
      return true;
    }
    return false;
  }

  private void usage() {
    System.err
        .println("For hadoop 0.21+, Usage: bin/hbase org.apache.hadoop.hbase.util.Merge "
            + "[-Dfs.defaultFS=hdfs://nn:port] <table-name> <region-1> <region-2>\n");
  }

  public static void main(String[] args) {
    int status;
    try {
      status = ToolRunner.run(HBaseConfiguration.create(), new Merge(), args);
    } catch (Exception e) {
      LOG.error("exiting due to error", e);
      status = -1;
    }
    System.exit(status);
  }
}
