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
package org.apache.hadoop.hbase.regionserver.storefiletracker;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.master.region.MasterRegionFactory;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.StoreUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
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
import org.apache.hbase.thirdparty.org.apache.commons.cli.Options;
import org.apache.hbase.thirdparty.org.apache.commons.cli.ParseException;
import org.apache.hbase.thirdparty.org.apache.commons.cli.PosixParser;

/**
 * Offline, operator-driven CLI to rebuild a corrupted FILE store-file-tracker manifest
 * ({@code .filelist}) for a single store ({@code table + region + family}).
 * <p>
 * This is the sole repair surface of the FSFT manifest-recover design (see
 * {@code dev-support/design-docs/fsft-manifest-recover.md}). It is offline by design: there is no
 * online/in-master path, because nothing in the master can truly fence a RegionServer away from the
 * store directory while a manifest is being rewritten. The operator instead acknowledges, via
 * {@code --region-offline}, that the target region is not hosted anywhere -- a real quiescence
 * guarantee -- before any manifest is written.
 * <p>
 * The recovered manifest is reconstructed purely from the store directory listing. For user-table
 * regions, the tool additionally consults {@code hbase:meta} for split/merge parents and reports
 * whether bringing the region online risks data loss (parents with unarchived HFiles) or not (all
 * parents already archived by the Catalog Janitor). All the logic lives in
 * {@link StoreFileListRecover}; this class is only the CLI surface: argument parsing, safety
 * acknowledgements, and report formatting.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
@InterfaceStability.Evolving
public class StoreFileListRecoverTool extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(StoreFileListRecoverTool.class);

  private final String tableNameOption = "t";
  private final String columnFamilyOption = "cf";
  private final String regionOption = "r";
  private final String dryRunOption = "dry-run";
  private final String forceMetaOption = "force-meta";
  private final String regionOfflineOption = "region-offline";

  private final String cmdString = "sftrecover";

  private final Options options = new Options();

  private String regionName;
  private String columnFamily;
  private TableName targetTableName;
  private boolean dryRun;
  private boolean forceMeta;
  private boolean regionOfflineAck;

  private PrintStream out = System.out;
  private PrintStream err = System.err;

  public StoreFileListRecoverTool() {
    super();
    init();
  }

  public StoreFileListRecoverTool(Configuration conf) {
    super(conf);
    init();
  }

  private void init() {
    options.addOption(new Option(tableNameOption, "table", true,
      "Table of the target store; e.g. test_table or ns:test_table"));
    options.addOption(new Option(columnFamilyOption, "columnfamily", true,
      "Column family of the target store; e.g. f"));
    options.addOption(new Option(regionOption, "region", true,
      "Encoded region name of the target store; e.g. '3d58e9067bf23e378e68c071f3dd39eb'"));
    options.addOption(new Option(null, dryRunOption, false,
      "Print the recover result without writing a new manifest"));
    options.addOption(new Option(null, forceMetaOption, false,
      "Allow recover against the hbase:meta table. Dangerous; only use with master offline."));
    options.addOption(new Option(null, regionOfflineOption, false,
      "Operator acknowledgement that the target region is offline (no master/RS hosting it)."));
  }

  private boolean parseOptions(String[] args) throws ParseException {
    HelpFormatter formatter = new HelpFormatter();
    if (args.length == 0) {
      formatter.printHelp(cmdString, options, true);
      return false;
    }
    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args);

    dryRun = cmd.hasOption(dryRunOption);
    forceMeta = cmd.hasOption(forceMetaOption);
    regionOfflineAck = cmd.hasOption(regionOfflineOption);

    regionName = cmd.getOptionValue(regionOption);
    if (StringUtils.isEmpty(regionName)) {
      err.println("Region name is not specified.");
      formatter.printHelp(cmdString, options, true);
      return false;
    }
    columnFamily = cmd.getOptionValue(columnFamilyOption);
    if (StringUtils.isEmpty(columnFamily)) {
      err.println("Column family is not specified.");
      formatter.printHelp(cmdString, options, true);
      return false;
    }
    String tableNameWithNS = cmd.getOptionValue(tableNameOption);
    if (StringUtils.isEmpty(tableNameWithNS)) {
      err.println("Table name is not specified.");
      formatter.printHelp(cmdString, options, true);
      return false;
    }
    targetTableName = TableName.valueOf(tableNameWithNS);
    return true;
  }

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
    } catch (IOException | ParseException ex) {
      LOG.error("Error parsing command-line options", ex);
      return 1;
    }
    try {
      return recoverStoreFileList();
    } catch (IOException e) {
      LOG.error("Error recovering store file list", e);
      return 2;
    }
  }

  private int recoverStoreFileList() throws IOException {
    if (!regionOfflineAck && !dryRun) {
      err.println("ERROR, recover requires either --" + dryRunOption + " or --"
        + regionOfflineOption + " to acknowledge the region is offline. Refusing to write a new"
        + " manifest while the region may be online.");
      return 2;
    }
    if (TableName.isMetaTableName(targetTableName) && !forceMeta) {
      err.println("ERROR, refusing to recover hbase:meta without --" + forceMetaOption
        + ". This is dangerous and only valid with the master offline.");
      return 2;
    }
    Path root = CommonFSUtils.getRootDir(getConf());
    Path tablePath = CommonFSUtils.getTableDir(root, targetTableName);
    Path regionPath = new Path(tablePath, regionName);
    FileSystem fs = root.getFileSystem(getConf());
    TableDescriptor tableDescriptor = FSTableDescriptors.getTableDescriptorFromFs(fs, tablePath);
    if (tableDescriptor == null) {
      err.println("ERROR, unable to load table descriptor for " + targetTableName);
      return 2;
    }
    ColumnFamilyDescriptor familyDescriptor =
      tableDescriptor.getColumnFamily(Bytes.toBytes(columnFamily));
    if (familyDescriptor == null) {
      err.println("ERROR, column family does not exist: " + columnFamily);
      return 2;
    }
    String trackerName = StoreFileTrackerFactory.getStoreFileTrackerName(
      StoreUtils.createStoreConfiguration(getConf(), tableDescriptor, familyDescriptor));
    if (
      !StoreFileTrackerFactory.Trackers.FILE.name().equalsIgnoreCase(trackerName)
        && !StoreFileTrackerFactory.Trackers.MIGRATION.name().equalsIgnoreCase(trackerName)
    ) {
      err.println("ERROR, table " + targetTableName + " is not configured to use FILE store file"
        + " tracker (current: " + trackerName + "). Refusing to write a manifest the runtime"
        + " will not consult.");
      return 2;
    }
    RegionInfo regionInfo = HRegionFileSystem.loadRegionInfoFileContent(fs, regionPath);
    HRegionFileSystem regionFs =
      HRegionFileSystem.openRegionFromFileSystem(getConf(), fs, tablePath, regionInfo, true);

    // Split/merge parent assessment is meaningful only for user-table regions. hbase:meta and
    // master:store have no catalog lineage to consult, so skip the meta-walk for them.
    List<RegionInfo> parents = Collections.emptyList();
    if (
      !TableName.isMetaTableName(targetTableName)
        && !MasterRegionFactory.TABLE_NAME.equals(targetTableName)
    ) {
      try {
        parents = StoreFileListRecover.resolveParents(getConf(), regionInfo);
      } catch (IOException e) {
        LOG.warn("Failed to resolve split/merge parents for {} from hbase:meta; the data-loss"
          + " assessment will be skipped.", regionInfo.getEncodedName(), e);
        parents = Collections.emptyList();
      }
    }

    StoreFileListRecover.RecoverReport report = StoreFileListRecover.recover(getConf(),
      tableDescriptor, familyDescriptor, regionFs, parents, dryRun);
    printRecoverReport(report);
    return 0;
  }

  private void printRecoverReport(StoreFileListRecover.RecoverReport report) {
    out.println("Dry run: " + dryRun);
    for (StoreFileListRecover.TrackerFileDiagnostic diagnostic : report.getDiagnostics()) {
      if (diagnostic.getError() == null) {
        out.println("Tracker file " + diagnostic.getPath() + " loaded with "
          + diagnostic.getStoreFileCount() + " entries");
      } else {
        out.println(
          "Tracker file " + diagnostic.getPath() + " is corrupted: " + diagnostic.getError());
      }
    }
    out.println("Manifest entries (rebuilt from disk): " + report.getManifestEntries().size());

    // Per-parent on-disk status and data-loss verdict.
    if (!report.getParentContributions().isEmpty()) {
      out.println("--- Split/merge parent assessment ---");
      for (StoreFileListRecover.ParentContribution pc : report.getParentContributions()) {
        String parentName = pc.getParent().getEncodedName();
        switch (pc.getStatus()) {
          case ARCHIVED:
            out.println("  Parent " + parentName + ": ARCHIVED (directory not found).");
            break;
          case PRESENT_WITH_FILES:
            out.println("  Parent " + parentName + ": PRESENT, " + pc.getUnarchivedHFileCount()
              + " unarchived HFile(s).");
            break;
          case PRESENT_NO_FILES:
            out.println("  Parent " + parentName + ": PRESENT, no unarchived HFiles.");
            break;
          default:
            break;
        }
      }
      if (report.hasUnarchivedParents()) {
        out.println("POTENTIAL DATA LOSS: one or more split/merge parents still have unarchived"
          + " HFiles. The Catalog Janitor had not finished propagating parent data to this region"
          + " when the manifest was lost. The disk-only manifest may be missing rows. Manual data"
          + " recovery may be required -- review the parent regions before bringing this region"
          + " online.");
      } else if (report.allParentsArchived()) {
        out.println("LIKELY NO DATA LOSS: all split/merge parent directories are missing, which is"
          + " inferred to mean the Catalog Janitor archived them after their data was compacted"
          + " into this region. NOTE: a missing directory is not by itself proof the data was"
          + " archived (the same symptom occurs if a parent dir was lost before archival). If in"
          + " doubt, verify the parents' HFiles exist under the archive before relying on the"
          + " disk-only manifest.");
      } else {
        out.println("NO DATA LOSS: split/merge parents are present but carry no unarchived HFiles."
          + " The disk-only manifest is authoritative.");
      }
    }

    if (dryRun) {
      out.println("Dry-run completed. No new manifest was written.");
    } else if (report.isNoOp()) {
      out.println(
        "No recover needed: existing tracker file already matches the recomputed manifest.");
    } else if (report.getWrittenManifest() != null) {
      out.println("Wrote recovered manifest to " + report.getWrittenManifest());
    } else {
      out.println("WARNING: recover did not write a manifest and was not a dry-run; this is"
        + " unexpected and may indicate a bug.");
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    int ret = ToolRunner.run(conf, new StoreFileListRecoverTool(), args);
    System.exit(ret);
  }
}
