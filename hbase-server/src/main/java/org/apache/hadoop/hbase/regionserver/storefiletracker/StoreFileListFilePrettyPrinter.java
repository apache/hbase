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
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.CatalogFamilyFormat;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.StoreUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.PairOfSameType;
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

import org.apache.hadoop.hbase.shaded.protobuf.generated.StoreFileTrackerProtos.StoreFileList;

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
@InterfaceStability.Evolving
public class StoreFileListFilePrettyPrinter extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(StoreFileListFilePrettyPrinter.class);

  private Options options = new Options();

  private final String fileOption = "f";
  private final String columnFamilyOption = "cf";
  private final String regionOption = "r";
  private final String tableNameOption = "t";
  private final String repairOption = "repair";
  private final String repairModeOption = "repair-mode";
  private final String dryRunOption = "dry-run";
  private final String forceMetaOption = "force-meta";
  private final String regionOfflineOption = "region-offline";

  private final String cmdString = "sft";

  private String namespace;
  private String regionName;
  private String columnFamily;
  private String tableName;
  private Path path;
  private TableName targetTableName;
  private boolean repair;
  private boolean dryRun;
  private boolean forceMeta;
  private boolean regionOfflineAck;
  private StoreFileListRepair.Mode repairMode = StoreFileListRepair.Mode.DISK_ONLY;
  private PrintStream err = System.err;
  private PrintStream out = System.out;

  public StoreFileListFilePrettyPrinter() {
    super();
    init();
  }

  public StoreFileListFilePrettyPrinter(Configuration conf) {
    super(conf);
    init();
  }

  private void init() {
    OptionGroup files = new OptionGroup();
    options.addOption(new Option(tableNameOption, "table", true,
      "Table to scan. Pass table name; e.g. test_table"));
    options.addOption(new Option(columnFamilyOption, "columnfamily", true,
      "column family to scan. Pass column family name; e.g. f"));
    files.addOption(new Option(regionOption, "region", true,
      "Region to scan. Pass region name; e.g. '3d58e9067bf23e378e68c071f3dd39eb'"));
    files.addOption(new Option(fileOption, "file", true,
      "File to scan. Pass full-path; e.g. /root/hbase-3.0.0-alpha-4-SNAPSHOT/hbase-data/"
        + "data/default/tbl-sft/093fa06bf84b3b631007f951a14b8457/f/.filelist/f2.1655139542249"));
    options.addOptionGroup(files);
    options.addOption(new Option(null, repairOption, false,
      "Repair a corrupted store file tracker manifest for the target table/region/family. "
        + "Requires --" + regionOfflineOption + " to acknowledge the region is offline."));
    options.addOption(new Option(null, repairModeOption, true,
      "Repair mode: disk-only or lineage-assisted (default: disk-only)"));
    options.addOption(new Option(null, dryRunOption, false,
      "Print the repair result without writing a new manifest"));
    options.addOption(new Option(null, forceMetaOption, false,
      "Allow repair against the hbase:meta table. Dangerous; only use with master offline."));
    options.addOption(new Option(null, regionOfflineOption, false,
      "Operator acknowledgement that the target region is offline (no master/RS hosting it)."));
  }

  public boolean parseOptions(String[] args) throws ParseException, IOException {
    HelpFormatter formatter = new HelpFormatter();
    if (args.length == 0) {
      formatter.printHelp(cmdString, options, true);
      return false;
    }

    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args);
    repair = cmd.hasOption(repairOption);
    dryRun = cmd.hasOption(dryRunOption);
    forceMeta = cmd.hasOption(forceMetaOption);
    regionOfflineAck = cmd.hasOption(regionOfflineOption);
    if (cmd.hasOption(repairModeOption)) {
      repairMode = StoreFileListRepair.Mode.valueOfOption(cmd.getOptionValue(repairModeOption));
    }

    if (cmd.hasOption(fileOption)) {
      if (repair) {
        err.println("--file can not be used together with --repair.");
        formatter.printHelp(cmdString, options, true);
        return false;
      }
      path = new Path(cmd.getOptionValue(fileOption));
    } else {
      regionName = cmd.getOptionValue(regionOption);
      if (StringUtils.isEmpty(regionName)) {
        err.println("Region name is not specified.");
        formatter.printHelp(cmdString, options, true);
        System.exit(1);
      }
      columnFamily = cmd.getOptionValue(columnFamilyOption);
      if (StringUtils.isEmpty(columnFamily)) {
        err.println("Column family is not specified.");
        formatter.printHelp(cmdString, options, true);
        System.exit(1);
      }
      String tableNameWtihNS = cmd.getOptionValue(tableNameOption);
      if (StringUtils.isEmpty(tableNameWtihNS)) {
        err.println("Table name is not specified.");
        formatter.printHelp(cmdString, options, true);
        System.exit(1);
      }
      targetTableName = TableName.valueOf(tableNameWtihNS);
      namespace = targetTableName.getNamespaceAsString();
      tableName = targetTableName.getNameAsString();
    }
    return true;
  }

  public int run(String[] args) {
    if (getConf() == null) {
      throw new RuntimeException("A Configuration instance must be provided.");
    }
    boolean pass = true;
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
    FileSystem fs = null;
    if (repair) {
      try {
        return repairStoreFileList();
      } catch (IOException e) {
        LOG.error("Error repairing store file list", e);
        return 2;
      }
    }
    if (path != null) {
      try {
        fs = path.getFileSystem(getConf());
        if (fs.isDirectory(path)) {
          err.println("ERROR, wrong path given: " + path);
          return 2;
        }
        return print(fs, path);
      } catch (IOException e) {
        LOG.error("Error reading " + path, e);
        return 2;
      }
    } else {
      try {
        Path root = CommonFSUtils.getRootDir(getConf());
        Path baseDir = new Path(root, HConstants.BASE_NAMESPACE_DIR);
        Path nameSpacePath = new Path(baseDir, namespace);
        Path tablePath = new Path(nameSpacePath, tableName);
        Path regionPath = new Path(tablePath, regionName);
        Path cfPath = new Path(regionPath, columnFamily);
        Path sftPath = new Path(cfPath, StoreFileListFile.TRACK_FILE_DIR);

        fs = FileSystem.newInstance(regionPath.toUri(), getConf());

        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(sftPath, false);

        while (iterator.hasNext()) {
          LocatedFileStatus lfs = iterator.next();
          if (
            lfs.isFile()
              && StoreFileListFile.TRACK_FILE_PATTERN.matcher(lfs.getPath().getName()).matches()
          ) {
            out.println("Printing contents for file " + lfs.getPath().toString());
            int ret = print(fs, lfs.getPath());
            if (ret != 0) {
              pass = false;
            }
          }
        }
      } catch (IOException e) {
        LOG.error("Error processing " + e);
        return 2;
      }
    }
    return pass ? 0 : 2;
  }

  private int repairStoreFileList() throws IOException {
    if (!regionOfflineAck && !dryRun) {
      err.println("ERROR, --" + repairOption + " requires either --" + dryRunOption
        + " or --" + regionOfflineOption
        + " to acknowledge the region is offline. Refusing to write a new manifest while the"
        + " region may be online.");
      return 2;
    }
    if (TableName.isMetaTableName(targetTableName) && !forceMeta) {
      err.println("ERROR, refusing to repair hbase:meta without --" + forceMetaOption
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
    String trackerName = StoreFileTrackerFactory.getStoreFileTrackerName(
      StoreUtils.createStoreConfiguration(getConf(), tableDescriptor,
        tableDescriptor.getColumnFamily(Bytes.toBytes(columnFamily)) != null
          ? tableDescriptor.getColumnFamily(Bytes.toBytes(columnFamily))
          : tableDescriptor.getColumnFamilies()[0]));
    if (
      !StoreFileTrackerFactory.Trackers.FILE.name().equalsIgnoreCase(trackerName)
        && !StoreFileTrackerFactory.Trackers.MIGRATION.name().equalsIgnoreCase(trackerName)
    ) {
      err.println("ERROR, table " + targetTableName + " is not configured to use FILE store file"
        + " tracker (current: " + trackerName + "). Refusing to write a manifest the runtime"
        + " will not consult.");
      return 2;
    }
    ColumnFamilyDescriptor familyDescriptor =
      tableDescriptor.getColumnFamily(Bytes.toBytes(columnFamily));
    if (familyDescriptor == null) {
      err.println("ERROR, column family does not exist: " + columnFamily);
      return 2;
    }
    RegionInfo regionInfo = HRegionFileSystem.loadRegionInfoFileContent(fs, regionPath);
    HRegionFileSystem regionFs =
      HRegionFileSystem.openRegionFromFileSystem(getConf(), fs, tablePath, regionInfo, true);
    StoreFileListRepair.Lineage lineage = StoreFileListRepair.Lineage.none();
    if (repairMode == StoreFileListRepair.Mode.LINEAGE_ASSISTED) {
      try {
        lineage = resolveLineage(regionInfo);
      } catch (IOException e) {
        LOG.warn("Failed to resolve lineage for {}; falling back to disk-only behaviour.",
          regionInfo.getEncodedName(), e);
        lineage = StoreFileListRepair.Lineage.none();
      }
    }
    StoreFileListRepair.RepairReport report = StoreFileListRepair.repair(getConf(), tableDescriptor,
      familyDescriptor, regionFs, lineage, repairMode, dryRun);
    printRepairReport(report);
    return 0;
  }

  private StoreFileListRepair.Lineage resolveLineage(RegionInfo regionInfo) throws IOException {
    try (Connection connection = ConnectionFactory.createConnection(getConf())) {
      Result childRow = MetaTableAccessor.getRegionResult(connection, regionInfo);
      if (childRow != null && !childRow.isEmpty()) {
        List<RegionInfo> mergeParents = CatalogFamilyFormat.getMergeRegions(childRow.rawCells());
        if (!mergeParents.isEmpty()) {
          return StoreFileListRepair.Lineage.mergeParents(mergeParents);
        }
      }
      final RegionInfo[] splitParent = new RegionInfo[1];
      MetaTableAccessor.scanMetaForTableRegions(connection, result -> {
        PairOfSameType<RegionInfo> daughters = MetaTableAccessor.getDaughterRegions(result);
        if (regionInfo.equals(daughters.getFirst()) || regionInfo.equals(daughters.getSecond())) {
          splitParent[0] = CatalogFamilyFormat.getRegionInfo(result);
          return false;
        }
        return true;
      }, regionInfo.getTable());
      return splitParent[0] != null ? StoreFileListRepair.Lineage.splitParent(splitParent[0])
        : StoreFileListRepair.Lineage.none();
    }
  }

  private void printRepairReport(StoreFileListRepair.RepairReport report) {
    out.println("Repair mode: " + repairMode.name().toLowerCase());
    out.println("Dry run: " + dryRun);
    for (StoreFileListRepair.TrackerFileDiagnostic diagnostic : report.getDiagnostics()) {
      if (diagnostic.getError() == null) {
        out.println("Tracker file " + diagnostic.getPath() + " loaded with "
          + diagnostic.getStoreFileCount() + " entries");
      } else {
        out.println("Tracker file " + diagnostic.getPath() + " is corrupted: "
          + diagnostic.getError());
      }
    }
    out.println("Disk entries: " + report.getDiskEntries().size());
    out.println("Lineage-derived entries: " + report.getLineageEntries().size());
    out.println("Manifest entries: " + report.getManifestEntries().size());

    // Per-parent contribution detail and data-loss confidence assessment.
    if (!report.getParentContributions().isEmpty()) {
      out.println("--- Parent contribution detail ---");
      for (StoreFileListRepair.ParentContribution pc : report.getParentContributions()) {
        String regionName = pc.getParent().getEncodedName();
        switch (pc.getStatus()) {
          case ARCHIVED:
            out.println("  Parent " + regionName + ": ARCHIVED (directory not found).");
            break;
          case PRESENT_WITH_FILES:
            out.println("  Parent " + regionName + ": PRESENT, contributed "
              + pc.getFilesContributed() + " reference(s)/link(s).");
            break;
          case PRESENT_NO_FILES:
            out.println("  Parent " + regionName + ": PRESENT, but no HFiles matched.");
            break;
          default:
            break;
        }
      }
      if (report.allParentsArchived()) {
        out.println("All parent regions are archived by Catalog Janitor. This means daughters "
          + "have already compacted away all split/merge references. "
          + "No data loss expected; the disk-only file set is authoritative.");
      } else if (report.hasUnarchivedParents()) {
        out.println("WARNING: One or more parent regions still have unarchived HFiles. "
          + "Reconstructed references/links from these parents may reintroduce data that "
          + "was previously compacted away by the daughter. Admin review recommended before "
          + "bringing the region online.");
      }
    }

    if (dryRun) {
      out.println("Dry-run completed. No new manifest was written.");
    } else if (report.isNoOp()) {
      out.println(
        "No repair needed: existing tracker file already matches the recomputed manifest.");
    } else if (report.getWrittenManifest() != null) {
      out.println("Wrote repaired manifest to " + report.getWrittenManifest());
    } else {
      out.println("WARNING: repair did not write a manifest and was not a dry-run; this is"
        + " unexpected and may indicate a bug.");
    }
  }

  private int print(FileSystem fs, Path path) throws IOException {
    try {
      if (!fs.exists(path)) {
        err.println("ERROR, file doesnt exist: " + path);
        return 2;
      }
    } catch (IOException e) {
      err.println("ERROR, reading file: " + path + e);
      return 2;
    }
    StoreFileList storeFile = StoreFileListFile.load(fs, path);
    int end = storeFile.getStoreFileCount();
    for (int i = 0; i < end; i++) {
      out.println(storeFile.getStoreFile(i).getName());
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    int ret = ToolRunner.run(conf, new StoreFileListFilePrettyPrinter(), args);
    System.exit(ret);
  }
}
