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
package org.apache.hadoop.hbase.migration;

import java.io.IOException;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALSplitter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HFileV1Detector;
import org.apache.hadoop.hbase.util.ZKDataMigrator;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class UpgradeTo96 extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(UpgradeTo96.class);

  private Options options = new Options();
  /**
   * whether to do overall upgrade (namespace and znodes)
   */
  private boolean upgrade;
  /**
   * whether to check for HFileV1
   */
  private boolean checkForHFileV1;
  /**
   * Path of directory to check for HFileV1
   */
  private String dirToCheckForHFileV1;

  UpgradeTo96() {
    setOptions();
  }

  private void setOptions() {
    options.addOption("h", "help", false, "Help");
    options.addOption(new Option("check", false, "Run upgrade check; looks for HFileV1 "
        + " under ${hbase.rootdir} or provided 'dir' directory."));
    options.addOption(new Option("execute", false, "Run upgrade; zk and hdfs must be up, hbase down"));
    Option pathOption = new Option("dir", true,
        "Relative path of dir to check for HFileV1s.");
    pathOption.setRequired(false);
    options.addOption(pathOption);
  }

  private boolean parseOption(String[] args) throws ParseException {
    if (args.length == 0) return false; // no args shows help.

    CommandLineParser parser = new GnuParser();
    CommandLine cmd = parser.parse(options, args);
    if (cmd.hasOption("h")) {
      return false;
    }
    if (cmd.hasOption("execute")) upgrade = true;
    if (cmd.hasOption("check")) checkForHFileV1 = true;
    if (checkForHFileV1 && cmd.hasOption("dir")) {
      this.dirToCheckForHFileV1 = cmd.getOptionValue("dir");
    }
    return true;
  }

  private void printUsage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("$bin/hbase upgrade -check [-dir DIR]|-execute", options);
    System.out.println("Read http://hbase.apache.org/book.html#upgrade0.96 before attempting upgrade");
    System.out.println();
    System.out.println("Example usage:");
    System.out.println();
    System.out.println("Run upgrade check; looks for HFileV1s under ${hbase.rootdir}:");
    System.out.println(" $ bin/hbase upgrade -check");
    System.out.println();
    System.out.println("Run the upgrade: ");
    System.out.println(" $ bin/hbase upgrade -execute");
  }

  @Override
  public int run(String[] args) throws Exception {
    if (!parseOption(args)) {
      printUsage();
      return -1;
    }
    if (checkForHFileV1) {
      int res = doHFileV1Check();
      if (res == 0) LOG.info("No HFileV1 found.");
      else {
        LOG.warn("There are some HFileV1, or corrupt files (files with incorrect major version).");
      }
      return res;
    }
    // if the user wants to upgrade, check for any HBase live process.
    // If yes, prompt the user to stop them
    else if (upgrade) {
      if (isAnyHBaseProcessAlive()) {
        LOG.error("Some HBase processes are still alive, or znodes not expired yet. "
            + "Please stop them before upgrade or try after some time.");
        throw new IOException("Some HBase processes are still alive, or znodes not expired yet");
      }
      return executeUpgrade();
    }
    return -1;
  }

  private boolean isAnyHBaseProcessAlive() throws IOException {
    ZooKeeperWatcher zkw = null;
    try {
      zkw = new ZooKeeperWatcher(getConf(), "Check Live Processes.", new Abortable() {
        private boolean aborted = false;

        @Override
        public void abort(String why, Throwable e) {
          LOG.warn("Got aborted with reason: " + why + ", and error: " + e);
          this.aborted = true;
        }

        @Override
        public boolean isAborted() {
          return this.aborted;
        }

      });
      boolean liveProcessesExists = false;
      if (ZKUtil.checkExists(zkw, zkw.baseZNode) == -1) {
        return false;
      }
      if (ZKUtil.checkExists(zkw, zkw.backupMasterAddressesZNode) != -1) {
        List<String> backupMasters = ZKUtil
            .listChildrenNoWatch(zkw, zkw.backupMasterAddressesZNode);
        if (!backupMasters.isEmpty()) {
          LOG.warn("Backup master(s) " + backupMasters
              + " are alive or backup-master znodes not expired.");
          liveProcessesExists = true;
        }
      }
      if (ZKUtil.checkExists(zkw, zkw.rsZNode) != -1) {
        List<String> regionServers = ZKUtil.listChildrenNoWatch(zkw, zkw.rsZNode);
        if (!regionServers.isEmpty()) {
          LOG.warn("Region server(s) " + regionServers + " are alive or rs znodes not expired.");
          liveProcessesExists = true;
        }
      }
      if (ZKUtil.checkExists(zkw, zkw.getMasterAddressZNode()) != -1) {
        byte[] data = ZKUtil.getData(zkw, zkw.getMasterAddressZNode());
        if (data != null && !Bytes.equals(data, HConstants.EMPTY_BYTE_ARRAY)) {
          LOG.warn("Active master at address " + Bytes.toString(data)
              + " is still alive or master znode not expired.");
          liveProcessesExists = true;
        }
      }
      return liveProcessesExists;
    } catch (Exception e) {
      LOG.error("Got exception while checking live hbase processes", e);
      throw new IOException(e);
    } finally {
      if (zkw != null) {
        zkw.close();
      }
    }
  }

  private int doHFileV1Check() throws Exception {
    String[] args = null;
    if (dirToCheckForHFileV1 != null) args = new String[] { "-p" + dirToCheckForHFileV1 };
    return ToolRunner.run(getConf(), new HFileV1Detector(), args);
  }

  /**
   * Executes the upgrade process. It involves:
   * <ul>
   * <li> Upgrading Namespace
   * <li> Upgrading Znodes
   * <li> Log splitting
   * </ul>
   * @throws Exception
   */
  private int executeUpgrade() throws Exception {
    executeTool("Namespace upgrade", new NamespaceUpgrade(),
      new String[] { "--upgrade" }, 0);
    executeTool("Znode upgrade", new ZKDataMigrator(), null, 0);
    doOfflineLogSplitting();
    return 0;
  }

  private void executeTool(String toolMessage, Tool tool, String[] args, int expectedResult)
      throws Exception {
    LOG.info("Starting " + toolMessage);
    int res = ToolRunner.run(getConf(), tool, new String[] { "--upgrade" });
    if (res != expectedResult) {
      LOG.error(toolMessage + "returned " + res + ", expected " + expectedResult);
      throw new Exception("Unexpected return code from " + toolMessage);
    }
    LOG.info("Successfully completed " + toolMessage);
  }

  /**
   * Performs log splitting for all regionserver directories.
   * @throws Exception
   */
  private void doOfflineLogSplitting() throws Exception {
    LOG.info("Starting Log splitting");
    final Path rootDir = FSUtils.getRootDir(getConf());
    final Path oldLogDir = new Path(rootDir, HConstants.HREGION_OLDLOGDIR_NAME);
    // since this is the singleton, we needn't close it.
    final WALFactory factory = WALFactory.getInstance(getConf());
    FileSystem fs = FSUtils.getCurrentFileSystem(getConf());
    Path logDir = new Path(rootDir, HConstants.HREGION_LOGDIR_NAME);
    FileStatus[] regionServerLogDirs = FSUtils.listStatus(fs, logDir);
    if (regionServerLogDirs == null || regionServerLogDirs.length == 0) {
      LOG.info("No log directories to split, returning");
      return;
    }
    try {
      for (FileStatus regionServerLogDir : regionServerLogDirs) {
        // split its log dir, if exists
        WALSplitter.split(rootDir, regionServerLogDir.getPath(), oldLogDir, fs, getConf(), factory);
      }
      LOG.info("Successfully completed Log splitting");
    } catch (Exception e) {
      LOG.error("Got exception while doing Log splitting ", e);
      throw e;
    }
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(HBaseConfiguration.create(), new UpgradeTo96(), args));
  }
}
