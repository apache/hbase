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
package org.apache.hadoop.hbase.backup;

import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.LONG_OPTION_ENABLE_CONTINUOUS_BACKUP;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_BACKUP_LIST_DESC;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_BANDWIDTH;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_BANDWIDTH_DESC;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_DEBUG;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_DEBUG_DESC;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_ENABLE_CONTINUOUS_BACKUP;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_ENABLE_CONTINUOUS_BACKUP_DESC;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_IGNORECHECKSUM;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_IGNORECHECKSUM_DESC;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_KEEP;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_KEEP_DESC;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_LIST;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_PATH;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_PATH_DESC;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_RECORD_NUMBER;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_RECORD_NUMBER_DESC;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_SET;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_SET_DESC;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_TABLE;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_TABLE_DESC;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_WORKERS;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_WORKERS_DESC;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_YARN_QUEUE_NAME;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_YARN_QUEUE_NAME_DESC;

import java.io.IOException;
import java.net.URI;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.backup.BackupRestoreConstants.BackupCommand;
import org.apache.hadoop.hbase.backup.impl.BackupCommands;
import org.apache.hadoop.hbase.backup.impl.BackupManager;
import org.apache.hadoop.hbase.logging.Log4jUtils;
import org.apache.hadoop.hbase.util.AbstractHBaseTool;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;

/**
 * Command-line entry point for backup operation
 */
@InterfaceAudience.Private
public class BackupDriver extends AbstractHBaseTool {

  private static final Logger LOG = LoggerFactory.getLogger(BackupDriver.class);
  private CommandLine cmd;

  public BackupDriver() throws IOException {
    init();
  }

  protected void init() throws IOException {
    // disable irrelevant loggers to avoid it mess up command output
    Log4jUtils.disableZkAndClientLoggers();
  }

  private int parseAndRun(String[] args) throws IOException {

    // Check if backup is enabled
    if (!BackupManager.isBackupEnabled(getConf())) {
      System.err.println(BackupRestoreConstants.ENABLE_BACKUP);
      return -1;
    }

    String cmd = null;
    String[] remainArgs = null;
    if (args == null || args.length == 0) {
      printToolUsage();
      return -1;
    } else {
      cmd = args[0];
      remainArgs = new String[args.length - 1];
      if (args.length > 1) {
        System.arraycopy(args, 1, remainArgs, 0, args.length - 1);
      }
    }

    BackupCommand type = BackupCommand.HELP;
    if (BackupCommand.CREATE.name().equalsIgnoreCase(cmd)) {
      type = BackupCommand.CREATE;
    } else if (BackupCommand.HELP.name().equalsIgnoreCase(cmd)) {
      type = BackupCommand.HELP;
    } else if (BackupCommand.DELETE.name().equalsIgnoreCase(cmd)) {
      type = BackupCommand.DELETE;
    } else if (BackupCommand.DESCRIBE.name().equalsIgnoreCase(cmd)) {
      type = BackupCommand.DESCRIBE;
    } else if (BackupCommand.HISTORY.name().equalsIgnoreCase(cmd)) {
      type = BackupCommand.HISTORY;
    } else if (BackupCommand.PROGRESS.name().equalsIgnoreCase(cmd)) {
      type = BackupCommand.PROGRESS;
    } else if (BackupCommand.SET.name().equalsIgnoreCase(cmd)) {
      type = BackupCommand.SET;
    } else if (BackupCommand.REPAIR.name().equalsIgnoreCase(cmd)) {
      type = BackupCommand.REPAIR;
    } else if (BackupCommand.MERGE.name().equalsIgnoreCase(cmd)) {
      type = BackupCommand.MERGE;
    } else {
      System.out.println("Unsupported command for backup: " + cmd);
      printToolUsage();
      return -1;
    }

    // enable debug logging
    if (this.cmd.hasOption(OPTION_DEBUG)) {
      Log4jUtils.setLogLevel("org.apache.hadoop.hbase.backup", "DEBUG");
    }

    BackupCommands.Command command = BackupCommands.createCommand(getConf(), type, this.cmd);
    if (type == BackupCommand.CREATE && conf != null) {
      ((BackupCommands.CreateCommand) command).setConf(conf);
    }
    try {
      command.execute();
    } catch (IOException e) {
      if (e.getMessage().equals(BackupCommands.INCORRECT_USAGE)) {
        return -1;
      }
      throw e;
    } finally {
      command.finish();
    }
    return 0;
  }

  @Override
  protected void addOptions() {
    // define supported options
    addOptNoArg(OPTION_DEBUG, OPTION_DEBUG_DESC);
    addOptWithArg(OPTION_TABLE, OPTION_TABLE_DESC);
    addOptWithArg(OPTION_BANDWIDTH, OPTION_BANDWIDTH_DESC);
    addOptWithArg(OPTION_LIST, OPTION_BACKUP_LIST_DESC);
    addOptWithArg(OPTION_WORKERS, OPTION_WORKERS_DESC);
    addOptNoArg(OPTION_IGNORECHECKSUM, OPTION_IGNORECHECKSUM_DESC);
    addOptWithArg(OPTION_RECORD_NUMBER, OPTION_RECORD_NUMBER_DESC);
    addOptWithArg(OPTION_SET, OPTION_SET_DESC);
    addOptWithArg(OPTION_PATH, OPTION_PATH_DESC);
    addOptWithArg(OPTION_KEEP, OPTION_KEEP_DESC);
    addOptWithArg(OPTION_YARN_QUEUE_NAME, OPTION_YARN_QUEUE_NAME_DESC);
    addOptNoArg(OPTION_ENABLE_CONTINUOUS_BACKUP, LONG_OPTION_ENABLE_CONTINUOUS_BACKUP,
      OPTION_ENABLE_CONTINUOUS_BACKUP_DESC);
  }

  @Override
  protected void processOptions(CommandLine cmd) {
    this.cmd = cmd;
  }

  @Override
  protected int doWork() throws Exception {
    return parseAndRun(cmd.getArgs());
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    Path hbasedir = CommonFSUtils.getRootDir(conf);
    URI defaultFs = hbasedir.getFileSystem(conf).getUri();
    CommonFSUtils.setFsDefault(conf, new Path(defaultFs));
    int ret = ToolRunner.run(conf, new BackupDriver(), args);
    System.exit(ret);
  }

  @Override
  public int run(String[] args) throws IOException {
    Objects.requireNonNull(conf, "Tool configuration is not initialized");

    CommandLine cmd;
    try {
      // parse the command line arguments
      cmd = parseArgs(args);
      cmdLineArgs = args;
    } catch (Exception e) {
      System.err.println("Error when parsing command-line arguments: " + e.getMessage());
      printToolUsage();
      return EXIT_FAILURE;
    }
    processOptions(cmd);

    int ret = EXIT_FAILURE;
    try {
      ret = doWork();
    } catch (Exception e) {
      LOG.error("Error running command-line tool", e);
      return EXIT_FAILURE;
    }
    return ret;
  }

  protected void printToolUsage() throws IOException {
    System.out.println(BackupCommands.USAGE);
    System.out.println(BackupRestoreConstants.VERIFY_BACKUP);
  }
}
