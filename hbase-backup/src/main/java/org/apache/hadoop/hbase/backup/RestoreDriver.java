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
package org.apache.hadoop.hbase.backup;

import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_CHECK;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_CHECK_DESC;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_DEBUG;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_DEBUG_DESC;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_OVERWRITE;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_OVERWRITE_DESC;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_SET;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_SET_RESTORE_DESC;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_TABLE;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_TABLE_LIST_DESC;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_TABLE_MAPPING;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_TABLE_MAPPING_DESC;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_YARN_QUEUE_NAME;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_YARN_QUEUE_NAME_RESTORE_DESC;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupAdminImpl;
import org.apache.hadoop.hbase.backup.impl.BackupManager;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.AbstractHBaseTool;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.hbase.thirdparty.org.apache.commons.cli.HelpFormatter;

/**
 *
 * Command-line entry point for restore operation
 *
 */
@InterfaceAudience.Private
public class RestoreDriver extends AbstractHBaseTool {
  private static final Logger LOG = LoggerFactory.getLogger(RestoreDriver.class);
  private CommandLine cmd;

  private static final String USAGE_STRING =
      "Usage: hbase restore <backup_path> <backup_id> [options]\n"
          + "  backup_path     Path to a backup destination root\n"
          + "  backup_id       Backup image ID to restore\n"
          + "  table(s)        Comma-separated list of tables to restore\n";

  private static final String USAGE_FOOTER = "";

  protected RestoreDriver() throws IOException {
    init();
  }

  protected void init() {
    // disable irrelevant loggers to avoid it mess up command output
    LogUtils.disableZkAndClientLoggers();
  }

  private int parseAndRun(String[] args) throws IOException {
    // Check if backup is enabled
    if (!BackupManager.isBackupEnabled(getConf())) {
      System.err.println(BackupRestoreConstants.ENABLE_BACKUP);
      return -1;
    }

    System.out.println(BackupRestoreConstants.VERIFY_BACKUP);

    // enable debug logging
    if (cmd.hasOption(OPTION_DEBUG)) {
      LogManager.getLogger("org.apache.hadoop.hbase.backup").setLevel(Level.DEBUG);
    }

    // whether to overwrite to existing table if any, false by default
    boolean overwrite = cmd.hasOption(OPTION_OVERWRITE);
    if (overwrite) {
      LOG.debug("Found -overwrite option in restore command, "
          + "will overwrite to existing table if any in the restore target");
    }

    // whether to only check the dependencies, false by default
    boolean check = cmd.hasOption(OPTION_CHECK);
    if (check) {
      LOG.debug("Found -check option in restore command, "
          + "will check and verify the dependencies");
    }

    if (cmd.hasOption(OPTION_SET) && cmd.hasOption(OPTION_TABLE)) {
      System.err.println("Options -s and -t are mutaully exclusive,"+
          " you can not specify both of them.");
      printToolUsage();
      return -1;
    }

    if (!cmd.hasOption(OPTION_SET) && !cmd.hasOption(OPTION_TABLE)) {
      System.err.println("You have to specify either set name or table list to restore");
      printToolUsage();
      return -1;
    }

    if (cmd.hasOption(OPTION_YARN_QUEUE_NAME)) {
      String queueName = cmd.getOptionValue(OPTION_YARN_QUEUE_NAME);
      // Set system property value for MR job
      System.setProperty("mapreduce.job.queuename", queueName);
    }

    // parse main restore command options
    String[] remainArgs = cmd.getArgs();
    if (remainArgs.length != 2) {
      printToolUsage();
      return -1;
    }

    String backupRootDir = remainArgs[0];
    String backupId = remainArgs[1];
    String tables;
    String tableMapping =
        cmd.hasOption(OPTION_TABLE_MAPPING) ? cmd.getOptionValue(OPTION_TABLE_MAPPING) : null;
    try (final Connection conn = ConnectionFactory.createConnection(conf);
        BackupAdmin client = new BackupAdminImpl(conn)) {
      // Check backup set
      if (cmd.hasOption(OPTION_SET)) {
        String setName = cmd.getOptionValue(OPTION_SET);
        try {
          tables = getTablesForSet(conn, setName, conf);
        } catch (IOException e) {
          System.out.println("ERROR: " + e.getMessage() + " for setName=" + setName);
          printToolUsage();
          return -2;
        }
        if (tables == null) {
          System.out.println("ERROR: Backup set '" + setName
              + "' is either empty or does not exist");
          printToolUsage();
          return -3;
        }
      } else {
        tables = cmd.getOptionValue(OPTION_TABLE);
      }

      TableName[] sTableArray = BackupUtils.parseTableNames(tables);
      TableName[] tTableArray = BackupUtils.parseTableNames(tableMapping);

      if (sTableArray != null && tTableArray != null &&
          (sTableArray.length != tTableArray.length)) {
        System.out.println("ERROR: table mapping mismatch: " + tables + " : " + tableMapping);
        printToolUsage();
        return -4;
      }

      client.restore(BackupUtils.createRestoreRequest(backupRootDir, backupId, check,
        sTableArray, tTableArray, overwrite));
    } catch (Exception e) {
      LOG.error("Error while running restore backup", e);
      return -5;
    }
    return 0;
  }

  private String getTablesForSet(Connection conn, String name, Configuration conf)
      throws IOException {
    try (final BackupSystemTable table = new BackupSystemTable(conn)) {
      List<TableName> tables = table.describeBackupSet(name);

      if (tables == null) {
        return null;
      }

      return StringUtils.join(tables, BackupRestoreConstants.TABLENAME_DELIMITER_IN_COMMAND);
    }
  }

  @Override
  protected void addOptions() {
    // define supported options
    addOptNoArg(OPTION_OVERWRITE, OPTION_OVERWRITE_DESC);
    addOptNoArg(OPTION_CHECK, OPTION_CHECK_DESC);
    addOptNoArg(OPTION_DEBUG, OPTION_DEBUG_DESC);
    addOptWithArg(OPTION_SET, OPTION_SET_RESTORE_DESC);
    addOptWithArg(OPTION_TABLE, OPTION_TABLE_LIST_DESC);
    addOptWithArg(OPTION_TABLE_MAPPING, OPTION_TABLE_MAPPING_DESC);
    addOptWithArg(OPTION_YARN_QUEUE_NAME, OPTION_YARN_QUEUE_NAME_RESTORE_DESC);
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
    Path hbasedir = FSUtils.getRootDir(conf);
    URI defaultFs = hbasedir.getFileSystem(conf).getUri();
    FSUtils.setFsDefault(conf, new Path(defaultFs));
    int ret = ToolRunner.run(conf, new RestoreDriver(), args);
    System.exit(ret);
  }

  @Override
  public int run(String[] args) {
    if (conf == null) {
      LOG.error("Tool configuration is not initialized");
      throw new NullPointerException("conf");
    }

    CommandLine cmd;
    try {
      // parse the command line arguments
      cmd = parseArgs(args);
      cmdLineArgs = args;
    } catch (Exception e) {
      System.out.println("Error when parsing command-line arguments: " + e.getMessage());
      printToolUsage();
      return EXIT_FAILURE;
    }

    if (cmd.hasOption(SHORT_HELP_OPTION) || cmd.hasOption(LONG_HELP_OPTION)) {
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

  protected void printToolUsage() {
    System.out.println(USAGE_STRING);
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.setLeftPadding(2);
    helpFormatter.setDescPadding(8);
    helpFormatter.setWidth(100);
    helpFormatter.setSyntaxPrefix("Options:");
    helpFormatter.printHelp(" ", null, options, USAGE_FOOTER);
  }
}
