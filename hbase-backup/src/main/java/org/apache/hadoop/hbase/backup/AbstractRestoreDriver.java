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
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupManager;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.logging.Log4jUtils;
import org.apache.hadoop.hbase.util.AbstractHBaseTool;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.hbase.thirdparty.org.apache.commons.cli.HelpFormatter;

@InterfaceAudience.Private
public abstract class AbstractRestoreDriver extends AbstractHBaseTool {
  protected static final Logger LOG = LoggerFactory.getLogger(AbstractRestoreDriver.class);
  protected CommandLine cmd;

  protected static final String USAGE_FOOTER = "";

  protected AbstractRestoreDriver() {
    init();
  }

  protected void init() {
    Log4jUtils.disableZkAndClientLoggers();
  }

  protected abstract int executeRestore(boolean check, TableName[] fromTables, TableName[] toTables,
    boolean isOverwrite);

  private int parseAndRun() throws IOException {
    if (!BackupManager.isBackupEnabled(getConf())) {
      System.err.println(BackupRestoreConstants.ENABLE_BACKUP);
      return -1;
    }

    if (cmd.hasOption(OPTION_DEBUG)) {
      Log4jUtils.setLogLevel("org.apache.hadoop.hbase.backup", "DEBUG");
    }

    boolean overwrite = cmd.hasOption(OPTION_OVERWRITE);
    if (overwrite) {
      LOG.debug("Found -overwrite option in restore command, "
        + "will overwrite to existing table if any in the restore target");
    }

    boolean check = cmd.hasOption(OPTION_CHECK);
    if (check) {
      LOG.debug(
        "Found -check option in restore command, " + "will check and verify the dependencies");
    }

    if (cmd.hasOption(OPTION_SET) && cmd.hasOption(OPTION_TABLE)) {
      System.err.println(
        "Options -s and -t are mutually exclusive," + " you can not specify both of them.");
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
      // Set MR job queuename to configuration
      getConf().set("mapreduce.job.queuename", queueName);
    }

    String tables;
    TableName[] sTableArray;
    TableName[] tTableArray;

    String tableMapping =
      cmd.hasOption(OPTION_TABLE_MAPPING) ? cmd.getOptionValue(OPTION_TABLE_MAPPING) : null;
    try (final Connection conn = ConnectionFactory.createConnection(conf)) {
      // Check backup set
      if (cmd.hasOption(OPTION_SET)) {
        String setName = cmd.getOptionValue(OPTION_SET);
        try {
          tables = getTablesForSet(conn, setName);
        } catch (IOException e) {
          System.out.println("ERROR: " + e.getMessage() + " for setName=" + setName);
          printToolUsage();
          return -2;
        }
        if (tables == null) {
          System.out
            .println("ERROR: Backup set '" + setName + "' is either empty or does not exist");
          printToolUsage();
          return -3;
        }
      } else {
        tables = cmd.getOptionValue(OPTION_TABLE);
      }

      sTableArray = BackupUtils.parseTableNames(tables);
      tTableArray = BackupUtils.parseTableNames(tableMapping);

      if (
        sTableArray != null && tTableArray != null && (sTableArray.length != tTableArray.length)
      ) {
        System.out.println("ERROR: table mapping mismatch: " + tables + " : " + tableMapping);
        printToolUsage();
        return -4;
      }
    }

    return executeRestore(check, sTableArray, tTableArray, overwrite);
  }

  @Override
  protected void addOptions() {
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
    return parseAndRun();
  }

  @Override
  public int run(String[] args) {
    Objects.requireNonNull(conf, "Tool configuration is not initialized");

    try {
      cmd = parseArgs(args);
    } catch (Exception e) {
      System.out.println("Error parsing command-line arguments: " + e.getMessage());
      printToolUsage();
      return EXIT_FAILURE;
    }

    if (cmd.hasOption(SHORT_HELP_OPTION) || cmd.hasOption(LONG_HELP_OPTION)) {
      printToolUsage();
      return EXIT_FAILURE;
    }

    processOptions(cmd);

    try {
      return doWork();
    } catch (Exception e) {
      LOG.error("Error running restore tool", e);
      return EXIT_FAILURE;
    }
  }

  protected void printToolUsage() {
    System.out.println(getUsageString());
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.setLeftPadding(2);
    helpFormatter.setDescPadding(8);
    helpFormatter.setWidth(100);
    helpFormatter.setSyntaxPrefix("Options:");
    helpFormatter.printHelp(" ", null, options, USAGE_FOOTER);
    System.out.println(BackupRestoreConstants.VERIFY_BACKUP);
  }

  protected abstract String getUsageString();

  private String getTablesForSet(Connection conn, String name) throws IOException {
    try (final BackupSystemTable table = new BackupSystemTable(conn)) {
      List<TableName> tables = table.describeBackupSet(name);

      if (tables == null) {
        return null;
      }

      return StringUtils.join(tables, BackupRestoreConstants.TABLENAME_DELIMITER_IN_COMMAND);
    }
  }
}
