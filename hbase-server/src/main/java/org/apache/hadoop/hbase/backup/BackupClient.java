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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupRestoreConstants.BACKUP_COMMAND;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

/**
 * Backup HBase tables locally or on a remote cluster Serve as client entry point for the following
 * features: - Full Backup provide local and remote back/restore for a list of tables - Incremental
 * backup to build on top of full backup as daily/weekly backup - Convert incremental backup WAL
 * files into hfiles - Merge several backup images into one(like merge weekly into monthly) - Add
 * and remove table to and from Backup image - Cancel a backup process - Full backup based on
 * existing snapshot - Describe information of a backup image
 */

@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class BackupClient {
  private static final Log LOG = LogFactory.getLog(BackupClient.class);
  private static Options opt;
  private static Configuration conf = null;

  private BackupClient() {
    throw new AssertionError("Instantiating utility class...");
  }

  protected static void init() throws IOException {
    // define supported options
    opt = new Options();

    opt.addOption("all", false, "All tables");
    opt.addOption("debug", false, "Enable debug loggings");
    opt.addOption("t", true, "Table name");

    // create configuration instance
    conf = getConf();

    // disable irrelevant loggers to avoid it mess up command output
    disableUselessLoggers();

  }


  public static void main(String[] args) throws IOException {
    init();
    parseAndRun(args);
    System.exit(0);
  }

  /**
   * Set the configuration from a given one.
   * @param newConf A new given configuration
   */
  public synchronized static void setConf(Configuration newConf) {
    conf = newConf;
    BackupUtil.setConf(newConf);
  }

  public static Configuration getConf() {
    if (conf == null) {
      conf = BackupUtil.getConf();
    }
    return conf;
  }

  private static void disableUselessLoggers() {
    // disable zookeeper log to avoid it mess up command output
    Logger zkLogger = Logger.getLogger("org.apache.zookeeper");
    LOG.debug("Zookeeper log level before set: " + zkLogger.getLevel());
    zkLogger.setLevel(Level.OFF);
    LOG.debug("Zookeeper log level after set: " + zkLogger.getLevel());

    // disable hbase zookeeper tool log to avoid it mess up command output
    Logger hbaseZkLogger = Logger.getLogger("org.apache.hadoop.hbase.zookeeper");
    LOG.debug("HBase zookeeper log level before set: " + hbaseZkLogger.getLevel());
    hbaseZkLogger.setLevel(Level.OFF);
    LOG.debug("HBase Zookeeper log level after set: " + hbaseZkLogger.getLevel());

    // disable hbase client log to avoid it mess up command output
    Logger hbaseClientLogger = Logger.getLogger("org.apache.hadoop.hbase.client");
    LOG.debug("HBase client log level before set: " + hbaseClientLogger.getLevel());
    hbaseClientLogger.setLevel(Level.OFF);
    LOG.debug("HBase client log level after set: " + hbaseClientLogger.getLevel());
  }

  private static void parseAndRun(String[] args) throws IOException {

    String cmd = null;
    String[] remainArgs = null;
    if (args == null || args.length == 0) {
      BackupCommands.createCommand(BackupRestoreConstants.BACKUP_COMMAND.HELP, null).execute();
    } else {
      cmd = args[0];
      remainArgs = new String[args.length - 1];
      if (args.length > 1) {
        System.arraycopy(args, 1, remainArgs, 0, args.length - 1);
      }
    }
    CommandLine cmdline = null;
    try {
      cmdline = new PosixParser().parse(opt, remainArgs);
    } catch (ParseException e) {
      LOG.error("Could not parse command", e);
      System.exit(-1);
    }

    BACKUP_COMMAND type = BACKUP_COMMAND.HELP;
    if (BACKUP_COMMAND.CREATE.name().equalsIgnoreCase(cmd)) {
      type = BACKUP_COMMAND.CREATE;
    } else if (BACKUP_COMMAND.HELP.name().equalsIgnoreCase(cmd)) {
      type = BACKUP_COMMAND.HELP;
    } else {
      System.out.println("Unsupported command for backup: " + cmd);
    }

    // enable debug logging
    Logger backupClientLogger = Logger.getLogger("org.apache.hadoop.hbase.backup");
    if (cmdline.hasOption("debug")) {
      backupClientLogger.setLevel(Level.DEBUG);
    } else {
      backupClientLogger.setLevel(Level.INFO);
    }

    BackupCommands.createCommand(type, cmdline).execute();
  }

  /**
   * Send backup request to server, and monitor the progress if necessary
   * @param backupType : full or incremental
   * @param backupRootPath : the rooPath specified by user
   * @param tableListStr : the table list specified by user
   * @param snapshot : using existing snapshot if specified by user (in future jira)
   * @return backupId backup id
   * @throws IOException exception
   * @throws KeeperException excpetion
   */
  public static String create(String backupType, String backupRootPath, String tableListStr,
      String snapshot) throws IOException {

    String backupId = BackupRestoreConstants.BACKUPID_PREFIX + EnvironmentEdgeManager.currentTime();

    // check target path first, confirm it doesn't exist before backup
    boolean isTargetExist = false;
    try {
      isTargetExist = HBackupFileSystem.checkPathExist(backupRootPath, conf);
    } catch (IOException e) {
      String expMsg = e.getMessage();
      String newMsg = null;
      if (expMsg.contains("No FileSystem for scheme")) {
        newMsg =
            "Unsupported filesystem scheme found in the backup target url. Error Message: "
                + newMsg;
        LOG.error(newMsg);
        throw new IOException(newMsg);
      } else {
        throw e;
      }
    } catch (RuntimeException e) {
      LOG.error(e.getMessage());
      throw e;
    }

    if (isTargetExist) {
      LOG.info("Using existing backup root dir: " + backupRootPath);
    } else {
      LOG.info("Backup root dir " + backupRootPath + " does not exist. Will be created.");
    }

    // table list specified for backup, trigger backup on specified tables
    String tableList = tableListStr;
    // (tableListStr == null) ? null : tableListStr.replaceAll(
    // BackupRestoreConstants.TABLENAME_DELIMITER_IN_COMMAND,
    // BackupRestoreConstants.TABLENAME_DELIMITER_IN_ZNODE);
    try {
      requestBackup(backupId, backupType, tableList, backupRootPath, snapshot);
    } catch (RuntimeException e) {
      String errMsg = e.getMessage();
      if (errMsg != null
          && (errMsg.startsWith("Non-existing tables found") || errMsg
              .startsWith("Snapshot is not found"))) {
        LOG.error(errMsg + ", please check your command");
        throw e;
      } else {
        throw e;
      }
    }
    return backupId;
  }

  /**
   * Prepare and submit Backup request
   * @param backupId : backup_timestame (something like backup_1398729212626)
   * @param backupType : full or incremental
   * @param tableList : tables to be backuped
   * @param targetRootDir : specified by user
   * @param snapshot : use existing snapshot if specified by user (for future jira)
   * @throws IOException exception
   */
  protected static void requestBackup(String backupId, String backupType, String tableList,
      String targetRootDir, String snapshot) throws IOException {

    Configuration conf = getConf();
    BackupManager backupManager = null;
    BackupContext backupContext = null;
    if (snapshot != null) {
      LOG.warn("Snapshot option specified, backup type and table option will be ignored,\n"
          + "full backup will be taken based on the given snapshot.");
      throw new IOException("backup using existing Snapshot will be implemented in future jira");
    }

    HBaseAdmin hbadmin = null;
    Connection conn = null;
    try {

      backupManager = new BackupManager(conf);
      String tables = tableList;
      if (backupType.equals(BackupRestoreConstants.BACKUP_TYPE_INCR)) {
        Set<String> incrTableSet = backupManager.getIncrementalBackupTableSet();
        if (incrTableSet.isEmpty()) {
          LOG.warn("Incremental backup table set contains no table.\n"
              + "Use 'backup create full' or 'backup stop' to \n "
              + "change the tables covered by incremental backup.");
          throw new RuntimeException("No table covered by incremental backup.");
        }
        StringBuilder sb = new StringBuilder();
        for (String tableName : incrTableSet) {
          sb.append(tableName + " ");
        }
        LOG.info("Incremental backup for the following table set: " + sb.toString());
        tables =
            sb.toString().trim()
            .replaceAll(" ", BackupRestoreConstants.TABLENAME_DELIMITER_IN_COMMAND);
      }

      // check whether table exists first before starting real request
      if (tables != null) {
        String[] tableNames = tables.split(BackupRestoreConstants.TABLENAME_DELIMITER_IN_COMMAND);
        ArrayList<String> noneExistingTableList = null;
        conn = ConnectionFactory.createConnection(conf);
        hbadmin = (HBaseAdmin) conn.getAdmin();
        for (String tableName : tableNames) {
          if (!hbadmin.tableExists(TableName.valueOf(tableName))) {
            if (noneExistingTableList == null) {
              noneExistingTableList = new ArrayList<String>();
            }
            noneExistingTableList.add(tableName);
          }
        }
        if (noneExistingTableList != null) {
          if (backupType.equals(BackupRestoreConstants.BACKUP_TYPE_INCR)) {
            LOG.warn("Incremental backup table set contains no-exising table: "
                + noneExistingTableList);
          } else {
            // Throw exception only in full mode - we try to backup non-existing table
            throw new RuntimeException("Non-existing tables found in the table list: "
                + noneExistingTableList);
          }
        }
      }

      // if any target table backup dir already exist, then no backup action taken
      String[] tableNames = null;
      if (tables != null && !tables.equals("")) {
        tableNames = tables.split(BackupRestoreConstants.TABLENAME_DELIMITER_IN_COMMAND);
      }
      if (tableNames != null && tableNames.length > 0) {
        for (String table : tableNames) {
          String targetTableBackupDir =
              HBackupFileSystem.getTableBackupDir(targetRootDir, backupId, table);
          Path targetTableBackupDirPath = new Path(targetTableBackupDir);
          FileSystem outputFs = FileSystem.get(targetTableBackupDirPath.toUri(), conf);
          if (outputFs.exists(targetTableBackupDirPath)) {
            throw new IOException("Target backup directory " + targetTableBackupDir
              + " exists already.");
          }
        }
      }
      backupContext =
          backupManager.createBackupContext(backupId, backupType, tables, targetRootDir, snapshot);
      backupManager.initialize();
      backupManager.dispatchRequest(backupContext);
    } catch (BackupException e) {
      // suppress the backup exception wrapped within #initialize or #dispatchRequest, backup
      // exception has already been handled normally
      StackTraceElement[] stes = e.getStackTrace();
      for (StackTraceElement ste : stes) {
        LOG.info(ste);
      }
      LOG.error("Backup Exception " + e.getMessage());
    } finally {
      if (hbadmin != null) {
        hbadmin.close();
      }
      if (conn != null) {
        conn.close();
      }
    }
  }
}
