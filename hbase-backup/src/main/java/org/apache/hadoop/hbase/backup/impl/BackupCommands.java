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
package org.apache.hadoop.hbase.backup.impl;

import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.CONF_CONTINUOUS_BACKUP_PITR_WINDOW_DAYS;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.DEFAULT_CONTINUOUS_BACKUP_PITR_WINDOW_DAYS;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_BACKUP_LIST_DESC;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_BANDWIDTH;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_BANDWIDTH_DESC;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_DEBUG;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_DEBUG_DESC;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_ENABLE_CONTINUOUS_BACKUP;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_ENABLE_CONTINUOUS_BACKUP_DESC;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_FORCE_DELETE;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_FORCE_DELETE_DESC;
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
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_SET_BACKUP_DESC;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_SET_DESC;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_TABLE;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_TABLE_DESC;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_TABLE_LIST_DESC;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_WORKERS;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_WORKERS_DESC;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_YARN_QUEUE_NAME;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_YARN_QUEUE_NAME_DESC;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupAdmin;
import org.apache.hadoop.hbase.backup.BackupInfo;
import org.apache.hadoop.hbase.backup.BackupInfo.BackupState;
import org.apache.hadoop.hbase.backup.BackupRequest;
import org.apache.hadoop.hbase.backup.BackupRestoreConstants;
import org.apache.hadoop.hbase.backup.BackupRestoreConstants.BackupCommand;
import org.apache.hadoop.hbase.backup.BackupType;
import org.apache.hadoop.hbase.backup.HBackupFileSystem;
import org.apache.hadoop.hbase.backup.util.BackupSet;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Splitter;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.hbase.thirdparty.org.apache.commons.cli.HelpFormatter;
import org.apache.hbase.thirdparty.org.apache.commons.cli.Options;

/**
 * General backup commands, options and usage messages
 */
@InterfaceAudience.Private
public final class BackupCommands {
  public final static String INCORRECT_USAGE = "Incorrect usage";

  public final static String TOP_LEVEL_NOT_ALLOWED =
    "Top level (root) folder is not allowed to be a backup destination";

  public static final String USAGE = "Usage: hbase backup COMMAND [command-specific arguments]\n"
    + "where COMMAND is one of:\n" + "  create     create a new backup image\n"
    + "  delete     delete an existing backup image\n"
    + "  describe   show the detailed information of a backup image\n"
    + "  history    show history of all successful backups\n"
    + "  progress   show the progress of the latest backup request\n"
    + "  set        backup set management\n" + "  repair     repair backup system table\n"
    + "  merge      merge backup images\n"
    + "Run \'hbase backup COMMAND -h\' to see help message for each command\n";

  public static final String CREATE_CMD_USAGE =
    "Usage: hbase backup create <type> <backup_path> [options]\n"
      + "  type           \"full\" to create a full backup image\n"
      + "                 \"incremental\" to create an incremental backup image\n"
      + "  backup_path     Full path to store the backup image\n";

  public static final String PROGRESS_CMD_USAGE = "Usage: hbase backup progress <backup_id>\n"
    + "  backup_id       Backup image id (optional). If no id specified, the command will show\n"
    + "                  progress for currently running backup session.";
  public static final String NO_INFO_FOUND = "No info was found for backup id: ";
  public static final String NO_ACTIVE_SESSION_FOUND = "No active backup sessions found.";

  public static final String DESCRIBE_CMD_USAGE =
    "Usage: hbase backup describe <backup_id>\n" + "  backup_id       Backup image id\n";

  public static final String HISTORY_CMD_USAGE = "Usage: hbase backup history [options]";

  public static final String DELETE_CMD_USAGE = "Usage: hbase backup delete [options]";

  public static final String REPAIR_CMD_USAGE = "Usage: hbase backup repair\n";

  public static final String SET_CMD_USAGE = "Usage: hbase backup set COMMAND [name] [tables]\n"
    + "  name            Backup set name\n" + "  tables          Comma separated list of tables.\n"
    + "COMMAND is one of:\n" + "  add             add tables to a set, create a set if needed\n"
    + "  remove          remove tables from a set\n"
    + "  list            list all backup sets in the system\n" + "  describe        describe set\n"
    + "  delete          delete backup set\n";
  public static final String MERGE_CMD_USAGE = "Usage: hbase backup merge [backup_ids]\n"
    + "  backup_ids      Comma separated list of backup image ids.\n";

  public static final String USAGE_FOOTER = "";

  public static abstract class Command extends Configured {
    CommandLine cmdline;
    Connection conn;

    Command(Configuration conf) {
      if (conf == null) {
        conf = HBaseConfiguration.create();
      }
      setConf(conf);
    }

    public void execute() throws IOException {
      if (cmdline.hasOption("h") || cmdline.hasOption("help")) {
        printUsage();
        throw new IOException(INCORRECT_USAGE);
      }

      if (cmdline.hasOption(OPTION_YARN_QUEUE_NAME)) {
        String queueName = cmdline.getOptionValue(OPTION_YARN_QUEUE_NAME);
        // Set MR job queuename to configuration
        getConf().set("mapreduce.job.queuename", queueName);
      }

      // Create connection
      conn = ConnectionFactory.createConnection(getConf());
      if (requiresNoActiveSession()) {
        // Check active session
        try (BackupSystemTable table = new BackupSystemTable(conn)) {
          List<BackupInfo> sessions = table.getBackupInfos(BackupState.RUNNING);

          if (sessions.size() > 0) {
            System.err.println("Found backup session in a RUNNING state: ");
            System.err.println(sessions.get(0));
            System.err.println("This may indicate that a previous session has failed abnormally.");
            System.err.println("In this case, backup recovery is recommended.");
            throw new IOException("Active session found, aborted command execution");
          }
        }
      }
      if (requiresConsistentState()) {
        // Check failed delete
        try (BackupSystemTable table = new BackupSystemTable(conn)) {
          String[] ids = table.getListOfBackupIdsFromDeleteOperation();

          if (ids != null && ids.length > 0) {
            System.err.println("Found failed backup DELETE coommand. ");
            System.err.println("Backup system recovery is required.");
            throw new IOException("Failed backup DELETE found, aborted command execution");
          }

          ids = table.getListOfBackupIdsFromMergeOperation();
          if (ids != null && ids.length > 0) {
            System.err.println("Found failed backup MERGE coommand. ");
            System.err.println("Backup system recovery is required.");
            throw new IOException("Failed backup MERGE found, aborted command execution");
          }
        }
      }
    }

    public void finish() throws IOException {
      if (conn != null) {
        conn.close();
      }
    }

    protected abstract void printUsage();

    /**
     * The command can't be run if active backup session is in progress
     * @return true if no active sessions are in progress
     */
    protected boolean requiresNoActiveSession() {
      return false;
    }

    /**
     * Command requires consistent state of a backup system Backup system may become inconsistent
     * because of an abnormal termination of a backup session or delete command
     * @return true, if yes
     */
    protected boolean requiresConsistentState() {
      return false;
    }
  }

  private BackupCommands() {
    throw new AssertionError("Instantiating utility class...");
  }

  public static Command createCommand(Configuration conf, BackupCommand type, CommandLine cmdline) {
    Command cmd;
    switch (type) {
      case CREATE:
        cmd = new CreateCommand(conf, cmdline);
        break;
      case DESCRIBE:
        cmd = new DescribeCommand(conf, cmdline);
        break;
      case PROGRESS:
        cmd = new ProgressCommand(conf, cmdline);
        break;
      case DELETE:
        cmd = new DeleteCommand(conf, cmdline);
        break;
      case HISTORY:
        cmd = new HistoryCommand(conf, cmdline);
        break;
      case SET:
        cmd = new BackupSetCommand(conf, cmdline);
        break;
      case REPAIR:
        cmd = new RepairCommand(conf, cmdline);
        break;
      case MERGE:
        cmd = new MergeCommand(conf, cmdline);
        break;
      case HELP:
      default:
        cmd = new HelpCommand(conf, cmdline);
        break;
    }
    return cmd;
  }

  static int numOfArgs(String[] args) {
    if (args == null) {
      return 0;
    }

    return args.length;
  }

  public static class CreateCommand extends Command {
    CreateCommand(Configuration conf, CommandLine cmdline) {
      super(conf);
      this.cmdline = cmdline;
    }

    @Override
    protected boolean requiresNoActiveSession() {
      return true;
    }

    @Override
    protected boolean requiresConsistentState() {
      return true;
    }

    @Override
    public void execute() throws IOException {
      if (cmdline == null || cmdline.getArgs() == null) {
        printUsage();
        throw new IOException(INCORRECT_USAGE);
      }
      String[] args = cmdline.getArgs();
      if (args.length != 3) {
        printUsage();
        throw new IOException(INCORRECT_USAGE);
      }

      if (
        !BackupType.FULL.toString().equalsIgnoreCase(args[1])
          && !BackupType.INCREMENTAL.toString().equalsIgnoreCase(args[1])
      ) {
        System.out.println("ERROR: invalid backup type: " + args[1]);
        printUsage();
        throw new IOException(INCORRECT_USAGE);
      }
      if (!verifyPath(args[2])) {
        System.out.println("ERROR: invalid backup destination: " + args[2]);
        printUsage();
        throw new IOException(INCORRECT_USAGE);
      }
      String targetBackupDir = args[2];
      // Check if backup destination is top level (root) folder - not allowed
      if (isRootFolder(targetBackupDir)) {
        throw new IOException(TOP_LEVEL_NOT_ALLOWED);
      }
      String tables;

      // Check if we have both: backup set and list of tables
      if (cmdline.hasOption(OPTION_TABLE) && cmdline.hasOption(OPTION_SET)) {
        System.out
          .println("ERROR: You can specify either backup set or list" + " of tables, but not both");
        printUsage();
        throw new IOException(INCORRECT_USAGE);
      }
      // Creates connection
      super.execute();
      // Check backup set
      String setName = null;
      if (cmdline.hasOption(OPTION_SET)) {
        setName = cmdline.getOptionValue(OPTION_SET);
        tables = getTablesForSet(setName);

        if (tables == null) {
          System.out
            .println("ERROR: Backup set '" + setName + "' is either empty or does not exist");
          printUsage();
          throw new IOException(INCORRECT_USAGE);
        }
      } else {
        tables = cmdline.getOptionValue(OPTION_TABLE);
      }
      int bandwidth = cmdline.hasOption(OPTION_BANDWIDTH)
        ? Integer.parseInt(cmdline.getOptionValue(OPTION_BANDWIDTH))
        : -1;
      int workers = cmdline.hasOption(OPTION_WORKERS)
        ? Integer.parseInt(cmdline.getOptionValue(OPTION_WORKERS))
        : -1;

      boolean ignoreChecksum = cmdline.hasOption(OPTION_IGNORECHECKSUM);

      BackupType backupType = BackupType.valueOf(args[1].toUpperCase());
      List<TableName> tableNameList = null;
      if (tables != null) {
        tableNameList = Lists.newArrayList(BackupUtils.parseTableNames(tables));
      }
      boolean continuousBackup = cmdline.hasOption(OPTION_ENABLE_CONTINUOUS_BACKUP);
      if (continuousBackup && !BackupType.FULL.equals(backupType)) {
        System.out.println("ERROR: Continuous backup can Only be specified for Full Backup");
        printUsage();
        throw new IOException(INCORRECT_USAGE);
      }

      /*
       * The `continuousBackup` flag is specified only during the first full backup to initiate
       * continuous WAL replication. After that, it is redundant because the tables are already set
       * up for continuous backup. If the `continuousBackup` flag is not explicitly enabled, we need
       * to determine the backup mode based on the current state of the specified tables: - If all
       * the specified tables are already part of continuous backup, we treat the request as a
       * continuous backup request and proceed accordingly (since these tables are already
       * continuously backed up, no additional setup is needed). - If none of the specified tables
       * are part of continuous backup, we treat the request as a normal full backup without
       * continuous backup. - If the request includes a mix of tables—some with continuous backup
       * enabled and others without—we cannot determine a clear backup strategy. In this case, we
       * throw an error. If all tables are already in continuous backup mode, we explicitly set the
       * `continuousBackup` flag to `true` so that the request is processed using the continuous
       * backup approach rather than the normal full backup flow.
       */
      if (!continuousBackup && tableNameList != null && !tableNameList.isEmpty()) {
        try (BackupSystemTable backupSystemTable = new BackupSystemTable(conn)) {
          Set<TableName> continuousBackupTableSet =
            backupSystemTable.getContinuousBackupTableSet().keySet();

          boolean allTablesInContinuousBackup = continuousBackupTableSet.containsAll(tableNameList);
          boolean noTablesInContinuousBackup =
            tableNameList.stream().noneMatch(continuousBackupTableSet::contains);

          // Ensure that all tables are either fully in continuous backup or not at all
          if (!allTablesInContinuousBackup && !noTablesInContinuousBackup) {
            System.err
              .println("ERROR: Some tables are already in continuous backup, while others are not. "
                + "Cannot mix both in a single request.");
            printUsage();
            throw new IOException(INCORRECT_USAGE);
          }

          // If all tables are already in continuous backup, enable the flag
          if (allTablesInContinuousBackup) {
            continuousBackup = true;
          }
        }
      }

      try (BackupAdminImpl admin = new BackupAdminImpl(conn)) {
        BackupRequest.Builder builder = new BackupRequest.Builder();
        BackupRequest request = builder.withBackupType(backupType).withTableList(tableNameList)
          .withTargetRootDir(targetBackupDir).withTotalTasks(workers)
          .withBandwidthPerTasks(bandwidth).withNoChecksumVerify(ignoreChecksum)
          .withBackupSetName(setName).withContinuousBackupEnabled(continuousBackup).build();
        String backupId = admin.backupTables(request);
        System.out.println("Backup session " + backupId + " finished. Status: SUCCESS");
      } catch (IOException e) {
        System.out.println("Backup session finished. Status: FAILURE");
        throw e;
      }
    }

    private boolean isRootFolder(String targetBackupDir) {
      Path p = new Path(targetBackupDir);
      return p.isRoot();
    }

    private boolean verifyPath(String path) {
      try {
        Path p = new Path(path);
        Configuration conf = getConf() != null ? getConf() : HBaseConfiguration.create();
        URI uri = p.toUri();

        if (uri.getScheme() == null) {
          return false;
        }

        FileSystem.get(uri, conf);
        return true;
      } catch (Exception e) {
        return false;
      }
    }

    private String getTablesForSet(String name) throws IOException {
      try (final BackupSystemTable table = new BackupSystemTable(conn)) {
        List<TableName> tables = table.describeBackupSet(name);

        if (tables == null) {
          return null;
        }

        return StringUtils.join(tables, BackupRestoreConstants.TABLENAME_DELIMITER_IN_COMMAND);
      }
    }

    @Override
    protected void printUsage() {
      System.out.println(CREATE_CMD_USAGE);
      Options options = new Options();
      options.addOption(OPTION_WORKERS, true, OPTION_WORKERS_DESC);
      options.addOption(OPTION_BANDWIDTH, true, OPTION_BANDWIDTH_DESC);
      options.addOption(OPTION_SET, true, OPTION_SET_BACKUP_DESC);
      options.addOption(OPTION_TABLE, true, OPTION_TABLE_LIST_DESC);
      options.addOption(OPTION_YARN_QUEUE_NAME, true, OPTION_YARN_QUEUE_NAME_DESC);
      options.addOption(OPTION_DEBUG, false, OPTION_DEBUG_DESC);
      options.addOption(OPTION_IGNORECHECKSUM, false, OPTION_IGNORECHECKSUM_DESC);
      options.addOption(OPTION_ENABLE_CONTINUOUS_BACKUP, false,
        OPTION_ENABLE_CONTINUOUS_BACKUP_DESC);

      HelpFormatter helpFormatter = new HelpFormatter();
      helpFormatter.setLeftPadding(2);
      helpFormatter.setDescPadding(8);
      helpFormatter.setWidth(100);
      helpFormatter.setSyntaxPrefix("Options:");
      helpFormatter.printHelp(" ", null, options, USAGE_FOOTER);
    }
  }

  public static class HelpCommand extends Command {
    HelpCommand(Configuration conf, CommandLine cmdline) {
      super(conf);
      this.cmdline = cmdline;
    }

    @Override
    public void execute() throws IOException {
      if (cmdline == null) {
        printUsage();
        throw new IOException(INCORRECT_USAGE);
      }

      String[] args = cmdline.getArgs();
      if (args == null || args.length == 0) {
        printUsage();
        throw new IOException(INCORRECT_USAGE);
      }

      if (args.length != 2) {
        System.out.println("ERROR: Only supports help message of a single command type");
        printUsage();
        throw new IOException(INCORRECT_USAGE);
      }

      String type = args[1];

      if (BackupCommand.CREATE.name().equalsIgnoreCase(type)) {
        System.out.println(CREATE_CMD_USAGE);
      } else if (BackupCommand.DESCRIBE.name().equalsIgnoreCase(type)) {
        System.out.println(DESCRIBE_CMD_USAGE);
      } else if (BackupCommand.HISTORY.name().equalsIgnoreCase(type)) {
        System.out.println(HISTORY_CMD_USAGE);
      } else if (BackupCommand.PROGRESS.name().equalsIgnoreCase(type)) {
        System.out.println(PROGRESS_CMD_USAGE);
      } else if (BackupCommand.DELETE.name().equalsIgnoreCase(type)) {
        System.out.println(DELETE_CMD_USAGE);
      } else if (BackupCommand.SET.name().equalsIgnoreCase(type)) {
        System.out.println(SET_CMD_USAGE);
      } else {
        System.out.println("Unknown command : " + type);
        printUsage();
      }
    }

    @Override
    protected void printUsage() {
      System.out.println(USAGE);
    }
  }

  public static class DescribeCommand extends Command {
    DescribeCommand(Configuration conf, CommandLine cmdline) {
      super(conf);
      this.cmdline = cmdline;
    }

    @Override
    public void execute() throws IOException {
      if (cmdline == null || cmdline.getArgs() == null) {
        printUsage();
        throw new IOException(INCORRECT_USAGE);
      }
      String[] args = cmdline.getArgs();
      if (args.length != 2) {
        printUsage();
        throw new IOException(INCORRECT_USAGE);
      }

      super.execute();

      String backupId = args[1];
      try (final BackupSystemTable sysTable = new BackupSystemTable(conn)) {
        BackupInfo info = sysTable.readBackupInfo(backupId);
        if (info == null) {
          System.out.println("ERROR: " + backupId + " does not exist");
          printUsage();
          throw new IOException(INCORRECT_USAGE);
        }
        System.out.println(info.getShortDescription());
      }
    }

    @Override
    protected void printUsage() {
      System.out.println(DESCRIBE_CMD_USAGE);
    }
  }

  public static class ProgressCommand extends Command {
    ProgressCommand(Configuration conf, CommandLine cmdline) {
      super(conf);
      this.cmdline = cmdline;
    }

    @Override
    public void execute() throws IOException {

      if (cmdline == null || cmdline.getArgs() == null || cmdline.getArgs().length == 1) {
        System.out.println(
          "No backup id was specified, " + "will retrieve the most recent (ongoing) session");
      }
      String[] args = cmdline == null ? null : cmdline.getArgs();
      if (args != null && args.length > 2) {
        System.err.println("ERROR: wrong number of arguments: " + args.length);
        printUsage();
        throw new IOException(INCORRECT_USAGE);
      }

      super.execute();

      String backupId = (args == null || args.length <= 1) ? null : args[1];
      try (final BackupSystemTable sysTable = new BackupSystemTable(conn)) {
        BackupInfo info = null;

        if (backupId != null) {
          info = sysTable.readBackupInfo(backupId);
        } else {
          List<BackupInfo> infos = sysTable.getBackupInfos(BackupState.RUNNING);
          if (infos != null && infos.size() > 0) {
            info = infos.get(0);
            backupId = info.getBackupId();
            System.out.println("Found ongoing session with backupId=" + backupId);
          }
        }
        int progress = info == null ? -1 : info.getProgress();
        if (progress < 0) {
          if (backupId != null) {
            System.out.println(NO_INFO_FOUND + backupId);
          } else {
            System.err.println(NO_ACTIVE_SESSION_FOUND);
          }
        } else {
          System.out.println(backupId + " progress=" + progress + "%");
        }
      }
    }

    @Override
    protected void printUsage() {
      System.out.println(PROGRESS_CMD_USAGE);
    }
  }

  public static class DeleteCommand extends Command {
    DeleteCommand(Configuration conf, CommandLine cmdline) {
      super(conf);
      this.cmdline = cmdline;
    }

    @Override
    protected boolean requiresNoActiveSession() {
      return true;
    }

    @Override
    public void execute() throws IOException {

      if (cmdline == null || cmdline.getArgs() == null || cmdline.getArgs().length < 1) {
        printUsage();
        throw new IOException(INCORRECT_USAGE);
      }

      if (!cmdline.hasOption(OPTION_KEEP) && !cmdline.hasOption(OPTION_LIST)) {
        printUsage();
        throw new IOException(INCORRECT_USAGE);
      }

      boolean isForceDelete = cmdline.hasOption(OPTION_FORCE_DELETE);
      super.execute();
      if (cmdline.hasOption(OPTION_KEEP)) {
        executeDeleteOlderThan(cmdline, isForceDelete);
      } else if (cmdline.hasOption(OPTION_LIST)) {
        executeDeleteListOfBackups(cmdline, isForceDelete);
      }
    }

    private void executeDeleteOlderThan(CommandLine cmdline, boolean isForceDelete)
      throws IOException {
      String value = cmdline.getOptionValue(OPTION_KEEP);
      int days = 0;
      try {
        days = Integer.parseInt(value);
      } catch (NumberFormatException e) {
        throw new IOException(value + " is not an integer number");
      }
      final long fdays = days;
      BackupInfo.Filter dateFilter = new BackupInfo.Filter() {
        @Override
        public boolean apply(BackupInfo info) {
          long currentTime = EnvironmentEdgeManager.currentTime();
          long maxTsToDelete = currentTime - fdays * 24 * 3600 * 1000;
          return info.getCompleteTs() <= maxTsToDelete;
        }
      };
      List<BackupInfo> history = null;
      try (final BackupSystemTable sysTable = new BackupSystemTable(conn);
        BackupAdminImpl admin = new BackupAdminImpl(conn)) {
        history = sysTable.getBackupHistory(-1, dateFilter);
        String[] backupIds = convertToBackupIds(history);
        validatePITRBackupDeletion(backupIds, isForceDelete);
        int deleted = admin.deleteBackups(backupIds);
        System.out.println("Deleted " + deleted + " backups. Total older than " + days + " days: "
          + backupIds.length);
      } catch (IOException e) {
        System.err.println("Delete command FAILED. Please run backup repair tool to restore backup "
          + "system integrity");
        throw e;
      }
    }

    private String[] convertToBackupIds(List<BackupInfo> history) {
      String[] ids = new String[history.size()];
      for (int i = 0; i < ids.length; i++) {
        ids[i] = history.get(i).getBackupId();
      }
      return ids;
    }

    private void executeDeleteListOfBackups(CommandLine cmdline, boolean isForceDelete)
      throws IOException {
      String value = cmdline.getOptionValue(OPTION_LIST);
      String[] backupIds = value.split(",");
      validatePITRBackupDeletion(backupIds, isForceDelete);
      try (BackupAdminImpl admin = new BackupAdminImpl(conn)) {
        int deleted = admin.deleteBackups(backupIds);
        System.out.println("Deleted " + deleted + " backups. Total requested: " + backupIds.length);
      } catch (IOException e) {
        System.err.println("Delete command FAILED. Please run backup repair tool to restore backup "
          + "system integrity");
        throw e;
      }

    }

    /**
     * Validates whether the specified backups can be deleted while preserving Point-In-Time
     * Recovery (PITR) capabilities. If a backup is the only remaining full backup enabling PITR for
     * certain tables, deletion is prevented unless forced.
     * @param backupIds     Array of backup IDs to validate.
     * @param isForceDelete Flag indicating whether deletion should proceed regardless of PITR
     *                      constraints.
     * @throws IOException If a backup is essential for PITR and force deletion is not enabled.
     */
    private void validatePITRBackupDeletion(String[] backupIds, boolean isForceDelete)
      throws IOException {
      if (!isForceDelete) {
        for (String backupId : backupIds) {
          List<TableName> affectedTables = getTablesDependentOnBackupForPITR(backupId);
          if (!affectedTables.isEmpty()) {
            String errMsg = String.format(
              "Backup %s is the only FULL backup remaining that enables PITR for tables: %s. "
                + "Use the force option to delete it anyway.",
              backupId, affectedTables);
            System.err.println(errMsg);
            throw new IOException(errMsg);
          }
        }
      }
    }

    /**
     * Identifies tables that rely on the specified backup for PITR (Point-In-Time Recovery). A
     * table is considered dependent on the backup if it does not have any other valid full backups
     * that can cover the PITR window enabled by the specified backup.
     * @param backupId The ID of the backup being evaluated for PITR coverage.
     * @return A list of tables that are dependent on the specified backup for PITR recovery.
     * @throws IOException If there is an error retrieving the backup metadata or backup system
     *                     table.
     */
    private List<TableName> getTablesDependentOnBackupForPITR(String backupId) throws IOException {
      List<TableName> dependentTables = new ArrayList<>();

      try (final BackupSystemTable backupSystemTable = new BackupSystemTable(conn)) {
        // Fetch the target backup's info using the backup ID
        BackupInfo targetBackup = backupSystemTable.readBackupInfo(backupId);
        if (targetBackup == null) {
          throw new IOException("Backup info not found for backupId: " + backupId);
        }

        // Only full backups are mandatory for PITR
        if (!BackupType.FULL.equals(targetBackup.getType())) {
          return List.of();
        }

        // Retrieve the tables with continuous backup enabled along with their start times
        Map<TableName, Long> continuousBackupStartTimes =
          backupSystemTable.getContinuousBackupTableSet();

        // Calculate the PITR window by fetching configuration and current time
        long pitrWindowDays = getConf().getLong(CONF_CONTINUOUS_BACKUP_PITR_WINDOW_DAYS,
          DEFAULT_CONTINUOUS_BACKUP_PITR_WINDOW_DAYS);
        long currentTime = EnvironmentEdgeManager.getDelegate().currentTime();
        final long maxAllowedPITRTime = currentTime - TimeUnit.DAYS.toMillis(pitrWindowDays);

        // Check each table associated with the target backup
        for (TableName table : targetBackup.getTableNames()) {
          // Skip tables without continuous backup enabled
          if (!continuousBackupStartTimes.containsKey(table)) {
            continue;
          }

          // Calculate the PITR window this backup covers for the table
          Optional<Pair<Long, Long>> coveredPitrWindow = getCoveredPitrWindowForTable(targetBackup,
            continuousBackupStartTimes.get(table), maxAllowedPITRTime, currentTime);

          // If this backup does not cover a valid PITR window for the table, skip
          if (coveredPitrWindow.isEmpty()) {
            continue;
          }

          // Check if there is any other valid backup that can cover the PITR window
          List<BackupInfo> allBackups = backupSystemTable.getBackupInfos(BackupState.COMPLETE);
          boolean hasAnotherValidBackup =
            canAnyOtherBackupCover(allBackups, targetBackup, table, coveredPitrWindow.get(),
              continuousBackupStartTimes.get(table), maxAllowedPITRTime, currentTime);

          // If no other valid backup exists, add the table to the dependent list
          if (!hasAnotherValidBackup) {
            dependentTables.add(table);
          }
        }
      }

      return dependentTables;
    }

    /**
     * Calculates the PITR (Point-In-Time Recovery) window that the given backup enables for a
     * table.
     * @param backupInfo                Metadata of the backup being evaluated.
     * @param continuousBackupStartTime When continuous backups started for the table.
     * @param maxAllowedPITRTime        The earliest timestamp from which PITR is supported in the
     *                                  cluster.
     * @param currentTime               Current time.
     * @return Optional PITR window as a pair (start, end), or empty if backup is not useful for
     *         PITR.
     */
    private Optional<Pair<Long, Long>> getCoveredPitrWindowForTable(BackupInfo backupInfo,
      long continuousBackupStartTime, long maxAllowedPITRTime, long currentTime) {

      long backupStartTs = backupInfo.getStartTs();
      long backupEndTs = backupInfo.getCompleteTs();
      long effectiveStart = Math.max(continuousBackupStartTime, maxAllowedPITRTime);

      if (backupStartTs < continuousBackupStartTime) {
        return Optional.empty();
      }

      return Optional.of(Pair.newPair(Math.max(backupEndTs, effectiveStart), currentTime));
    }

    /**
     * Checks if any backup (excluding the current backup) can cover the specified PITR window for
     * the given table. A backup can cover the PITR window if it fully encompasses the target time
     * range specified.
     * @param allBackups                List of all backups available.
     * @param currentBackup             The current backup that should not be considered for
     *                                  coverage.
     * @param table                     The table for which we need to check backup coverage.
     * @param targetWindow              A pair representing the target PITR window (start and end
     *                                  times).
     * @param continuousBackupStartTime When continuous backups started for the table.
     * @param maxAllowedPITRTime        The earliest timestamp from which PITR is supported in the
     *                                  cluster.
     * @param currentTime               Current time.
     * @return {@code true} if any backup (excluding the current one) fully covers the target PITR
     *         window; {@code false} otherwise.
     */
    private boolean canAnyOtherBackupCover(List<BackupInfo> allBackups, BackupInfo currentBackup,
      TableName table, Pair<Long, Long> targetWindow, long continuousBackupStartTime,
      long maxAllowedPITRTime, long currentTime) {

      long targetStart = targetWindow.getFirst();
      long targetEnd = targetWindow.getSecond();

      // Iterate through all backups (including the current one)
      for (BackupInfo backup : allBackups) {
        // Skip if the backup is not full or doesn't contain the table
        if (!BackupType.FULL.equals(backup.getType())) continue;
        if (!backup.getTableNames().contains(table)) continue;

        // Skip the current backup itself
        if (backup.equals(currentBackup)) continue;

        // Get the covered PITR window for this backup
        Optional<Pair<Long, Long>> coveredWindow = getCoveredPitrWindowForTable(backup,
          continuousBackupStartTime, maxAllowedPITRTime, currentTime);

        if (coveredWindow.isPresent()) {
          Pair<Long, Long> covered = coveredWindow.get();

          // The backup must fully cover the target window
          if (covered.getFirst() <= targetStart && covered.getSecond() >= targetEnd) {
            return true;
          }
        }
      }

      return false;
    }

    @Override
    protected void printUsage() {
      System.out.println(DELETE_CMD_USAGE);
      Options options = new Options();
      options.addOption(OPTION_KEEP, true, OPTION_KEEP_DESC);
      options.addOption(OPTION_LIST, true, OPTION_BACKUP_LIST_DESC);
      options.addOption(OPTION_FORCE_DELETE, false, OPTION_FORCE_DELETE_DESC);

      HelpFormatter helpFormatter = new HelpFormatter();
      helpFormatter.setLeftPadding(2);
      helpFormatter.setDescPadding(8);
      helpFormatter.setWidth(100);
      helpFormatter.setSyntaxPrefix("Options:");
      helpFormatter.printHelp(" ", null, options, USAGE_FOOTER);

    }
  }

  public static class RepairCommand extends Command {
    RepairCommand(Configuration conf, CommandLine cmdline) {
      super(conf);
      this.cmdline = cmdline;
    }

    @Override
    public void execute() throws IOException {
      super.execute();

      String[] args = cmdline == null ? null : cmdline.getArgs();
      if (args != null && args.length > 1) {
        System.err.println("ERROR: wrong number of arguments: " + args.length);
        printUsage();
        throw new IOException(INCORRECT_USAGE);
      }

      Configuration conf = getConf() != null ? getConf() : HBaseConfiguration.create();
      try (final Connection conn = ConnectionFactory.createConnection(conf);
        final BackupSystemTable sysTable = new BackupSystemTable(conn)) {
        // Failed backup
        BackupInfo backupInfo;
        List<BackupInfo> list = sysTable.getBackupInfos(BackupState.RUNNING);
        if (list.size() == 0) {
          // No failed sessions found
          System.out.println("REPAIR status: no failed sessions found."
            + " Checking failed delete backup operation ...");
          repairFailedBackupDeletionIfAny(conn, sysTable);
          repairFailedBackupMergeIfAny(conn, sysTable);
          return;
        }
        backupInfo = list.get(0);
        // If this is a cancel exception, then we've already cleaned.
        // set the failure timestamp of the overall backup
        backupInfo.setCompleteTs(EnvironmentEdgeManager.currentTime());
        // set failure message
        backupInfo.setFailedMsg("REPAIR status: repaired after failure:\n" + backupInfo);
        // set overall backup status: failed
        backupInfo.setState(BackupState.FAILED);
        // compose the backup failed data
        String backupFailedData = "BackupId=" + backupInfo.getBackupId() + ",startts="
          + backupInfo.getStartTs() + ",failedts=" + backupInfo.getCompleteTs() + ",failedphase="
          + backupInfo.getPhase() + ",failedmessage=" + backupInfo.getFailedMsg();
        System.out.println(backupFailedData);
        TableBackupClient.cleanupAndRestoreBackupSystem(conn, backupInfo, conf);
        // If backup session is updated to FAILED state - means we
        // processed recovery already.
        sysTable.updateBackupInfo(backupInfo);
        sysTable.finishBackupExclusiveOperation();
        System.out.println("REPAIR status: finished repair failed session:\n " + backupInfo);
      }
    }

    private void repairFailedBackupDeletionIfAny(Connection conn, BackupSystemTable sysTable)
      throws IOException {
      String[] backupIds = sysTable.getListOfBackupIdsFromDeleteOperation();
      if (backupIds == null || backupIds.length == 0) {
        System.out.println("No failed backup DELETE operation found");
        // Delete backup table snapshot if exists
        BackupSystemTable.deleteSnapshot(conn);
        return;
      }
      System.out.println("Found failed DELETE operation for: " + StringUtils.join(backupIds));
      System.out.println("Running DELETE again ...");
      // Restore table from snapshot
      BackupSystemTable.restoreFromSnapshot(conn);
      // Finish previous failed session
      sysTable.finishBackupExclusiveOperation();
      try (BackupAdmin admin = new BackupAdminImpl(conn)) {
        admin.deleteBackups(backupIds);
      }
      System.out.println("DELETE operation finished OK: " + StringUtils.join(backupIds));
    }

    public static void repairFailedBackupMergeIfAny(Connection conn, BackupSystemTable sysTable)
      throws IOException {

      String[] backupIds = sysTable.getListOfBackupIdsFromMergeOperation();
      if (backupIds == null || backupIds.length == 0) {
        System.out.println("No failed backup MERGE operation found");
        // Delete backup table snapshot if exists
        BackupSystemTable.deleteSnapshot(conn);
        return;
      }
      System.out.println("Found failed MERGE operation for: " + StringUtils.join(backupIds));
      // Check if backup .tmp exists
      BackupInfo bInfo = sysTable.readBackupInfo(backupIds[0]);
      String backupRoot = bInfo.getBackupRootDir();
      FileSystem fs = FileSystem.get(new Path(backupRoot).toUri(), new Configuration());
      String backupId = BackupUtils.findMostRecentBackupId(backupIds);
      Path tmpPath = HBackupFileSystem.getBackupTmpDirPathForBackupId(backupRoot, backupId);
      if (fs.exists(tmpPath)) {
        // Move data back
        Path destPath = HBackupFileSystem.getBackupPath(backupRoot, backupId);
        if (!fs.delete(destPath, true)) {
          System.out.println("Failed to delete " + destPath);
        }
        boolean res = fs.rename(tmpPath, destPath);
        if (!res) {
          throw new IOException(
            "MERGE repair: failed  to rename from " + tmpPath + " to " + destPath);
        }
        System.out
          .println("MERGE repair: renamed from " + tmpPath + " to " + destPath + " res=" + res);
      } else {
        checkRemoveBackupImages(fs, backupRoot, backupIds);
      }
      // Restore table from snapshot
      BackupSystemTable.restoreFromSnapshot(conn);
      // Unlock backup system
      sysTable.finishBackupExclusiveOperation();
      // Finish previous failed session
      sysTable.finishMergeOperation();

      System.out.println("MERGE repair operation finished OK: " + StringUtils.join(backupIds));
    }

    private static void checkRemoveBackupImages(FileSystem fs, String backupRoot,
      String[] backupIds) throws IOException {
      String mergedBackupId = BackupUtils.findMostRecentBackupId(backupIds);
      for (String backupId : backupIds) {
        if (backupId.equals(mergedBackupId)) {
          continue;
        }
        Path path = HBackupFileSystem.getBackupPath(backupRoot, backupId);
        if (fs.exists(path)) {
          if (!fs.delete(path, true)) {
            System.out.println("MERGE repair removing: " + path + " - FAILED");
          } else {
            System.out.println("MERGE repair removing: " + path + " - OK");
          }
        }
      }
    }

    @Override
    protected void printUsage() {
      System.out.println(REPAIR_CMD_USAGE);
    }
  }

  public static class MergeCommand extends Command {
    MergeCommand(Configuration conf, CommandLine cmdline) {
      super(conf);
      this.cmdline = cmdline;
    }

    @Override
    protected boolean requiresNoActiveSession() {
      return true;
    }

    @Override
    protected boolean requiresConsistentState() {
      return true;
    }

    @Override
    public void execute() throws IOException {
      super.execute();

      String[] args = cmdline == null ? null : cmdline.getArgs();
      if (args == null || (args.length != 2)) {
        System.err
          .println("ERROR: wrong number of arguments: " + (args == null ? null : args.length));
        printUsage();
        throw new IOException(INCORRECT_USAGE);
      }

      String[] backupIds = args[1].split(",");
      if (backupIds.length < 2) {
        String msg = "ERROR: can not merge a single backup image. "
          + "Number of images must be greater than 1.";
        System.err.println(msg);
        throw new IOException(msg);

      }
      Configuration conf = getConf() != null ? getConf() : HBaseConfiguration.create();
      try (final Connection conn = ConnectionFactory.createConnection(conf);
        final BackupAdminImpl admin = new BackupAdminImpl(conn)) {
        admin.mergeBackups(backupIds);
      }
    }

    @Override
    protected void printUsage() {
      System.out.println(MERGE_CMD_USAGE);
    }
  }

  public static class HistoryCommand extends Command {
    private final static int DEFAULT_HISTORY_LENGTH = 10;

    HistoryCommand(Configuration conf, CommandLine cmdline) {
      super(conf);
      this.cmdline = cmdline;
    }

    @Override
    public void execute() throws IOException {
      int n = parseHistoryLength();
      final TableName tableName = getTableName();
      final String setName = getTableSetName();
      BackupInfo.Filter tableNameFilter = new BackupInfo.Filter() {
        @Override
        public boolean apply(BackupInfo info) {
          if (tableName == null) {
            return true;
          }

          List<TableName> names = info.getTableNames();
          return names.contains(tableName);
        }
      };
      BackupInfo.Filter tableSetFilter = new BackupInfo.Filter() {
        @Override
        public boolean apply(BackupInfo info) {
          if (setName == null) {
            return true;
          }

          String backupId = info.getBackupId();
          return backupId.startsWith(setName);
        }
      };
      Path backupRootPath = getBackupRootPath();
      List<BackupInfo> history;
      if (backupRootPath == null) {
        // Load from backup system table
        super.execute();
        try (final BackupSystemTable sysTable = new BackupSystemTable(conn)) {
          history = sysTable.getBackupHistory(n, tableNameFilter, tableSetFilter);
        }
      } else {
        // load from backup FS
        history =
          BackupUtils.getHistory(getConf(), n, backupRootPath, tableNameFilter, tableSetFilter);
      }
      for (BackupInfo info : history) {
        System.out.println(info.getShortDescription());
      }
    }

    private Path getBackupRootPath() throws IOException {
      String value = null;
      try {
        value = cmdline.getOptionValue(OPTION_PATH);

        if (value == null) {
          return null;
        }

        return new Path(value);
      } catch (IllegalArgumentException e) {
        System.out.println("ERROR: Illegal argument for backup root path: " + value);
        printUsage();
        throw new IOException(INCORRECT_USAGE);
      }
    }

    private TableName getTableName() throws IOException {
      String value = cmdline.getOptionValue(OPTION_TABLE);

      if (value == null) {
        return null;
      }

      try {
        return TableName.valueOf(value);
      } catch (IllegalArgumentException e) {
        System.out.println("Illegal argument for table name: " + value);
        printUsage();
        throw new IOException(INCORRECT_USAGE);
      }
    }

    private String getTableSetName() {
      return cmdline.getOptionValue(OPTION_SET);
    }

    private int parseHistoryLength() throws IOException {
      String value = cmdline.getOptionValue(OPTION_RECORD_NUMBER);
      try {
        if (value == null) {
          return DEFAULT_HISTORY_LENGTH;
        }

        return Integer.parseInt(value);
      } catch (NumberFormatException e) {
        System.out.println("Illegal argument for history length: " + value);
        printUsage();
        throw new IOException(INCORRECT_USAGE);
      }
    }

    @Override
    protected void printUsage() {
      System.out.println(HISTORY_CMD_USAGE);
      Options options = new Options();
      options.addOption(OPTION_RECORD_NUMBER, true, OPTION_RECORD_NUMBER_DESC);
      options.addOption(OPTION_PATH, true, OPTION_PATH_DESC);
      options.addOption(OPTION_TABLE, true, OPTION_TABLE_DESC);
      options.addOption(OPTION_SET, true, OPTION_SET_DESC);

      HelpFormatter helpFormatter = new HelpFormatter();
      helpFormatter.setLeftPadding(2);
      helpFormatter.setDescPadding(8);
      helpFormatter.setWidth(100);
      helpFormatter.setSyntaxPrefix("Options:");
      helpFormatter.printHelp(" ", null, options, USAGE_FOOTER);
    }
  }

  public static class BackupSetCommand extends Command {
    private final static String SET_ADD_CMD = "add";
    private final static String SET_REMOVE_CMD = "remove";
    private final static String SET_DELETE_CMD = "delete";
    private final static String SET_DESCRIBE_CMD = "describe";
    private final static String SET_LIST_CMD = "list";

    BackupSetCommand(Configuration conf, CommandLine cmdline) {
      super(conf);
      this.cmdline = cmdline;
    }

    @Override
    public void execute() throws IOException {
      // Command-line must have at least one element
      if (cmdline == null || cmdline.getArgs() == null || cmdline.getArgs().length < 2) {
        printUsage();
        throw new IOException(INCORRECT_USAGE);
      }

      String[] args = cmdline.getArgs();
      String cmdStr = args[1];
      BackupCommand cmd = getCommand(cmdStr);

      switch (cmd) {
        case SET_ADD:
          processSetAdd(args);
          break;
        case SET_REMOVE:
          processSetRemove(args);
          break;
        case SET_DELETE:
          processSetDelete(args);
          break;
        case SET_DESCRIBE:
          processSetDescribe(args);
          break;
        case SET_LIST:
          processSetList();
          break;
        default:
          break;
      }
    }

    private void processSetList() throws IOException {
      super.execute();

      // List all backup set names
      // does not expect any args
      try (BackupAdminImpl admin = new BackupAdminImpl(conn)) {
        List<BackupSet> list = admin.listBackupSets();
        for (BackupSet bs : list) {
          System.out.println(bs);
        }
      }
    }

    private void processSetDescribe(String[] args) throws IOException {
      if (args == null || args.length != 3) {
        printUsage();
        throw new IOException(INCORRECT_USAGE);
      }
      super.execute();

      String setName = args[2];
      try (final BackupSystemTable sysTable = new BackupSystemTable(conn)) {
        List<TableName> tables = sysTable.describeBackupSet(setName);
        BackupSet set = tables == null ? null : new BackupSet(setName, tables);
        if (set == null) {
          System.out.println("Set '" + setName + "' does not exist.");
        } else {
          System.out.println(set);
        }
      }
    }

    private void processSetDelete(String[] args) throws IOException {
      if (args == null || args.length != 3) {
        printUsage();
        throw new IOException(INCORRECT_USAGE);
      }
      super.execute();

      String setName = args[2];
      try (final BackupAdminImpl admin = new BackupAdminImpl(conn)) {
        boolean result = admin.deleteBackupSet(setName);
        if (result) {
          System.out.println("Delete set " + setName + " OK.");
        } else {
          System.out.println("Set " + setName + " does not exist");
        }
      }
    }

    private void processSetRemove(String[] args) throws IOException {
      if (args == null || args.length != 4) {
        printUsage();
        throw new IOException(INCORRECT_USAGE);
      }
      super.execute();

      String setName = args[2];
      String[] tables = args[3].split(",");
      TableName[] tableNames = toTableNames(tables);
      try (final BackupAdminImpl admin = new BackupAdminImpl(conn)) {
        admin.removeFromBackupSet(setName, tableNames);
      }
    }

    private TableName[] toTableNames(String[] tables) {
      TableName[] arr = new TableName[tables.length];
      for (int i = 0; i < tables.length; i++) {
        arr[i] = TableName.valueOf(tables[i]);
      }
      return arr;
    }

    private void processSetAdd(String[] args) throws IOException {
      if (args == null || args.length != 4) {
        printUsage();
        throw new IOException(INCORRECT_USAGE);
      }
      super.execute();
      String setName = args[2];
      TableName[] tableNames =
        Splitter.on(',').splitToStream(args[3]).map(TableName::valueOf).toArray(TableName[]::new);
      try (final BackupAdminImpl admin = new BackupAdminImpl(conn)) {
        admin.addToBackupSet(setName, tableNames);
      }
    }

    private BackupCommand getCommand(String cmdStr) throws IOException {
      switch (cmdStr) {
        case SET_ADD_CMD:
          return BackupCommand.SET_ADD;
        case SET_REMOVE_CMD:
          return BackupCommand.SET_REMOVE;
        case SET_DELETE_CMD:
          return BackupCommand.SET_DELETE;
        case SET_DESCRIBE_CMD:
          return BackupCommand.SET_DESCRIBE;
        case SET_LIST_CMD:
          return BackupCommand.SET_LIST;
        default:
          System.out.println("ERROR: Unknown command for 'set' :" + cmdStr);
          printUsage();
          throw new IOException(INCORRECT_USAGE);
      }
    }

    @Override
    protected void printUsage() {
      System.out.println(SET_CMD_USAGE);
    }
  }
}
