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

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.hbase.backup.BackupRestoreConstants.BACKUP_COMMAND;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * General backup commands, options and usage messages
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
final class BackupCommands {

  private static final String USAGE = "Usage: hbase backup COMMAND\n"
      + "where COMMAND is one of:\n" + "  create     create a new backup image\n"
      + "Enter \'help COMMAND\' to see help message for each command\n";

  private static final String CREATE_CMD_USAGE =
      "Usage: hbase backup create <type> <backup_root_path> [tables] [-s name] [-convert] "
          + "[-silent]\n" + " type          \"full\" to create a full backup image;\n"
          + "               \"incremental\" to create an incremental backup image\n"
          + " backup_root_path   The full root path to store the backup image,\n"
          + "                    the prefix can be gpfs, hdfs or webhdfs\n" + " Options:\n"
          + "   tables      If no tables (\"\") are specified, all tables are backed up. "
          + "Otherwise it is a\n" + "               comma separated list of tables.\n"
          + "   -s name     Use the specified snapshot for full backup\n"
          + "   -convert    For an incremental backup, convert WAL files to HFiles\n";

  interface Command {
    void execute() throws IOException;
  }

  private BackupCommands() {
    throw new AssertionError("Instantiating utility class...");
  }

  static Command createCommand(BACKUP_COMMAND type, CommandLine cmdline) {
    Command cmd = null;
    switch (type) {
      case CREATE:
        cmd = new CreateCommand(cmdline);
        break;
      case HELP:
      default:
        cmd = new HelpCommand(cmdline);
        break;
    }
    return cmd;
  }

  private static class CreateCommand implements Command {
    CommandLine cmdline;

    CreateCommand(CommandLine cmdline) {
      this.cmdline = cmdline;
    }

    @Override
    public void execute() throws IOException {
      if (cmdline == null || cmdline.getArgs() == null) {
        System.out.println("ERROR: missing arguments");
        System.out.println(CREATE_CMD_USAGE);
        System.exit(-1);
      }
      String[] args = cmdline.getArgs();
      if (args.length < 2 || args.length > 3) {
        System.out.println("ERROR: wrong number of arguments");
        System.out.println(CREATE_CMD_USAGE);
        System.exit(-1);
      }

      if (!BackupRestoreConstants.BACKUP_TYPE_FULL.equalsIgnoreCase(args[0])
          && !BackupRestoreConstants.BACKUP_TYPE_INCR.equalsIgnoreCase(args[0])) {
        System.out.println("ERROR: invalid backup type");
        System.out.println(CREATE_CMD_USAGE);
        System.exit(-1);
      }

      String snapshot = cmdline.hasOption('s') ? cmdline.getOptionValue('s') : null;
      String tables = (args.length == 3) ? args[2] : null;

      try {
        BackupClient.create(args[0], args[1], tables, snapshot);
      } catch (RuntimeException e) {
        System.out.println("ERROR: " + e.getMessage());
        System.exit(-1);
      }
    }
  }

  private static class HelpCommand implements Command {
    CommandLine cmdline;

    HelpCommand(CommandLine cmdline) {
      this.cmdline = cmdline;
    }

    @Override
    public void execute() throws IOException {
      if (cmdline == null) {
        System.out.println(USAGE);
        System.exit(0);
      }

      String[] args = cmdline.getArgs();
      if (args == null || args.length == 0) {
        System.out.println(USAGE);
        System.exit(0);
      }

      if (args.length != 1) {
        System.out.println("Only support check help message of a single command type");
        System.out.println(USAGE);
        System.exit(0);
      }

      String type = args[0];

      if (BACKUP_COMMAND.CREATE.name().equalsIgnoreCase(type)) {
        System.out.println(CREATE_CMD_USAGE);
      } // other commands will be supported in future jira
      System.exit(0);
    }
  }

}
