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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupManifest.BackupImage;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * The main class which interprets the given arguments and trigger restore operation.
 */

@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class RestoreClient {

  private static final Log LOG = LogFactory.getLog(RestoreClient.class);

  private static Options opt;
  private static Configuration conf;
  private static Set<BackupImage> lastRestoreImagesSet;

  // delimiter in tablename list in restore command
  private static final String DELIMITER_IN_COMMAND = ",";

  private static final String OPTION_OVERWRITE = "overwrite";
  private static final String OPTION_CHECK = "check";
  private static final String OPTION_AUTOMATIC = "automatic";

  private static final String USAGE =
      "Usage: hbase restore <backup_root_path> <backup_id> <tables> [tableMapping] \n"
          + "       [-overwrite] [-check] [-automatic]\n"
          + " backup_root_path  The parent location where the backup images are stored\n"
          + " backup_id         The id identifying the backup image\n"
          + " table(s)          Table(s) from the backup image to be restored.\n"
          + "                   Tables are separated by comma.\n"
          + " Options:\n"
          + "   tableMapping    A comma separated list of target tables.\n"
          + "                   If specified, each table in <tables> must have a mapping.\n"
          + "   -overwrite      With this option, restore overwrites to the existing table "
          + "if there's any in\n"
          + "                   restore target. The existing table must be online before restore.\n"
          + "   -check          With this option, restore sequence and dependencies are checked\n"
          + "                   and verified without executing the restore\n"
          + "   -automatic      With this option, all the dependencies are automatically restored\n"
          + "                   together with this backup image following the correct order.\n"
          + "                   The restore dependencies can be checked by using \"-check\" "
          + "option,\n"
          + "                   or using \"hbase backup describe\" command. Without this option, "
          + "only\n" + "                   this backup image is restored\n";

  private RestoreClient(){
    throw new AssertionError("Instantiating utility class...");
  }

  protected static void init() throws IOException {
    // define supported options
    opt = new Options();
    opt.addOption(OPTION_OVERWRITE, false,
        "Overwrite the data if any of the restore target tables exists");
    opt.addOption(OPTION_CHECK, false, "Check restore sequence and dependencies");
    opt.addOption(OPTION_AUTOMATIC, false, "Restore all dependencies");
    opt.addOption("debug", false, "Enable debug logging");

    conf = getConf();

    // disable irrelevant loggers to avoid it mess up command output
    disableUselessLoggers();
  }

  public static void main(String[] args) throws IOException {
    init();
    parseAndRun(args);
  }

  private static void parseAndRun(String[] args) {
    CommandLine cmd = null;
    try {
      cmd = new PosixParser().parse(opt, args);
    } catch (ParseException e) {
      LOG.error("Could not parse command", e);
      System.exit(-1);
    }

    // enable debug logging
    Logger backupClientLogger = Logger.getLogger("org.apache.hadoop.hbase.backup");
    if (cmd.hasOption("debug")) {
      backupClientLogger.setLevel(Level.DEBUG);
    }

    // whether to overwrite to existing table if any, false by default
    boolean isOverwrite = cmd.hasOption(OPTION_OVERWRITE);
    if (isOverwrite) {
      LOG.debug("Found -overwrite option in restore command, "
          + "will overwrite to existing table if any in the restore target");
    }

    // whether to only check the dependencies, false by default
    boolean check = cmd.hasOption(OPTION_CHECK);
    if (check) {
      LOG.debug("Found -check option in restore command, "
          + "will check and verify the dependencies");
    }

    // whether to restore all dependencies, false by default
    boolean autoRestore = cmd.hasOption(OPTION_AUTOMATIC);
    if (autoRestore) {
      LOG.debug("Found -automatic option in restore command, "
          + "will automatically retore all the dependencies");
    }

    // parse main restore command options
    String[] remainArgs = cmd.getArgs();
    if (remainArgs.length < 3) {
      System.out.println("ERROR: missing arguments");
      System.out.println(USAGE);
      System.exit(-1);
    }

    String backupRootDir = remainArgs[0];
    String backupId = remainArgs[1];
    String tables = remainArgs[2];

    String tableMapping = (remainArgs.length > 3) ? remainArgs[3] : null;

    String[] sTableArray = (tables != null) ? tables.split(DELIMITER_IN_COMMAND) : null;
    String[] tTableArray = (tableMapping != null) ? tableMapping.split(DELIMITER_IN_COMMAND) : null;

    if (tableMapping != null && tTableArray != null && (sTableArray.length != tTableArray.length)) {
      System.err.println("ERROR: table mapping mismatch: " + tables + " : " + tableMapping);
      System.out.println(USAGE);
      System.exit(-1);
    }

    try {
      HBackupFileSystem hBackupFS = new HBackupFileSystem(conf, new Path(backupRootDir), backupId);
      restore_stage1(hBackupFS, backupRootDir, backupId, check, autoRestore, sTableArray,
        tTableArray, isOverwrite);
    } catch (IOException e) {
      System.err.println("ERROR: " + e.getMessage());
      System.exit(-1);
    }
  }

  /**
   * Restore operation. Stage 1: validate backupManifest, and check target tables
   * @param hBackupFS to access the backup image
   * @param backupRootDir The root dir for backup image
   * @param backupId The backup id for image to be restored
   * @param check True if only do dependency check
   * @param autoRestore True if automatically restore following the dependency
   * @param sTableArray The array of tables to be restored
   * @param tTableArray The array of mapping tables to restore to
   * @param isOverwrite True then do restore overwrite if target table exists, otherwise fail the
   *          request if target table exists
   * @return True if only do dependency check
   * @throws IOException if any failure during restore
   */
  public static boolean restore_stage1(HBackupFileSystem hBackupFS, String backupRootDir,
      String backupId, boolean check, boolean autoRestore, String[] sTableArray,
      String[] tTableArray, boolean isOverwrite) throws IOException {

    HashMap<String, BackupManifest> backupManifestMap = new HashMap<String, BackupManifest>();
    // check and load backup image manifest for the tables
    hBackupFS.checkImageManifestExist(backupManifestMap, sTableArray);

    try {
      // Check and validate the backup image and its dependencies
      if (check || autoRestore) {
        if (validate(backupManifestMap)) {
          LOG.info("Checking backup images: ok");
        } else {
          String errMsg = "Some dependencies are missing for restore";
          LOG.error(errMsg);
          throw new IOException(errMsg);
        }
      }

      // return true if only for check
      if (check) {
        return true;
      }

      if (tTableArray == null) {
        tTableArray = sTableArray;
      }

      // check the target tables
      checkTargetTables(tTableArray, isOverwrite);

      // start restore process
      Set<BackupImage> restoreImageSet =
          restore_stage2(hBackupFS, backupManifestMap, sTableArray, tTableArray, autoRestore);

      LOG.info("Restore for " + Arrays.asList(sTableArray) + " are successful!");
      lastRestoreImagesSet = restoreImageSet;

    } catch (IOException e) {
      LOG.error("ERROR: restore failed with error: " + e.getMessage());
      throw e;
    }

    // not only for check, return false
    return false;
  }

  /**
   * Get last restore image set. The value is globally set for the latest finished restore.
   * @return the last restore image set
   */
  public static Set<BackupImage> getLastRestoreImagesSet() {
    return lastRestoreImagesSet;
  }

  private static boolean validate(HashMap<String, BackupManifest> backupManifestMap)
      throws IOException {
    boolean isValid = true;

    for (Entry<String, BackupManifest> manifestEntry : backupManifestMap.entrySet()) {

      String table = manifestEntry.getKey();
      TreeSet<BackupImage> imageSet = new TreeSet<BackupImage>();

      ArrayList<BackupImage> depList = manifestEntry.getValue().getDependentListByTable(table);
      if (depList != null && !depList.isEmpty()) {
        imageSet.addAll(depList);
      }

      // todo merge
      LOG.debug("merge will be implemented in future jira");
      // BackupUtil.clearMergedImages(table, imageSet, conf);

      LOG.info("Dependent image(s) from old to new:");
      for (BackupImage image : imageSet) {
        String imageDir =
            HBackupFileSystem.getTableBackupDir(image.getRootDir(), image.getBackupId(), table);
        if (!HBackupFileSystem.checkPathExist(imageDir, getConf())) {
          LOG.error("ERROR: backup image does not exist: " + imageDir);
          isValid = false;
          break;
        }
        // TODO More validation?
        LOG.info("Backup image: " + image.getBackupId() + " for '" + table + "' is available");
      }
    }
    return isValid;
  }

  /**
   * Validate target Tables
   * @param tTableArray: target tables
   * @param isOverwrite overwrite existing table
   * @throws IOException exception
   */
  private static void checkTargetTables(String[] tTableArray, boolean isOverwrite)
      throws IOException {
    ArrayList<String> existTableList = new ArrayList<String>();
    ArrayList<String> disabledTableList = new ArrayList<String>();

    // check if the tables already exist
    HBaseAdmin admin = null;
    Connection conn = null;
    try {
      conn = ConnectionFactory.createConnection(conf);
      admin = (HBaseAdmin) conn.getAdmin();
      for (String tableName : tTableArray) {
        if (admin.tableExists(TableName.valueOf(tableName))) {
          existTableList.add(tableName);
          if (admin.isTableDisabled(TableName.valueOf(tableName))) {
            disabledTableList.add(tableName);
          }
        } else {
          LOG.info("HBase table " + tableName
              + " does not exist. It will be create during backup process");
        }
      }
    } finally {
      if (admin != null) {
        admin.close();
      }
      if (conn != null) {
        conn.close();
      }
    }

    if (existTableList.size() > 0) {
      if (!isOverwrite) {
        LOG.error("Existing table found in the restore target, please add \"-overwrite\" "
            + "option in the command if you mean to restore to these existing tables");
        LOG.info("Existing table list in restore target: " + existTableList);
        throw new IOException("Existing table found in target while no \"-overwrite\" "
            + "option found");
      } else {
        if (disabledTableList.size() > 0) {
          LOG.error("Found offline table in the restore target, "
              + "please enable them before restore with \"-overwrite\" option");
          LOG.info("Offline table list in restore target: " + disabledTableList);
          throw new IOException(
              "Found offline table in the target when restore with \"-overwrite\" option");
        }
      }
    }

  }

  /**
   * Restore operation. Stage 2: resolved Backup Image dependency
   * @param hBackupFS to access the backup image
   * @param backupManifestMap : tableName,  Manifest
   * @param sTableArray The array of tables to be restored
   * @param tTableArray The array of mapping tables to restore to
   * @param autoRestore : yes, restore all the backup images on the dependency list
   * @return set of BackupImages restored
   * @throws IOException exception
   */
  private static Set<BackupImage> restore_stage2(HBackupFileSystem hBackupFS,
    HashMap<String, BackupManifest> backupManifestMap, String[] sTableArray,
    String[] tTableArray, boolean autoRestore) throws IOException {
    TreeSet<BackupImage> restoreImageSet = new TreeSet<BackupImage>();

    for (int i = 0; i < sTableArray.length; i++) {
      restoreImageSet.clear();
      String table = sTableArray[i];
      BackupManifest manifest = backupManifestMap.get(table);
      if (autoRestore) {
        // Get the image list of this backup for restore in time order from old
        // to new.
        TreeSet<BackupImage> restoreList =
            new TreeSet<BackupImage>(manifest.getDependentListByTable(table));
        LOG.debug("need to clear merged Image. to be implemented in future jira");

        for (BackupImage image : restoreList) {
          restoreImage(image, table, tTableArray[i]);
        }
        restoreImageSet.addAll(restoreList);
      } else {
        BackupImage image = manifest.getBackupImage();
        List<BackupImage> depList = manifest.getDependentListByTable(table);
        // The dependency list always contains self.
        if (depList != null && depList.size() > 1) {
          LOG.warn("Backup image " + image.getBackupId() + " depends on other images.\n"
              + "this operation will only restore the delta contained within backupImage "
              + image.getBackupId());
        }
        restoreImage(image, table, tTableArray[i]);
        restoreImageSet.add(image);
      }

      if (autoRestore) {
        if (restoreImageSet != null && !restoreImageSet.isEmpty()) {
          LOG.info("Restore includes the following image(s):");
          for (BackupImage image : restoreImageSet) {
            LOG.info("  Backup: "
                + image.getBackupId()
                + " "
                + HBackupFileSystem.getTableBackupDir(image.getRootDir(), image.getBackupId(),
                  table));
          }
        }
      }

    }
    return restoreImageSet;
  }

  /**
   * Restore operation handle each backupImage
   * @param image: backupImage
   * @param sTable: table to be restored
   * @param tTable: table to be restored to
   * @throws IOException exception
   */
  private static void restoreImage(BackupImage image, String sTable, String tTable)
      throws IOException {

    Configuration conf = getConf();

    String rootDir = image.getRootDir();
    LOG.debug("Image root dir " + rootDir);
    String backupId = image.getBackupId();

    HBackupFileSystem hFS = new HBackupFileSystem(conf, new Path(rootDir), backupId);
    RestoreUtil restoreTool = new RestoreUtil(conf, hFS);
    BackupManifest manifest = hFS.getManifest(sTable);

    Path tableBackupPath = hFS.getTableBackupPath(sTable);

    // todo: convert feature will be provided in a future jira
    boolean converted = false;

    if (manifest.getType().equals(BackupRestoreConstants.BACKUP_TYPE_FULL) || converted) {
      LOG.info("Restoring '" + sTable + "' to '" + tTable + "' from "
          + (converted ? "converted" : "full") + " backup image " + tableBackupPath.toString());
      restoreTool.fullRestoreTable(tableBackupPath, sTable, tTable, converted);
    } else { // incremental Backup
      String logBackupDir =
          HBackupFileSystem.getLogBackupDir(image.getRootDir(), image.getBackupId());
      LOG.info("Restoring '" + sTable + "' to '" + tTable + "' from incremental backup image "
          + logBackupDir);
      restoreTool.incrementalRestoreTable(logBackupDir, new String[] { sTable },
        new String[] { tTable });
    }

    LOG.info(sTable + " has been successfully restored to " + tTable);
  }

  /**
   * Set the configuration from a given one.
   * @param newConf A new given configuration
   */
  public synchronized static void setConf(Configuration newConf) {
    conf = newConf;
  }

  /**
   * Get and merge Hadoop and HBase configuration.
   * @throws IOException exception
   */
  protected static Configuration getConf() {
    if (conf == null) {
      synchronized (RestoreClient.class) {
        conf = new Configuration();
        HBaseConfiguration.merge(conf, HBaseConfiguration.create());
      }
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

    // disable other related log to avoid mess up command output
    Logger otherLogger = Logger.getLogger("org.apache.hadoop.hbase.io.hfile");
    otherLogger.setLevel(Level.OFF);
    otherLogger = Logger.getLogger("org.apache.hadoop.hbase.util");
    otherLogger.setLevel(Level.OFF);
    otherLogger = Logger.getLogger("org.apache.hadoop.hbase.mapreduce");
    otherLogger.setLevel(Level.OFF);
  }
}
