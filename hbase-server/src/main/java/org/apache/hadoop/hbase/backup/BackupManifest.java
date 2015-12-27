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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.util.FSUtils;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Backup manifest Contains all the meta data of a backup image. The manifest info will be bundled
 * as manifest file together with data. So that each backup image will contain all the info needed
 * for restore.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BackupManifest {

  private static final Log LOG = LogFactory.getLog(BackupManifest.class);

  // manifest file name
  public static final String FILE_NAME = ".backup.manifest";

  // manifest file version, current is 1.0
  public static final String MANIFEST_VERSION = "1.0";

  // tags of fields for manifest file
  public static final String TAG_VERSION = "Manifest-Version";
  public static final String TAG_BACKUPID = "Backup-Id";
  public static final String TAG_BACKUPTYPE = "Backup-Type";
  public static final String TAG_TABLESET = "Table-Set";
  public static final String TAG_STARTTS = "Start-Timestamp";
  public static final String TAG_COMPLETETS = "Complete-Timestamp";
  public static final String TAG_TABLEBYTES = "Total-Table-Bytes";
  public static final String TAG_LOGBYTES = "Total-Log-Bytes";
  public static final String TAG_INCRTIMERANGE = "Incremental-Time-Range";
  public static final String TAG_DEPENDENCY = "Dependency";
  public static final String TAG_IMAGESTATE = "Image-State";
  public static final String TAG_COMPACTION = "Compaction";

  public static final String ERROR_DEPENDENCY = "DEPENDENCY_ERROR";

  public static final int DELETE_SUCCESS = 0;
  public static final int DELETE_FAILED = -1;

  // currently only one state, will have CONVERTED, and MERGED in future JIRA
  public static final String IMAGE_STATE_ORIG = "ORIGINAL";
  public static final String IMAGE_STATE_CONVERT = "CONVERTED";
  public static final String IMAGE_STATE_MERGE = "MERGED";
  public static final String IMAGE_STATE_CONVERT_MERGE = "CONVERTED,MERGED";

  // backup image, the dependency graph is made up by series of backup images

  public static class BackupImage implements Comparable<BackupImage> {

    private String backupId;
    private String type;
    private String rootDir;
    private String tableSet;
    private long startTs;
    private long completeTs;
    private ArrayList<BackupImage> ancestors;

    public BackupImage() {
      super();
    }

    public BackupImage(String backupId, String type, String rootDir, String tableSet, long startTs,
        long completeTs) {
      this.backupId = backupId;
      this.type = type;
      this.rootDir = rootDir;
      this.tableSet = tableSet;
      this.startTs = startTs;
      this.completeTs = completeTs;
    }

    public String getBackupId() {
      return backupId;
    }

    public void setBackupId(String backupId) {
      this.backupId = backupId;
    }

    public String getType() {
      return type;
    }

    public void setType(String type) {
      this.type = type;
    }

    public String getRootDir() {
      return rootDir;
    }

    public void setRootDir(String rootDir) {
      this.rootDir = rootDir;
    }

    public String getTableSet() {
      return tableSet;
    }

    public void setTableSet(String tableSet) {
      this.tableSet = tableSet;
    }

    public long getStartTs() {
      return startTs;
    }

    public void setStartTs(long startTs) {
      this.startTs = startTs;
    }

    public long getCompleteTs() {
      return completeTs;
    }

    public void setCompleteTs(long completeTs) {
      this.completeTs = completeTs;
    }

    public ArrayList<BackupImage> getAncestors() {
      if (this.ancestors == null) {
        this.ancestors = new ArrayList<BackupImage>();
      }
      return this.ancestors;
    }

    public void addAncestor(BackupImage backupImage) {
      this.getAncestors().add(backupImage);
    }

    public boolean hasAncestor(String token) {
      for (BackupImage image : this.getAncestors()) {
        if (image.getBackupId().equals(token)) {
          return true;
        }
      }
      return false;
    }

    public boolean hasTable(String table) {
      String[] tables = this.getTableSet().split(";");
      for (String t : tables) {
        if (t.equals(table)) {
          return true;
        }
      }
      return false;
    }

    @Override
    public int compareTo(BackupImage other) {
      String thisBackupId = this.getBackupId();
      String otherBackupId = other.getBackupId();
      Long thisTS = new Long(thisBackupId.substring(thisBackupId.lastIndexOf("_") + 1));
      Long otherTS = new Long(otherBackupId.substring(otherBackupId.lastIndexOf("_") + 1));
      return thisTS.compareTo(otherTS);
    }
  }

  // manifest version
  private String version = MANIFEST_VERSION;

  // hadoop hbase configuration
  protected Configuration config = null;

  // backup root directory
  private String rootDir = null;

  // backup image directory
  private String tableBackupDir = null;

  // backup log directory if this is an incremental backup
  private String logBackupDir = null;

  // backup token
  private String token;

  // backup type, full or incremental
  private String type;

  // the table set for the backup
  private ArrayList<String> tableSet;

  // actual start timestamp of the backup process
  private long startTs;

  // actual complete timestamp of the backup process
  private long completeTs;

  // total bytes for table backup image
  private long tableBytes;

  // total bytes for the backed-up logs for incremental backup
  private long logBytes;

  // the region server timestamp for tables:
  // <table, <rs, timestamp>>
  private Map<String, HashMap<String, String>> incrTimeRanges;

  // dependency of this backup, including all the dependent images to do PIT recovery
  private Map<String, BackupImage> dependency;

  // the state of backup image
  private String imageState;

  // the indicator of the image compaction
  private boolean isCompacted = false;

  // the merge chain of the original backups, null if not a merged backup
  private LinkedList<String> mergeChain;

  /**
   * Construct manifest for a ongoing backup.
   * @param backupCtx The ongoing backup context
   */
  public BackupManifest(BackupContext backupCtx) {
    this.token = backupCtx.getBackupId();
    this.type = backupCtx.getType();
    this.rootDir = backupCtx.getTargetRootDir();
    if (this.type.equals(BackupRestoreConstants.BACKUP_TYPE_INCR)) {
      this.logBackupDir = backupCtx.getHLogTargetDir();
      this.logBytes = backupCtx.getTotalBytesCopied();
    }
    this.startTs = backupCtx.getStartTs();
    this.completeTs = backupCtx.getEndTs();
    this.loadTableSet(backupCtx.getTableListAsString());
    this.setImageOriginal();
  }

  /**
   * Construct a table level manifest for a backup of the named table.
   * @param backupCtx The ongoing backup context
   */
  public BackupManifest(BackupContext backupCtx, String table) {
    this.token = backupCtx.getBackupId();
    this.type = backupCtx.getType();
    this.rootDir = backupCtx.getTargetRootDir();
    this.tableBackupDir = backupCtx.getBackupStatus(table).getTargetDir();
    if (this.type.equals(BackupRestoreConstants.BACKUP_TYPE_INCR)) {
      this.logBackupDir = backupCtx.getHLogTargetDir();
      this.logBytes = backupCtx.getTotalBytesCopied();
    }
    this.startTs = backupCtx.getStartTs();
    this.completeTs = backupCtx.getEndTs();
    this.loadTableSet(table);
    this.setImageOriginal();
  }

  /**
   * Construct manifest from a backup directory.
   * @param conf configuration
   * @param backupPath backup path
   * @throws BackupException exception
   */
  public BackupManifest(Configuration conf, Path backupPath) throws BackupException {

    LOG.debug("Loading manifest from: " + backupPath.toString());
    // The input backupDir may not exactly be the backup table dir.
    // It could be the backup log dir where there is also a manifest file stored.
    // This variable's purpose is to keep the correct and original location so
    // that we can store/persist it.
    this.tableBackupDir = backupPath.toString();
    this.config = conf;
    try {

      FileSystem fs = backupPath.getFileSystem(conf);
      FileStatus[] subFiles = FSUtils.listStatus(fs, backupPath);
      if (subFiles == null) {
        String errorMsg = backupPath.toString() + " does not exist";
        LOG.error(errorMsg);
        throw new IOException(errorMsg);
      }
      for (FileStatus subFile : subFiles) {
        if (subFile.getPath().getName().equals(FILE_NAME)) {

          // load and set manifest field from file content
          FSDataInputStream in = fs.open(subFile.getPath());
          Properties props = new Properties();
          try {
            props.load(in);
          } catch (IOException e) {
            LOG.error("Error when loading from manifest file!");
            throw e;
          } finally {
            in.close();
          }

          this.version = props.getProperty(TAG_VERSION);
          this.token = props.getProperty(TAG_BACKUPID);
          this.type = props.getProperty(TAG_BACKUPTYPE);
          // Here the parameter backupDir is where the manifest file is.
          // There should always be a manifest file under:
          // backupRootDir/namespace/table/backupId/.backup.manifest
          this.rootDir = backupPath.getParent().getParent().getParent().toString();

          Path p = backupPath.getParent();
          if (p.getName().equals(HConstants.HREGION_LOGDIR_NAME)) {
            this.rootDir = p.getParent().toString();
          } else {
            this.rootDir = p.getParent().getParent().toString();
          }

          this.loadTableSet(props.getProperty(TAG_TABLESET));

          this.startTs = Long.parseLong(props.getProperty(TAG_STARTTS));
          this.completeTs = Long.parseLong(props.getProperty(TAG_COMPLETETS));
          this.tableBytes = Long.parseLong(props.getProperty(TAG_TABLEBYTES));
          if (this.type.equals(BackupRestoreConstants.BACKUP_TYPE_INCR)) {
            this.logBytes = (Long.parseLong(props.getProperty(TAG_LOGBYTES)));
            LOG.debug("convert will be implemented by future jira");
          }
          this.loadIncrementalTimeRanges(props.getProperty(TAG_INCRTIMERANGE));
          this.loadDependency(props.getProperty(TAG_DEPENDENCY));
          this.imageState = props.getProperty(TAG_IMAGESTATE);
          this.isCompacted =
              props.getProperty(TAG_COMPACTION).equalsIgnoreCase("TRUE") ? true : false;
          LOG.debug("merge and from existing snapshot will be implemented by future jira");
          LOG.debug("Loaded manifest instance from manifest file: "
              + FSUtils.getPath(subFile.getPath()));
          return;
        }
      }
      String errorMsg = "No manifest file found in: " + backupPath.toString();
      LOG.error(errorMsg);
      throw new IOException(errorMsg);

    } catch (IOException e) {
      throw new BackupException(e.getMessage());
    }
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  /**
   * Load table set from a table set list string (t1;t2;t3;...).
   * @param tableSetStr Table set list string
   */
  private void loadTableSet(String tableSetStr) {

    LOG.debug("Loading table set: " + tableSetStr);

    String[] tableSet = tableSetStr.split(";");
    this.tableSet = this.getTableSet();
    if (this.tableSet.size() > 0) {
      this.tableSet.clear();
    }
    for (int i = 0; i < tableSet.length; i++) {
      this.tableSet.add(tableSet[i]);
    }

    LOG.debug(tableSet.length + " tables exist in table set.");
  }

  public void setImageOriginal() {
    this.imageState = IMAGE_STATE_ORIG;
  }

  /**
   * Get the table set of this image.
   * @return The table set list
   */
  public ArrayList<String> getTableSet() {
    if (this.tableSet == null) {
      this.tableSet = new ArrayList<String>();
    }
    return this.tableSet;
  }

  /**
   * Persist the manifest file.
   * @throws IOException IOException when storing the manifest file.
   */
  public void store(Configuration conf) throws BackupException {
    Properties props = new Properties();
    props.setProperty(TAG_VERSION, this.version);
    props.setProperty(TAG_BACKUPID, this.token);
    props.setProperty(TAG_BACKUPTYPE, this.type);
    props.setProperty(TAG_TABLESET, this.getTableSetStr());
    LOG.debug("convert will be supported in future jira");
    // String convertedTables = this.getConvertedTableSetStr();
    // if (convertedTables != null )
    // props.setProperty(TAG_CONVERTEDTABLESET, convertedTables);
    props.setProperty(TAG_STARTTS, Long.toString(this.startTs));
    props.setProperty(TAG_COMPLETETS, Long.toString(this.completeTs));
    props.setProperty(TAG_TABLEBYTES, Long.toString(this.tableBytes));
    if (this.type.equals(BackupRestoreConstants.BACKUP_TYPE_INCR)) {
      props.setProperty(TAG_LOGBYTES, Long.toString(this.logBytes));
    }
    props.setProperty(TAG_INCRTIMERANGE, this.getIncrTimestampStr());
    props.setProperty(TAG_DEPENDENCY, this.getDependencyStr());
    props.setProperty(TAG_IMAGESTATE, this.getImageState());
    props.setProperty(TAG_COMPACTION, this.isCompacted ? "TRUE" : "FALSE");
    LOG.debug("merge will be supported in future jira");
    // props.setProperty(TAG_MERGECHAIN, this.getMergeChainStr());
    LOG.debug("backup from existing snapshot will be supported in future jira");
    // props.setProperty(TAG_FROMSNAPSHOT, this.isFromSnapshot() ? "TRUE" : "FALSE");

    // write the file, overwrite if already exist
    Path manifestFilePath =
        new Path((this.tableBackupDir != null ? this.tableBackupDir : this.logBackupDir)
            + File.separator + FILE_NAME);
    try {
      FSDataOutputStream out = manifestFilePath.getFileSystem(conf).create(manifestFilePath, true);
      props.store(out, "HBase backup manifest.");
      out.close();
    } catch (IOException e) {
      throw new BackupException(e.getMessage());
    }

    LOG.debug("Manifest file stored to " + this.tableBackupDir != null ? this.tableBackupDir
        : this.logBackupDir + File.separator + FILE_NAME);
  }

  /**
   * Get the table set string in the format of t1;t2;t3...
   */
  private String getTableSetStr() {
    return BackupUtil.concat(getTableSet(), ";");
  }

  public String getImageState() {
    return imageState;
  }

  public String getVersion() {
    return version;
  }

  /**
   * Get this backup image.
   * @return the backup image.
   */
  public BackupImage getBackupImage() {
    return this.getDependency().get(this.token);
  }

  /**
   * Add dependent backup image for this backup.
   * @param image The direct dependent backup image
   */
  public void addDependentImage(BackupImage image) {
    this.getDependency().get(this.token).addAncestor(image);
    this.setDependencyMap(this.getDependency(), image);
  }

  /**
   * Get the dependency' string in the json format.
   */
  private String getDependencyStr() {
    BackupImage thisImage = this.getDependency().get(this.token);
    if (thisImage == null) {
      LOG.warn("There is no dependency set yet.");
      return null;
    }

    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.writeValueAsString(thisImage);
    } catch (JsonGenerationException e) {
      LOG.error("Error when generating dependency string from backup image.", e);
      return ERROR_DEPENDENCY;
    } catch (JsonMappingException e) {
      LOG.error("Error when generating dependency string from backup image.", e);
      return ERROR_DEPENDENCY;
    } catch (IOException e) {
      LOG.error("Error when generating dependency string from backup image.", e);
      return ERROR_DEPENDENCY;
    }
  }

  /**
   * Get all dependent backup images. The image of this backup is also contained.
   * @return The dependent backup images map
   */
  public Map<String, BackupImage> getDependency() {
    if (this.dependency == null) {
      this.dependency = new HashMap<String, BackupImage>();
      LOG.debug(this.rootDir + " " + this.token + " " + this.type);
      this.dependency.put(this.token,
        new BackupImage(this.token, this.type, this.rootDir, this.getTableSetStr(), this.startTs,
            this.completeTs));
    }
    return this.dependency;
  }

  /**
   * Set the incremental timestamp map directly.
   * @param incrTimestampMap timestamp map
   */
  public void setIncrTimestampMap(HashMap<String, HashMap<String, String>> incrTimestampMap) {
    this.incrTimeRanges = incrTimestampMap;
  }

  /**
   * Get the incremental time range string in the format of:
   * t1,rs1:ts,rs2:ts,...;t2,rs1:ts,rs2:ts,...;t3,rs1:ts,rs2:ts,...
   */
  private String getIncrTimestampStr() {
    StringBuilder sb = new StringBuilder();
    for (Entry<String, HashMap<String, String>> tableEntry : this.getIncrTimestamps().entrySet()) {
      sb.append(tableEntry.getKey() + ","); // table
      for (Entry<String, String> rsEntry : tableEntry.getValue().entrySet()) {
        sb.append(rsEntry.getKey() + ":"); // region server
        sb.append(rsEntry.getValue() + ","); // timestamp
      }
      if (sb.length() > 1 && sb.charAt(sb.length() - 1) == ',') {
        sb.deleteCharAt(sb.length() - 1);
      }
      sb.append(";");
    }
    if (sb.length() > 1 && sb.charAt(sb.length() - 1) == ';') {
      sb.deleteCharAt(sb.length() - 1);
    }
    return sb.toString();
  }

  public Map<String, HashMap<String, String>> getIncrTimestamps() {
    if (this.incrTimeRanges == null) {
      this.incrTimeRanges = new HashMap<String, HashMap<String, String>>();
    }
    return this.incrTimeRanges;
  }

  /**
   * Load incremental timestamps from a given string, and store them in the collection. The
   * timestamps in string is in the format of
   * t1,rs1:ts,rs2:ts,...;t2,rs1:ts,rs2:ts,...;t3,rs1:ts,rs2:ts,...
   * @param timeRangesInStr Incremental time ranges in string
   */
  private void loadIncrementalTimeRanges(String timeRangesStr) throws IOException {

    LOG.debug("Loading table's incremental time ranges of region servers from string in manifest: "
        + timeRangesStr);

    Map<String, HashMap<String, String>> timeRangeMap = this.getIncrTimestamps();

    String[] entriesOfTables = timeRangesStr.split(";");
    for (int i = 0; i < entriesOfTables.length; i++) {
      String[] itemsForTable = entriesOfTables[i].split(",");

      // validate the incremental timestamps string format for a table:
      // t1,rs1:ts,rs2:ts,...
      if (itemsForTable.length < 1) {
        String errorMsg = "Wrong incremental time range format: " + timeRangesStr;
        LOG.error(errorMsg);
        throw new IOException(errorMsg);
      }

      HashMap<String, String> rsTimestampMap = new HashMap<String, String>();
      for (int j = 1; j < itemsForTable.length; j++) {
        String[] rsTsEntry = itemsForTable[j].split(":");

        // validate the incremental timestamps string format for a region server:
        // rs1:ts
        if (rsTsEntry.length != 2) {
          String errorMsg = "Wrong incremental timestamp format: " + itemsForTable[j];
          LOG.error(errorMsg);
          throw new IOException(errorMsg);
        }

        // an entry for timestamp of a region server
        rsTimestampMap.put(rsTsEntry[0], rsTsEntry[1]);
      }

      timeRangeMap.put(itemsForTable[0], rsTimestampMap);
    }

    // all entries have been loaded
    LOG.debug(entriesOfTables.length + " tables' incremental time ranges have been loaded.");
  }

  /**
   * Get the image list of this backup for restore in time order.
   * @param reverse If true, then output in reverse order, otherwise in time order from old to new
   * @return the backup image list for restore in time order
   */
  public ArrayList<BackupImage> getRestoreDependentList(boolean reverse) {
    TreeMap<Long, BackupImage> restoreImages = new TreeMap<Long, BackupImage>();
    for (BackupImage image : this.getDependency().values()) {
      restoreImages.put(Long.valueOf(image.startTs), image);
    }
    return new ArrayList<BackupImage>(reverse ? (restoreImages.descendingMap().values())
        : (restoreImages.values()));
  }

  /**
   * Get the dependent image list for a specific table of this backup in time order from old to new
   * if want to restore to this backup image level.
   * @param table table
   * @return the backup image list for a table in time order
   */
  public ArrayList<BackupImage> getDependentListByTable(String table) {
    ArrayList<BackupImage> tableImageList = new ArrayList<BackupImage>();
    ArrayList<BackupImage> imageList = getRestoreDependentList(true);
    for (BackupImage image : imageList) {
      if (image.hasTable(table)) {
        tableImageList.add(image);
        if (image.getType().equals(BackupRestoreConstants.BACKUP_TYPE_FULL)) {
          break;
        }
      }
    }
    Collections.reverse(tableImageList);
    return tableImageList;
  }

  /**
   * Get the full dependent image list in the whole dependency scope for a specific table of this
   * backup in time order from old to new.
   * @param table table
   * @return the full backup image list for a table in time order in the whole scope of the
   *         dependency of this image
   */
  public ArrayList<BackupImage> getAllDependentListByTable(String table) {
    ArrayList<BackupImage> tableImageList = new ArrayList<BackupImage>();
    ArrayList<BackupImage> imageList = getRestoreDependentList(false);
    for (BackupImage image : imageList) {
      if (image.hasTable(table)) {
        tableImageList.add(image);
      }
    }
    return tableImageList;
  }

  /**
   * Load dependency from a dependency json string.
   * @param dependencyStr The dependency string
   * @throws IOException exception
   */
  private void loadDependency(String dependencyStr) throws IOException {

    LOG.debug("Loading dependency: " + dependencyStr);

    String msg = "Dependency is broken in the manifest.";
    if (dependencyStr.equals(ERROR_DEPENDENCY)) {
      throw new IOException(msg);
    }

    ObjectMapper mapper = new ObjectMapper();
    BackupImage image = null;
    try {
      image = mapper.readValue(dependencyStr, BackupImage.class);
    } catch (JsonParseException e) {
      LOG.error(msg);
      throw new IOException(e.getMessage());
    } catch (JsonMappingException e) {
      LOG.error(msg);
      throw new IOException(e.getMessage());
    } catch (IOException e) {
      LOG.error(msg);
      throw new IOException(e.getMessage());
    }
    LOG.debug("Manifest's current backup image information:");
    LOG.debug("  Token: " + image.getBackupId());
    LOG.debug("  Backup directory: " + image.getRootDir());
    this.setDependencyMap(this.getDependency(), image);

    LOG.debug("Dependent images map:");
    for (Entry<String, BackupImage> entry : this.getDependency().entrySet()) {
      LOG.debug("  " + entry.getKey() + " : " + entry.getValue().getBackupId() + " -- "
          + entry.getValue().getRootDir());
    }

    LOG.debug("Dependency has been loaded.");
  }

  /**
   * Recursively set the dependency map of the backup images.
   * @param map The dependency map
   * @param image The backup image
   */
  private void setDependencyMap(Map<String, BackupImage> map, BackupImage image) {
    if (image == null) {
      return;
    } else {
      map.put(image.getBackupId(), image);
      for (BackupImage img : image.getAncestors()) {
        setDependencyMap(map, img);
      }
    }
  }

  /**
   * Check whether backup image1 could cover backup image2 or not.
   * @param image1 backup image 1
   * @param image2 backup image 2
   * @return true if image1 can cover image2, otherwise false
   */
  public static boolean canCoverImage(BackupImage image1, BackupImage image2) {
    // image1 can cover image2 only when the following conditions are satisfied:
    // - image1 must not be an incremental image;
    // - image1 must be taken after image2 has been taken;
    // - table set of image1 must cover the table set of image2.
    if (image1.getType().equals(BackupRestoreConstants.BACKUP_TYPE_INCR)) {
      return false;
    }
    if (image1.getStartTs() < image2.getStartTs()) {
      return false;
    }
    String[] image1TableSet = image1.getTableSet().split(";");
    String[] image2TableSet = image2.getTableSet().split(";");
    boolean found = false;
    for (int i = 0; i < image2TableSet.length; i++) {
      found = false;
      for (int j = 0; j < image1TableSet.length; j++) {
        if (image2TableSet[i].equals(image1TableSet[j])) {
          found = true;
          break;
        }
      }
      if (!found) {
        return false;
      }
    }

    LOG.debug("Backup image " + image1.getBackupId() + " can cover " + image2.getBackupId());
    return true;
  }

  /**
   * Check whether backup image set could cover a backup image or not.
   * @param fullImages The backup image set
   * @param image The target backup image
   * @return true if fullImages can cover image, otherwise false
   */
  public static boolean canCoverImage(ArrayList<BackupImage> fullImages, BackupImage image) {
    // fullImages can cover image only when the following conditions are satisfied:
    // - each image of fullImages must not be an incremental image;
    // - each image of fullImages must be taken after image has been taken;
    // - sum table set of fullImages must cover the table set of image.
    for (BackupImage image1 : fullImages) {
      if (image1.getType().equals(BackupRestoreConstants.BACKUP_TYPE_INCR)) {
        return false;
      }
      if (image1.getStartTs() < image.getStartTs()) {
        return false;
      }
    }

    ArrayList<String> image1TableSet = new ArrayList<String>();
    for (BackupImage image1 : fullImages) {
      String[] tableSet = image1.getTableSet().split(";");
      for (String table : tableSet) {
        image1TableSet.add(table);
      }
    }
    ArrayList<String> image2TableSet = new ArrayList<String>();
    String[] tableSet = image.getTableSet().split(";");
    for (String table : tableSet) {
      image2TableSet.add(table);
    }
    
    for (int i = 0; i < image2TableSet.size(); i++) {
      if (image1TableSet.contains(image2TableSet.get(i)) == false) {
        return false;
      }
    }

    LOG.debug("Full image set can cover image " + image.getBackupId());
    return true;
  }

}
