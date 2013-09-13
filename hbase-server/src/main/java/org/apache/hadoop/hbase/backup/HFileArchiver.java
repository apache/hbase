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
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.hadoop.io.MultipleIOException;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;

/**
 * Utility class to handle the removal of HFiles (or the respective {@link StoreFile StoreFiles})
 * for a HRegion from the {@link FileSystem}. The hfiles will be archived or deleted, depending on
 * the state of the system.
 */
public class HFileArchiver {
  private static final Log LOG = LogFactory.getLog(HFileArchiver.class);
  private static final String SEPARATOR = ".";

  /** Number of retries in case of fs operation failure */
  private static final int DEFAULT_RETRIES_NUMBER = 3;

  private HFileArchiver() {
    // hidden ctor since this is just a util
  }

  /**
   * Cleans up all the files for a HRegion by archiving the HFiles to the
   * archive directory
   * @param conf the configuration to use
   * @param fs the file system object
   * @param info HRegionInfo for region to be deleted
   * @throws IOException
   */
  public static void archiveRegion(Configuration conf, FileSystem fs, HRegionInfo info)
      throws IOException {
    Path rootDir = FSUtils.getRootDir(conf);
    archiveRegion(fs, rootDir, FSUtils.getTableDir(rootDir, info.getTable()),
      HRegion.getRegionDir(rootDir, info));
  }

  /**
   * Remove an entire region from the table directory via archiving the region's hfiles.
   * @param fs {@link FileSystem} from which to remove the region
   * @param rootdir {@link Path} to the root directory where hbase files are stored (for building
   *          the archive path)
   * @param tableDir {@link Path} to where the table is being stored (for building the archive path)
   * @param regionDir {@link Path} to where a region is being stored (for building the archive path)
   * @return <tt>true</tt> if the region was sucessfully deleted. <tt>false</tt> if the filesystem
   *         operations could not complete.
   * @throws IOException if the request cannot be completed
   */
  public static boolean archiveRegion(FileSystem fs, Path rootdir, Path tableDir, Path regionDir)
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("ARCHIVING " + regionDir.toString());
    }

    // otherwise, we archive the files
    // make sure we can archive
    if (tableDir == null || regionDir == null) {
      LOG.error("No archive directory could be found because tabledir (" + tableDir
          + ") or regiondir (" + regionDir + "was null. Deleting files instead.");
      deleteRegionWithoutArchiving(fs, regionDir);
      // we should have archived, but failed to. Doesn't matter if we deleted
      // the archived files correctly or not.
      return false;
    }

    // make sure the regiondir lives under the tabledir
    Preconditions.checkArgument(regionDir.toString().startsWith(tableDir.toString()));
    Path regionArchiveDir = HFileArchiveUtil.getRegionArchiveDir(rootdir,
        FSUtils.getTableName(tableDir),
        regionDir.getName());

    FileStatusConverter getAsFile = new FileStatusConverter(fs);
    // otherwise, we attempt to archive the store files

    // build collection of just the store directories to archive
    Collection<File> toArchive = new ArrayList<File>();
    final PathFilter dirFilter = new FSUtils.DirFilter(fs);
    PathFilter nonHidden = new PathFilter() {
      @Override
      public boolean accept(Path file) {
        return dirFilter.accept(file) && !file.getName().toString().startsWith(".");
      }
    };
    FileStatus[] storeDirs = FSUtils.listStatus(fs, regionDir, nonHidden);
    // if there no files, we can just delete the directory and return;
    if (storeDirs == null) {
      LOG.debug("Region directory (" + regionDir + ") was empty, just deleting and returning!");
      return deleteRegionWithoutArchiving(fs, regionDir);
    }

    // convert the files in the region to a File
    toArchive.addAll(Lists.transform(Arrays.asList(storeDirs), getAsFile));
    LOG.debug("Archiving " + toArchive);
    boolean success = false;
    try {
      success = resolveAndArchive(fs, regionArchiveDir, toArchive);
    } catch (IOException e) {
      LOG.error("Failed to archive " + toArchive, e);
      success = false;
    }

    // if that was successful, then we delete the region
    if (success) {
      return deleteRegionWithoutArchiving(fs, regionDir);
    }

    throw new IOException("Received error when attempting to archive files (" + toArchive
        + "), cannot delete region directory. ");
  }

  /**
   * Remove from the specified region the store files of the specified column family,
   * either by archiving them or outright deletion
   * @param fs the filesystem where the store files live
   * @param conf {@link Configuration} to examine to determine the archive directory
   * @param parent Parent region hosting the store files
   * @param tableDir {@link Path} to where the table is being stored (for building the archive path)
   * @param family the family hosting the store files
   * @throws IOException if the files could not be correctly disposed.
   */
  public static void archiveFamily(FileSystem fs, Configuration conf,
      HRegionInfo parent, Path tableDir, byte[] family) throws IOException {
    Path familyDir = new Path(tableDir, new Path(parent.getEncodedName(), Bytes.toString(family)));
    FileStatus[] storeFiles = FSUtils.listStatus(fs, familyDir);
    if (storeFiles == null) {
      LOG.debug("No store files to dispose for region=" + parent.getRegionNameAsString() +
          ", family=" + Bytes.toString(family));
      return;
    }

    FileStatusConverter getAsFile = new FileStatusConverter(fs);
    Collection<File> toArchive = Lists.transform(Arrays.asList(storeFiles), getAsFile);
    Path storeArchiveDir = HFileArchiveUtil.getStoreArchivePath(conf, parent, tableDir, family);

    // do the actual archive
    if (!resolveAndArchive(fs, storeArchiveDir, toArchive)) {
      throw new IOException("Failed to archive/delete all the files for region:"
          + Bytes.toString(parent.getRegionName()) + ", family:" + Bytes.toString(family)
          + " into " + storeArchiveDir + ". Something is probably awry on the filesystem.");
    }
  }

  /**
   * Remove the store files, either by archiving them or outright deletion
   * @param conf {@link Configuration} to examine to determine the archive directory
   * @param fs the filesystem where the store files live
   * @param regionInfo {@link HRegionInfo} of the region hosting the store files
   * @param family the family hosting the store files
   * @param compactedFiles files to be disposed of. No further reading of these files should be
   *          attempted; otherwise likely to cause an {@link IOException}
   * @throws IOException if the files could not be correctly disposed.
   */
  public static void archiveStoreFiles(Configuration conf, FileSystem fs, HRegionInfo regionInfo,
      Path tableDir, byte[] family, Collection<StoreFile> compactedFiles) throws IOException {

    // sometimes in testing, we don't have rss, so we need to check for that
    if (fs == null) {
      LOG.warn("Passed filesystem is null, so just deleting the files without archiving for region:"
          + Bytes.toString(regionInfo.getRegionName()) + ", family:" + Bytes.toString(family));
      deleteStoreFilesWithoutArchiving(compactedFiles);
      return;
    }

    // short circuit if we don't have any files to delete
    if (compactedFiles.size() == 0) {
      LOG.debug("No store files to dispose, done!");
      return;
    }

    // build the archive path
    if (regionInfo == null || family == null) throw new IOException(
        "Need to have a region and a family to archive from.");

    Path storeArchiveDir = HFileArchiveUtil.getStoreArchivePath(conf, regionInfo, tableDir, family);

    // make sure we don't archive if we can't and that the archive dir exists
    if (!fs.mkdirs(storeArchiveDir)) {
      throw new IOException("Could not make archive directory (" + storeArchiveDir + ") for store:"
          + Bytes.toString(family) + ", deleting compacted files instead.");
    }

    // otherwise we attempt to archive the store files
    if (LOG.isTraceEnabled()) LOG.trace("Archiving compacted store files.");

    // wrap the storefile into a File
    StoreToFile getStorePath = new StoreToFile(fs);
    Collection<File> storeFiles = Collections2.transform(compactedFiles, getStorePath);

    // do the actual archive
    if (!resolveAndArchive(fs, storeArchiveDir, storeFiles)) {
      throw new IOException("Failed to archive/delete all the files for region:"
          + Bytes.toString(regionInfo.getRegionName()) + ", family:" + Bytes.toString(family)
          + " into " + storeArchiveDir + ". Something is probably awry on the filesystem.");
    }
  }

  /**
   * Archive the store file
   * @param fs the filesystem where the store files live
   * @param regionInfo region hosting the store files
   * @param conf {@link Configuration} to examine to determine the archive directory
   * @param tableDir {@link Path} to where the table is being stored (for building the archive path)
   * @param family the family hosting the store files
   * @param storeFile file to be archived
   * @throws IOException if the files could not be correctly disposed.
   */
  public static void archiveStoreFile(Configuration conf, FileSystem fs, HRegionInfo regionInfo,
      Path tableDir, byte[] family, Path storeFile) throws IOException {
    Path storeArchiveDir = HFileArchiveUtil.getStoreArchivePath(conf, regionInfo, tableDir, family);
    // make sure we don't archive if we can't and that the archive dir exists
    if (!fs.mkdirs(storeArchiveDir)) {
      throw new IOException("Could not make archive directory (" + storeArchiveDir + ") for store:"
          + Bytes.toString(family) + ", deleting compacted files instead.");
    }

    // do the actual archive
    long start = EnvironmentEdgeManager.currentTimeMillis();
    File file = new FileablePath(fs, storeFile);
    if (!resolveAndArchiveFile(storeArchiveDir, file, Long.toString(start))) {
      throw new IOException("Failed to archive/delete the file for region:"
          + regionInfo.getRegionNameAsString() + ", family:" + Bytes.toString(family)
          + " into " + storeArchiveDir + ". Something is probably awry on the filesystem.");
    }
  }

  /**
   * Archive the given files and resolve any conflicts with existing files via appending the time
   * archiving started (so all conflicts in the same group have the same timestamp appended).
   * <p>
   * If any of the passed files to archive are directories, archives all the files under that
   * directory. Archive directory structure for children is the base archive directory name + the
   * parent directory and is built recursively is passed files are directories themselves.
   * @param fs {@link FileSystem} on which to archive the files
   * @param baseArchiveDir base archive directory to archive the given files
   * @param toArchive files to be archived
   * @return <tt>true</tt> on success, <tt>false</tt> otherwise
   * @throws IOException on unexpected failure
   */
  private static boolean resolveAndArchive(FileSystem fs, Path baseArchiveDir,
      Collection<File> toArchive) throws IOException {
    if (LOG.isTraceEnabled()) LOG.trace("Starting to archive " + toArchive);
    long start = EnvironmentEdgeManager.currentTimeMillis();
    List<File> failures = resolveAndArchive(fs, baseArchiveDir, toArchive, start);

    // notify that some files were not archived.
    // We can't delete the files otherwise snapshots or other backup system
    // that relies on the archiver end up with data loss.
    if (failures.size() > 0) {
      LOG.warn("Failed to complete archive of: " + failures +
        ". Those files are still in the original location, and they may slow down reads.");
      return false;
    }
    return true;
  }

  /**
   * Resolve any conflict with an existing archive file via timestamp-append
   * renaming of the existing file and then archive the passed in files.
   * @param fs {@link FileSystem} on which to archive the files
   * @param baseArchiveDir base archive directory to store the files. If any of
   *          the files to archive are directories, will append the name of the
   *          directory to the base archive directory name, creating a parallel
   *          structure.
   * @param toArchive files/directories that need to be archvied
   * @param start time the archiving started - used for resolving archive
   *          conflicts.
   * @return the list of failed to archive files.
   * @throws IOException if an unexpected file operation exception occured
   */
  private static List<File> resolveAndArchive(FileSystem fs, Path baseArchiveDir,
      Collection<File> toArchive, long start) throws IOException {
    // short circuit if no files to move
    if (toArchive.size() == 0) return Collections.emptyList();

    if (LOG.isTraceEnabled()) LOG.trace("moving files to the archive directory: " + baseArchiveDir);

    // make sure the archive directory exists
    if (!fs.exists(baseArchiveDir)) {
      if (!fs.mkdirs(baseArchiveDir)) {
        throw new IOException("Failed to create the archive directory:" + baseArchiveDir
            + ", quitting archive attempt.");
      }
      if (LOG.isTraceEnabled()) LOG.trace("Created archive directory:" + baseArchiveDir);
    }

    List<File> failures = new ArrayList<File>();
    String startTime = Long.toString(start);
    for (File file : toArchive) {
      // if its a file archive it
      try {
        if (LOG.isTraceEnabled()) LOG.trace("Archiving: " + file);
        if (file.isFile()) {
          // attempt to archive the file
          if (!resolveAndArchiveFile(baseArchiveDir, file, startTime)) {
            LOG.warn("Couldn't archive " + file + " into backup directory: " + baseArchiveDir);
            failures.add(file);
          }
        } else {
          // otherwise its a directory and we need to archive all files
          if (LOG.isTraceEnabled()) LOG.trace(file + " is a directory, archiving children files");
          // so we add the directory name to the one base archive
          Path parentArchiveDir = new Path(baseArchiveDir, file.getName());
          // and then get all the files from that directory and attempt to
          // archive those too
          Collection<File> children = file.getChildren();
          failures.addAll(resolveAndArchive(fs, parentArchiveDir, children, start));
        }
      } catch (IOException e) {
        LOG.warn("Failed to archive " + file, e);
        failures.add(file);
      }
    }
    return failures;
  }

  /**
   * Attempt to archive the passed in file to the archive directory.
   * <p>
   * If the same file already exists in the archive, it is moved to a timestamped directory under
   * the archive directory and the new file is put in its place.
   * @param archiveDir {@link Path} to the directory that stores the archives of the hfiles
   * @param currentFile {@link Path} to the original HFile that will be archived
   * @param archiveStartTime time the archiving started, to resolve naming conflicts
   * @return <tt>true</tt> if the file is successfully archived. <tt>false</tt> if there was a
   *         problem, but the operation still completed.
   * @throws IOException on failure to complete {@link FileSystem} operations.
   */
  private static boolean resolveAndArchiveFile(Path archiveDir, File currentFile,
      String archiveStartTime) throws IOException {
    // build path as it should be in the archive
    String filename = currentFile.getName();
    Path archiveFile = new Path(archiveDir, filename);
    FileSystem fs = currentFile.getFileSystem();

    // if the file already exists in the archive, move that one to a timestamped backup. This is a
    // really, really unlikely situtation, where we get the same name for the existing file, but
    // is included just for that 1 in trillion chance.
    if (fs.exists(archiveFile)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("File:" + archiveFile + " already exists in archive, moving to "
            + "timestamped backup and overwriting current.");
      }

      // move the archive file to the stamped backup
      Path backedupArchiveFile = new Path(archiveDir, filename + SEPARATOR + archiveStartTime);
      if (!fs.rename(archiveFile, backedupArchiveFile)) {
        LOG.error("Could not rename archive file to backup: " + backedupArchiveFile
            + ", deleting existing file in favor of newer.");
        // try to delete the exisiting file, if we can't rename it
        if (!fs.delete(archiveFile, false)) {
          throw new IOException("Couldn't delete existing archive file (" + archiveFile
              + ") or rename it to the backup file (" + backedupArchiveFile
              + ") to make room for similarly named file.");
        }
      }
      LOG.debug("Backed up archive file from " + archiveFile);
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace("No existing file in archive for: " + archiveFile +
        ", free to archive original file.");
    }

    // at this point, we should have a free spot for the archive file
    boolean success = false;
    for (int i = 0; !success && i < DEFAULT_RETRIES_NUMBER; ++i) {
      if (i > 0) {
        // Ensure that the archive directory exists.
        // The previous "move to archive" operation has failed probably because
        // the cleaner has removed our archive directory (HBASE-7643).
        // (we're in a retry loop, so don't worry too much about the exception)
        try {
          if (!fs.exists(archiveDir)) {
            if (fs.mkdirs(archiveDir)) {
              LOG.debug("Created archive directory:" + archiveDir);
            }
          }
        } catch (IOException e) {
          LOG.warn("Failed to create directory: " + archiveDir, e);
        }
      }

      try {
        success = currentFile.moveAndClose(archiveFile);
      } catch (IOException e) {
        LOG.warn("Failed to archive " + currentFile + " on try #" + i, e);
        success = false;
      }
    }

    if (!success) {
      LOG.error("Failed to archive " + currentFile);
      return false;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Finished archiving from " + currentFile + ", to " + archiveFile);
    }
    return true;
  }

  /**
   * Without regard for backup, delete a region. Should be used with caution.
   * @param regionDir {@link Path} to the region to be deleted.
   * @param fs FileSystem from which to delete the region
   * @return <tt>true</tt> on successful deletion, <tt>false</tt> otherwise
   * @throws IOException on filesystem operation failure
   */
  private static boolean deleteRegionWithoutArchiving(FileSystem fs, Path regionDir)
      throws IOException {
    if (fs.delete(regionDir, true)) {
      LOG.debug("Deleted all region files in: " + regionDir);
      return true;
    }
    LOG.debug("Failed to delete region directory:" + regionDir);
    return false;
  }

  /**
   * Just do a simple delete of the given store files
   * <p>
   * A best effort is made to delete each of the files, rather than bailing on the first failure.
   * <p>
   * This method is preferable to {@link #deleteFilesWithoutArchiving(Collection)} since it consumes
   * less resources, but is limited in terms of usefulness
   * @param compactedFiles store files to delete from the file system.
   * @throws IOException if a file cannot be deleted. All files will be attempted to deleted before
   *           throwing the exception, rather than failing at the first file.
   */
  private static void deleteStoreFilesWithoutArchiving(Collection<StoreFile> compactedFiles)
      throws IOException {
    LOG.debug("Deleting store files without archiving.");
    List<IOException> errors = new ArrayList<IOException>(0);
    for (StoreFile hsf : compactedFiles) {
      try {
        hsf.deleteReader();
      } catch (IOException e) {
        LOG.error("Failed to delete store file:" + hsf.getPath());
        errors.add(e);
      }
    }
    if (errors.size() > 0) {
      throw MultipleIOException.createIOException(errors);
    }
  }

  /**
   * Adapt a type to match the {@link File} interface, which is used internally for handling
   * archival/removal of files
   * @param <T> type to adapt to the {@link File} interface
   */
  private static abstract class FileConverter<T> implements Function<T, File> {
    protected final FileSystem fs;

    public FileConverter(FileSystem fs) {
      this.fs = fs;
    }
  }

  /**
   * Convert a FileStatus to something we can manage in the archiving
   */
  private static class FileStatusConverter extends FileConverter<FileStatus> {
    public FileStatusConverter(FileSystem fs) {
      super(fs);
    }

    @Override
    public File apply(FileStatus input) {
      return new FileablePath(fs, input.getPath());
    }
  }

  /**
   * Convert the {@link StoreFile} into something we can manage in the archive
   * methods
   */
  private static class StoreToFile extends FileConverter<StoreFile> {
    public StoreToFile(FileSystem fs) {
      super(fs);
    }

    @Override
    public File apply(StoreFile input) {
      return new FileableStoreFile(fs, input);
    }
  }

  /**
   * Wrapper to handle file operations uniformly
   */
  private static abstract class File {
    protected final FileSystem fs;

    public File(FileSystem fs) {
      this.fs = fs;
    }

    /**
     * Delete the file
     * @throws IOException on failure
     */
    abstract void delete() throws IOException;

    /**
     * Check to see if this is a file or a directory
     * @return <tt>true</tt> if it is a file, <tt>false</tt> otherwise
     * @throws IOException on {@link FileSystem} connection error
     */
    abstract boolean isFile() throws IOException;

    /**
     * @return if this is a directory, returns all the children in the
     *         directory, otherwise returns an empty list
     * @throws IOException
     */
    abstract Collection<File> getChildren() throws IOException;

    /**
     * close any outside readers of the file
     * @throws IOException
     */
    abstract void close() throws IOException;

    /**
     * @return the name of the file (not the full fs path, just the individual
     *         file name)
     */
    abstract String getName();

    /**
     * @return the path to this file
     */
    abstract Path getPath();

    /**
     * Move the file to the given destination
     * @param dest
     * @return <tt>true</tt> on success
     * @throws IOException
     */
    public boolean moveAndClose(Path dest) throws IOException {
      this.close();
      Path p = this.getPath();
      return FSUtils.renameAndSetModifyTime(fs, p, dest);
    }

    /**
     * @return the {@link FileSystem} on which this file resides
     */
    public FileSystem getFileSystem() {
      return this.fs;
    }

    @Override
    public String toString() {
      return this.getClass() + ", file:" + getPath().toString();
    }
  }

  /**
   * A {@link File} that wraps a simple {@link Path} on a {@link FileSystem}.
   */
  private static class FileablePath extends File {
    private final Path file;
    private final FileStatusConverter getAsFile;

    public FileablePath(FileSystem fs, Path file) {
      super(fs);
      this.file = file;
      this.getAsFile = new FileStatusConverter(fs);
    }

    @Override
    public void delete() throws IOException {
      if (!fs.delete(file, true)) throw new IOException("Failed to delete:" + this.file);
    }

    @Override
    public String getName() {
      return file.getName();
    }

    @Override
    public Collection<File> getChildren() throws IOException {
      if (fs.isFile(file)) return Collections.emptyList();
      return Collections2.transform(Arrays.asList(fs.listStatus(file)), getAsFile);
    }

    @Override
    public boolean isFile() throws IOException {
      return fs.isFile(file);
    }

    @Override
    public void close() throws IOException {
      // NOOP - files are implicitly closed on removal
    }

    @Override
    Path getPath() {
      return file;
    }
  }

  /**
   * {@link File} adapter for a {@link StoreFile} living on a {@link FileSystem}
   * .
   */
  private static class FileableStoreFile extends File {
    StoreFile file;

    public FileableStoreFile(FileSystem fs, StoreFile store) {
      super(fs);
      this.file = store;
    }

    @Override
    public void delete() throws IOException {
      file.deleteReader();
    }

    @Override
    public String getName() {
      return file.getPath().getName();
    }

    @Override
    public boolean isFile() {
      return true;
    }

    @Override
    public Collection<File> getChildren() throws IOException {
      // storefiles don't have children
      return Collections.emptyList();
    }

    @Override
    public void close() throws IOException {
      file.closeReader(true);
    }

    @Override
    Path getPath() {
      return file.getPath();
    }
  }
}
