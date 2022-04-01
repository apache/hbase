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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * Utility class to handle the removal of HFiles (or the respective {@link HStoreFile StoreFiles})
 * for a HRegion from the {@link FileSystem}. The hfiles will be archived or deleted, depending on
 * the state of the system.
 */
@InterfaceAudience.Private
public class HFileArchiver {
  private static final Logger LOG = LoggerFactory.getLogger(HFileArchiver.class);
  private static final String SEPARATOR = ".";

  /** Number of retries in case of fs operation failure */
  private static final int DEFAULT_RETRIES_NUMBER = 3;

  private static final Function<File, Path> FUNC_FILE_TO_PATH =
      new Function<File, Path>() {
        @Override
        public Path apply(File file) {
          return file == null ? null : file.getPath();
        }
      };

  private static ThreadPoolExecutor archiveExecutor;

  private HFileArchiver() {
    // hidden ctor since this is just a util
  }

  /**
   * @return True if the Region exits in the filesystem.
   */
  public static boolean exists(Configuration conf, FileSystem fs, RegionInfo info)
      throws IOException {
    Path rootDir = CommonFSUtils.getRootDir(conf);
    Path regionDir = FSUtils.getRegionDirFromRootDir(rootDir, info);
    return fs.exists(regionDir);
  }

  /**
   * Cleans up all the files for a HRegion by archiving the HFiles to the archive directory
   * @param conf the configuration to use
   * @param fs the file system object
   * @param info RegionInfo for region to be deleted
   */
  public static void archiveRegion(Configuration conf, FileSystem fs, RegionInfo info)
      throws IOException {
    Path rootDir = CommonFSUtils.getRootDir(conf);
    archiveRegion(fs, rootDir, CommonFSUtils.getTableDir(rootDir, info.getTable()),
      FSUtils.getRegionDirFromRootDir(rootDir, info));
  }

  /**
   * Remove an entire region from the table directory via archiving the region's hfiles.
   * @param fs {@link FileSystem} from which to remove the region
   * @param rootdir {@link Path} to the root directory where hbase files are stored (for building
   *          the archive path)
   * @param tableDir {@link Path} to where the table is being stored (for building the archive path)
   * @param regionDir {@link Path} to where a region is being stored (for building the archive path)
   * @return <tt>true</tt> if the region was successfully deleted. <tt>false</tt> if the filesystem
   *         operations could not complete.
   * @throws IOException if the request cannot be completed
   */
  public static boolean archiveRegion(FileSystem fs, Path rootdir, Path tableDir, Path regionDir)
      throws IOException {
    // otherwise, we archive the files
    // make sure we can archive
    if (tableDir == null || regionDir == null) {
      LOG.error("No archive directory could be found because tabledir (" + tableDir
          + ") or regiondir (" + regionDir + "was null. Deleting files instead.");
      if (regionDir != null) {
        deleteRegionWithoutArchiving(fs, regionDir);
      }
      // we should have archived, but failed to. Doesn't matter if we deleted
      // the archived files correctly or not.
      return false;
    }

    LOG.debug("ARCHIVING {}", regionDir);

    // make sure the regiondir lives under the tabledir
    Preconditions.checkArgument(regionDir.toString().startsWith(tableDir.toString()));
    Path regionArchiveDir = HFileArchiveUtil.getRegionArchiveDir(rootdir,
      CommonFSUtils.getTableName(tableDir), regionDir.getName());

    FileStatusConverter getAsFile = new FileStatusConverter(fs);
    // otherwise, we attempt to archive the store files

    // build collection of just the store directories to archive
    Collection<File> toArchive = new ArrayList<>();
    final PathFilter dirFilter = new FSUtils.DirFilter(fs);
    PathFilter nonHidden = new PathFilter() {
      @Override
      public boolean accept(Path file) {
        return dirFilter.accept(file) && !file.getName().startsWith(".");
      }
    };
    FileStatus[] storeDirs = CommonFSUtils.listStatus(fs, regionDir, nonHidden);
    // if there no files, we can just delete the directory and return;
    if (storeDirs == null) {
      LOG.debug("Directory {} empty.", regionDir);
      return deleteRegionWithoutArchiving(fs, regionDir);
    }

    // convert the files in the region to a File
    Stream.of(storeDirs).map(getAsFile).forEachOrdered(toArchive::add);
    LOG.debug("Archiving " + toArchive);
    List<File> failedArchive = resolveAndArchive(fs, regionArchiveDir, toArchive,
        EnvironmentEdgeManager.currentTime());
    if (!failedArchive.isEmpty()) {
      throw new FailedArchiveException(
        "Failed to archive/delete all the files for region:" + regionDir.getName() + " into " +
          regionArchiveDir + ". Something is probably awry on the filesystem.",
        failedArchive.stream().map(FUNC_FILE_TO_PATH).collect(Collectors.toList()));
    }
    // if that was successful, then we delete the region
    return deleteRegionWithoutArchiving(fs, regionDir);
  }

  /**
   * Archive the specified regions in parallel.
   * @param conf the configuration to use
   * @param fs {@link FileSystem} from which to remove the region
   * @param rootDir {@link Path} to the root directory where hbase files are stored (for building
   *                            the archive path)
   * @param tableDir {@link Path} to where the table is being stored (for building the archive
   *                             path)
   * @param regionDirList {@link Path} to where regions are being stored (for building the archive
   *                                  path)
   * @throws IOException if the request cannot be completed
   */
  public static void archiveRegions(Configuration conf, FileSystem fs, Path rootDir, Path tableDir,
    List<Path> regionDirList) throws IOException {
    List<Future<Void>> futures = new ArrayList<>(regionDirList.size());
    for (Path regionDir: regionDirList) {
      Future<Void> future = getArchiveExecutor(conf).submit(() -> {
        archiveRegion(fs, rootDir, tableDir, regionDir);
        return null;
      });
      futures.add(future);
    }
    try {
      for (Future<Void> future: futures) {
        future.get();
      }
    } catch (InterruptedException e) {
      throw new InterruptedIOException(e.getMessage());
    } catch (ExecutionException e) {
      throw new IOException(e.getCause());
    }
  }

  private static synchronized ThreadPoolExecutor getArchiveExecutor(final Configuration conf) {
    if (archiveExecutor == null) {
      int maxThreads = conf.getInt("hbase.hfilearchiver.thread.pool.max", 8);
      archiveExecutor = Threads.getBoundedCachedThreadPool(maxThreads, 30L, TimeUnit.SECONDS,
        getThreadFactory());

      // Shutdown this ThreadPool in a shutdown hook
      Runtime.getRuntime().addShutdownHook(new Thread(() -> archiveExecutor.shutdown()));
    }
    return archiveExecutor;
  }

  // We need this method instead of Threads.getNamedThreadFactory() to pass some tests.
  // The difference from Threads.getNamedThreadFactory() is that it doesn't fix ThreadGroup for
  // new threads. If we use Threads.getNamedThreadFactory(), we will face ThreadGroup related
  // issues in some tests.
  private static ThreadFactory getThreadFactory() {
    return new ThreadFactory() {
      final AtomicInteger threadNumber = new AtomicInteger(1);

      @Override
      public Thread newThread(Runnable r) {
        final String name = "HFileArchiver-" + threadNumber.getAndIncrement();
        Thread t = new Thread(r, name);
        t.setDaemon(true);
        return t;
      }
    };
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
      RegionInfo parent, Path tableDir, byte[] family) throws IOException {
    Path familyDir = new Path(tableDir, new Path(parent.getEncodedName(), Bytes.toString(family)));
    archiveFamilyByFamilyDir(fs, conf, parent, familyDir, family);
  }

  /**
   * Removes from the specified region the store files of the specified column family,
   * either by archiving them or outright deletion
   * @param fs the filesystem where the store files live
   * @param conf {@link Configuration} to examine to determine the archive directory
   * @param parent Parent region hosting the store files
   * @param familyDir {@link Path} to where the family is being stored
   * @param family the family hosting the store files
   * @throws IOException if the files could not be correctly disposed.
   */
  public static void archiveFamilyByFamilyDir(FileSystem fs, Configuration conf,
      RegionInfo parent, Path familyDir, byte[] family) throws IOException {
    FileStatus[] storeFiles = CommonFSUtils.listStatus(fs, familyDir);
    if (storeFiles == null) {
      LOG.debug("No files to dispose of in {}, family={}", parent.getRegionNameAsString(),
          Bytes.toString(family));
      return;
    }

    FileStatusConverter getAsFile = new FileStatusConverter(fs);
    Collection<File> toArchive = Stream.of(storeFiles).map(getAsFile).collect(Collectors.toList());
    Path storeArchiveDir = HFileArchiveUtil.getStoreArchivePath(conf, parent, family);

    // do the actual archive
    List<File> failedArchive = resolveAndArchive(fs, storeArchiveDir, toArchive,
        EnvironmentEdgeManager.currentTime());
    if (!failedArchive.isEmpty()){
      throw new FailedArchiveException("Failed to archive/delete all the files for region:"
          + Bytes.toString(parent.getRegionName()) + ", family:" + Bytes.toString(family)
          + " into " + storeArchiveDir + ". Something is probably awry on the filesystem.",
          failedArchive.stream().map(FUNC_FILE_TO_PATH).collect(Collectors.toList()));
    }
  }

  /**
   * Remove the store files, either by archiving them or outright deletion
   * @param conf {@link Configuration} to examine to determine the archive directory
   * @param fs the filesystem where the store files live
   * @param regionInfo {@link RegionInfo} of the region hosting the store files
   * @param family the family hosting the store files
   * @param compactedFiles files to be disposed of. No further reading of these files should be
   *          attempted; otherwise likely to cause an {@link IOException}
   * @throws IOException if the files could not be correctly disposed.
   */
  public static void archiveStoreFiles(Configuration conf, FileSystem fs, RegionInfo regionInfo,
      Path tableDir, byte[] family, Collection<HStoreFile> compactedFiles)
      throws IOException {
    Path storeArchiveDir = HFileArchiveUtil.getStoreArchivePath(conf, regionInfo, tableDir, family);
    archive(fs, regionInfo, family, compactedFiles, storeArchiveDir);
  }

  /**
   * Archive recovered edits using existing logic for archiving store files. This is currently only
   * relevant when <b>hbase.region.archive.recovered.edits</b> is true, as recovered edits shouldn't
   * be kept after replay. In theory, we could use very same method available for archiving
   * store files, but supporting WAL dir and store files on different FileSystems added the need for
   * extra validation of the passed FileSystem instance and the path where the archiving edits
   * should be placed.
   * @param conf {@link Configuration} to determine the archive directory.
   * @param fs the filesystem used for storing WAL files.
   * @param regionInfo {@link RegionInfo} a pseudo region representation for the archiving logic.
   * @param family a pseudo familiy representation for the archiving logic.
   * @param replayedEdits the recovered edits to be archived.
   * @throws IOException if files can't be achived due to some internal error.
   */
  public static void archiveRecoveredEdits(Configuration conf, FileSystem fs, RegionInfo regionInfo,
    byte[] family, Collection<HStoreFile> replayedEdits)
    throws IOException {
    String workingDir = conf.get(CommonFSUtils.HBASE_WAL_DIR, conf.get(HConstants.HBASE_DIR));
    //extra sanity checks for the right FS
    Path path = new Path(workingDir);
    if(path.isAbsoluteAndSchemeAuthorityNull()){
      //no schema specified on wal dir value, so it's on same FS as StoreFiles
      path = new Path(conf.get(HConstants.HBASE_DIR));
    }
    if(path.toUri().getScheme()!=null && !path.toUri().getScheme().equals(fs.getScheme())){
      throw new IOException("Wrong file system! Should be " + path.toUri().getScheme() +
        ", but got " +  fs.getScheme());
    }
    path = HFileArchiveUtil.getStoreArchivePathForRootDir(path, regionInfo, family);
    archive(fs, regionInfo, family, replayedEdits, path);
  }

  private static void archive(FileSystem fs, RegionInfo regionInfo, byte[] family,
    Collection<HStoreFile> compactedFiles, Path storeArchiveDir) throws IOException {
    // sometimes in testing, we don't have rss, so we need to check for that
    if (fs == null) {
      LOG.warn("Passed filesystem is null, so just deleting files without archiving for {}," +
              "family={}", Bytes.toString(regionInfo.getRegionName()), Bytes.toString(family));
      deleteStoreFilesWithoutArchiving(compactedFiles);
      return;
    }

    // short circuit if we don't have any files to delete
    if (compactedFiles.isEmpty()) {
      LOG.debug("No files to dispose of, done!");
      return;
    }

    // build the archive path
    if (regionInfo == null || family == null) throw new IOException(
        "Need to have a region and a family to archive from.");
    // make sure we don't archive if we can't and that the archive dir exists
    if (!fs.mkdirs(storeArchiveDir)) {
      throw new IOException("Could not make archive directory (" + storeArchiveDir + ") for store:"
          + Bytes.toString(family) + ", deleting compacted files instead.");
    }

    // otherwise we attempt to archive the store files
    LOG.debug("Archiving compacted files.");

    // Wrap the storefile into a File
    StoreToFile getStorePath = new StoreToFile(fs);
    Collection<File> storeFiles =
      compactedFiles.stream().map(getStorePath).collect(Collectors.toList());

    // do the actual archive
    List<File> failedArchive =
      resolveAndArchive(fs, storeArchiveDir, storeFiles, EnvironmentEdgeManager.currentTime());

    if (!failedArchive.isEmpty()){
      throw new FailedArchiveException("Failed to archive/delete all the files for region:"
          + Bytes.toString(regionInfo.getRegionName()) + ", family:" + Bytes.toString(family)
          + " into " + storeArchiveDir + ". Something is probably awry on the filesystem.",
          failedArchive.stream().map(FUNC_FILE_TO_PATH).collect(Collectors.toList()));
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
  public static void archiveStoreFile(Configuration conf, FileSystem fs, RegionInfo regionInfo,
      Path tableDir, byte[] family, Path storeFile) throws IOException {
    Path storeArchiveDir = HFileArchiveUtil.getStoreArchivePath(conf, regionInfo, tableDir, family);
    // make sure we don't archive if we can't and that the archive dir exists
    if (!fs.mkdirs(storeArchiveDir)) {
      throw new IOException("Could not make archive directory (" + storeArchiveDir + ") for store:"
          + Bytes.toString(family) + ", deleting compacted files instead.");
    }

    // do the actual archive
    long start = EnvironmentEdgeManager.currentTime();
    File file = new FileablePath(fs, storeFile);
    if (!resolveAndArchiveFile(storeArchiveDir, file, Long.toString(start))) {
      throw new IOException("Failed to archive/delete the file for region:"
          + regionInfo.getRegionNameAsString() + ", family:" + Bytes.toString(family)
          + " into " + storeArchiveDir + ". Something is probably awry on the filesystem.");
    }
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
   * @throws IOException if an unexpected file operation exception occurred
   */
  private static List<File> resolveAndArchive(FileSystem fs, Path baseArchiveDir,
      Collection<File> toArchive, long start) throws IOException {
    // short circuit if no files to move
    if (toArchive.isEmpty()) {
      return Collections.emptyList();
    }

    LOG.trace("Moving files to the archive directory {}", baseArchiveDir);

    // make sure the archive directory exists
    if (!fs.exists(baseArchiveDir)) {
      if (!fs.mkdirs(baseArchiveDir)) {
        throw new IOException("Failed to create the archive directory:" + baseArchiveDir
            + ", quitting archive attempt.");
      }
      LOG.trace("Created archive directory {}", baseArchiveDir);
    }

    List<File> failures = new ArrayList<>();
    String startTime = Long.toString(start);
    for (File file : toArchive) {
      // if its a file archive it
      try {
        LOG.trace("Archiving {}", file);
        if (file.isFile()) {
          // attempt to archive the file
          if (!resolveAndArchiveFile(baseArchiveDir, file, startTime)) {
            LOG.warn("Couldn't archive " + file + " into backup directory: " + baseArchiveDir);
            failures.add(file);
          }
        } else {
          // otherwise its a directory and we need to archive all files
          LOG.trace("{} is a directory, archiving children files", file);
          // so we add the directory name to the one base archive
          Path parentArchiveDir = new Path(baseArchiveDir, file.getName());
          // and then get all the files from that directory and attempt to
          // archive those too
          Collection<File> children = file.getChildren();
          failures.addAll(resolveAndArchive(fs, parentArchiveDir, children, start));
        }
      } catch (IOException e) {
        LOG.warn("Failed to archive {}", file, e);
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

    // An existing destination file in the archive is unexpected, but we handle it here.
    if (fs.exists(archiveFile)) {
      if (!fs.exists(currentFile.getPath())) {
        // If the file already exists in the archive, and there is no current file to archive, then
        // assume that the file in archive is correct. This is an unexpected situation, suggesting a
        // race condition or split brain.
        // In HBASE-26718 this was found when compaction incorrectly happened during warmupRegion.
        LOG.warn("{} exists in archive. Attempted to archive nonexistent file {}.", archiveFile,
          currentFile);
        // We return success to match existing behavior in this method, where FileNotFoundException
        // in moveAndClose is ignored.
        return true;
      }
      // There is a conflict between the current file and the already existing archived file.
      // Move the archived file to a timestamped backup. This is a really, really unlikely
      // situation, where we get the same name for the existing file, but is included just for that
      // 1 in trillion chance. We are potentially incurring data loss in the archive directory if
      // the files are not identical. The timestamped backup will be cleaned by HFileCleaner as it
      // has no references.
      FileStatus curStatus = fs.getFileStatus(currentFile.getPath());
      FileStatus archiveStatus = fs.getFileStatus(archiveFile);
      long curLen = curStatus.getLen();
      long archiveLen = archiveStatus.getLen();
      long curMtime = curStatus.getModificationTime();
      long archiveMtime = archiveStatus.getModificationTime();
      if (curLen != archiveLen) {
        LOG.error("{} already exists in archive with different size than current {}."
            + " archiveLen: {} currentLen: {} archiveMtime: {} currentMtime: {}",
          archiveFile, currentFile, archiveLen, curLen, archiveMtime, curMtime);
        throw new IOException(archiveFile + " already exists in archive with different size" +
          " than " + currentFile);
      }

      LOG.error("{} already exists in archive, moving to timestamped backup and overwriting"
          + " current {}. archiveLen: {} currentLen: {} archiveMtime: {} currentMtime: {}",
        archiveFile, currentFile, archiveLen, curLen, archiveMtime, curMtime);

      // move the archive file to the stamped backup
      Path backedupArchiveFile = new Path(archiveDir, filename + SEPARATOR + archiveStartTime);
      if (!fs.rename(archiveFile, backedupArchiveFile)) {
        LOG.error("Could not rename archive file to backup: " + backedupArchiveFile
            + ", deleting existing file in favor of newer.");
        // try to delete the existing file, if we can't rename it
        if (!fs.delete(archiveFile, false)) {
          throw new IOException("Couldn't delete existing archive file (" + archiveFile
              + ") or rename it to the backup file (" + backedupArchiveFile
              + ") to make room for similarly named file.");
        }
      } else {
        LOG.info("Backed up archive file from {} to {}.", archiveFile, backedupArchiveFile);
      }
    }

    LOG.trace("No existing file in archive for {}, free to archive original file.", archiveFile);

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
              LOG.debug("Created archive directory {}", archiveDir);
            }
          }
        } catch (IOException e) {
          LOG.warn("Failed to create directory {}", archiveDir, e);
        }
      }

      try {
        success = currentFile.moveAndClose(archiveFile);
      } catch (FileNotFoundException fnfe) {
        LOG.warn("Failed to archive " + currentFile +
            " because it does not exist! Skipping and continuing on.", fnfe);
        success = true;
      } catch (IOException e) {
        LOG.warn("Failed to archive " + currentFile + " on try #" + i, e);
        success = false;
      }
    }

    if (!success) {
      LOG.error("Failed to archive " + currentFile);
      return false;
    }

    LOG.debug("Archived from {} to {}", currentFile, archiveFile);
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
      LOG.debug("Deleted {}", regionDir);
      return true;
    }
    LOG.debug("Failed to delete directory {}", regionDir);
    return false;
  }

  /**
   * Just do a simple delete of the given store files
   * <p>
   * A best effort is made to delete each of the files, rather than bailing on the first failure.
   * <p>
   * @param compactedFiles store files to delete from the file system.
   * @throws IOException if a file cannot be deleted. All files will be attempted to deleted before
   *           throwing the exception, rather than failing at the first file.
   */
  private static void deleteStoreFilesWithoutArchiving(Collection<HStoreFile> compactedFiles)
      throws IOException {
    LOG.debug("Deleting files without archiving.");
    List<IOException> errors = new ArrayList<>(0);
    for (HStoreFile hsf : compactedFiles) {
      try {
        hsf.deleteStoreFile();
      } catch (IOException e) {
        LOG.error("Failed to delete {}", hsf.getPath());
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
   * Convert the {@link HStoreFile} into something we can manage in the archive
   * methods
   */
  private static class StoreToFile extends FileConverter<HStoreFile> {
    public StoreToFile(FileSystem fs) {
      super(fs);
    }

    @Override
    public File apply(HStoreFile input) {
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
      return CommonFSUtils.renameAndSetModifyTime(fs, p, dest);
    }

    /**
     * @return the {@link FileSystem} on which this file resides
     */
    public FileSystem getFileSystem() {
      return this.fs;
    }

    @Override
    public String toString() {
      return this.getClass().getSimpleName() + ", " + getPath().toString();
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
      if (fs.isFile(file)) {
        return Collections.emptyList();
      }
      return Stream.of(fs.listStatus(file)).map(getAsFile).collect(Collectors.toList());
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
   * {@link File} adapter for a {@link HStoreFile} living on a {@link FileSystem}
   * .
   */
  private static class FileableStoreFile extends File {
    HStoreFile file;

    public FileableStoreFile(FileSystem fs, HStoreFile store) {
      super(fs);
      this.file = store;
    }

    @Override
    public void delete() throws IOException {
      file.deleteStoreFile();
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
      file.closeStoreFile(true);
    }

    @Override
    Path getPath() {
      return file.getPath();
    }
  }
}
