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
package org.apache.hadoop.hbase.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
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
import org.apache.hadoop.hbase.backup.FailedArchiveException;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotArchiver;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * MapReduce-local archiver used by {@link
 * org.apache.hadoop.hbase.mapreduce.MapreduceRestoreSnapshotHelper}. It mirrors the server-side
 * {@code HFileArchiver} but lives in the MapReduce module so the snapshot-scanning path can be
 * evolved independently. It is injected into the shared {@link
 * org.apache.hadoop.hbase.snapshot.RestoreSnapshotHelper} restore/clone logic via {@link
 * RestoreSnapshotArchiver}.
 */
@InterfaceAudience.Private
public class MapreduceHFileArchiver implements RestoreSnapshotArchiver {

  private static final Logger LOG = LoggerFactory.getLogger(MapreduceHFileArchiver.class);
  private static final String SEPARATOR = ".";

  /** Number of retries in case of fs operation failure */
  private static final int DEFAULT_RETRIES_NUMBER = 3;

  private static final Function<File, Path> FUNC_FILE_TO_PATH = new Function<File, Path>() {
    @Override
    public Path apply(File file) {
      return file == null ? null : file.getPath();
    }
  };

  /** Returns True if the Region exits in the filesystem. */
  public static boolean exists(Configuration conf, FileSystem fs, RegionInfo info)
    throws IOException {
    Path rootDir = CommonFSUtils.getRootDir(conf);
    Path regionDir = FSUtils.getRegionDirFromRootDir(rootDir, info);
    return fs.exists(regionDir);
  }

  /**
   * Cleans up all the files for a HRegion by archiving the HFiles to the archive directory
   * @param conf     the configuration to use
   * @param fs       the file system object
   * @param info     RegionInfo for region to be deleted
   * @param rootDir  {@link Path} to the root directory where hbase files are stored (for building
   *                 the archive path)
   * @param tableDir {@link Path} to where the table is being stored (for building the archive path)
   */
  @Override
  public void archiveRegion(Configuration conf, FileSystem fs, RegionInfo info, Path rootDir,
    Path tableDir) throws IOException {
    archiveRegion(conf, fs, rootDir, tableDir, FSUtils.getRegionDirFromRootDir(rootDir, info));
  }

  /**
   * Remove an entire region from the table directory via archiving the region's hfiles.
   * @param fs        {@link FileSystem} from which to remove the region
   * @param rootdir   {@link Path} to the root directory where hbase files are stored (for building
   *                  the archive path)
   * @param tableDir  {@link Path} to where the table is being stored (for building the archive
   *                  path)
   * @param regionDir {@link Path} to where a region is being stored (for building the archive path)
   * @return <tt>true</tt> if the region was successfully deleted. <tt>false</tt> if the filesystem
   *         operations could not complete.
   * @throws IOException if the request cannot be completed
   */
  public static boolean archiveRegion(Configuration conf, FileSystem fs, Path rootdir,
    Path tableDir, Path regionDir) throws IOException {
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
    List<File> failedArchive = resolveAndArchive(conf, fs, regionArchiveDir, toArchive,
      EnvironmentEdgeManager.currentTime());
    if (!failedArchive.isEmpty()) {
      throw new FailedArchiveException(
        "Failed to archive/delete all the files for region:" + regionDir.getName() + " into "
          + regionArchiveDir + ". Something is probably awry on the filesystem.",
        failedArchive.stream().map(FUNC_FILE_TO_PATH).collect(Collectors.toList()));
    }
    // if that was successful, then we delete the region
    return deleteRegionWithoutArchiving(fs, regionDir);
  }

  // We need this method instead of Threads.getNamedThreadFactory() to pass some tests.
  // The difference from Threads.getNamedThreadFactory() is that it doesn't fix ThreadGroup for
  // new threads. If we use Threads.getNamedThreadFactory(), we will face ThreadGroup related
  // issues in some tests.
  private static ThreadFactory getThreadFactory(String archiverName) {
    return new ThreadFactory() {
      final AtomicInteger threadNumber = new AtomicInteger(1);

      @Override
      public Thread newThread(Runnable r) {
        final String name = archiverName + "-" + threadNumber.getAndIncrement();
        Thread t = new Thread(r, name);
        t.setDaemon(true);
        return t;
      }
    };
  }

  /**
   * Removes from the specified region the store files of the specified column family, either by
   * archiving them or outright deletion
   * @param fs        the filesystem where the store files live
   * @param conf      {@link Configuration} to examine to determine the archive directory
   * @param parent    Parent region hosting the store files
   * @param familyDir {@link Path} to where the family is being stored
   * @param family    the family hosting the store files
   * @throws IOException if the files could not be correctly disposed.
   */
  @Override
  public void archiveFamilyByFamilyDir(FileSystem fs, Configuration conf, RegionInfo parent,
    Path familyDir, byte[] family) throws IOException {
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
    List<File> failedArchive =
      resolveAndArchive(conf, fs, storeArchiveDir, toArchive, EnvironmentEdgeManager.currentTime());
    if (!failedArchive.isEmpty()) {
      throw new FailedArchiveException(
        "Failed to archive/delete all the files for region:"
          + Bytes.toString(parent.getRegionName()) + ", family:" + Bytes.toString(family) + " into "
          + storeArchiveDir + ". Something is probably awry on the filesystem.",
        failedArchive.stream().map(FUNC_FILE_TO_PATH).collect(Collectors.toList()));
    }
  }

  /**
   * Resolve any conflict with an existing archive file via timestamp-append renaming of the
   * existing file and then archive the passed in files.
   * @param fs             {@link FileSystem} on which to archive the files
   * @param baseArchiveDir base archive directory to store the files. If any of the files to archive
   *                       are directories, will append the name of the directory to the base
   *                       archive directory name, creating a parallel structure.
   * @param toArchive      files/directories that need to be archvied
   * @param start          time the archiving started - used for resolving archive conflicts.
   * @return the list of failed to archive files.
   * @throws IOException if an unexpected file operation exception occurred
   */
  private static List<File> resolveAndArchive(Configuration conf, FileSystem fs,
    Path baseArchiveDir, Collection<File> toArchive, long start) throws IOException {
    // Early exit if no files to archive
    if (toArchive.isEmpty()) {
      LOG.trace("No files to archive, returning an empty list.");
      return Collections.emptyList();
    }

    LOG.trace("Preparing to archive files into directory: {}", baseArchiveDir);

    // Ensure the archive directory exists
    ensureArchiveDirectoryExists(fs, baseArchiveDir);

    // Thread-safe collection for storing failures
    Queue<File> failures = new ConcurrentLinkedQueue<>();
    String startTime = Long.toString(start);

    // Separate files and directories for processing
    List<File> filesOnly = new ArrayList<>();
    for (File file : toArchive) {
      if (file.isFile()) {
        filesOnly.add(file);
      } else {
        handleDirectory(conf, fs, baseArchiveDir, failures, file, start);
      }
    }

    // Archive files concurrently
    archiveFilesConcurrently(conf, baseArchiveDir, filesOnly, failures, startTime);

    return new ArrayList<>(failures); // Convert to a List for the return value
  }

  private static void ensureArchiveDirectoryExists(FileSystem fs, Path baseArchiveDir)
    throws IOException {
    if (!fs.exists(baseArchiveDir) && !fs.mkdirs(baseArchiveDir)) {
      throw new IOException("Failed to create the archive directory: " + baseArchiveDir);
    }
    LOG.trace("Archive directory ready: {}", baseArchiveDir);
  }

  private static void handleDirectory(Configuration conf, FileSystem fs, Path baseArchiveDir,
    Queue<File> failures, File directory, long start) {
    LOG.trace("Processing directory: {}, archiving its children.", directory);
    Path subArchiveDir = new Path(baseArchiveDir, directory.getName());

    try {
      Collection<File> children = directory.getChildren();
      failures.addAll(resolveAndArchive(conf, fs, subArchiveDir, children, start));
    } catch (IOException e) {
      LOG.warn("Failed to archive directory: {}", directory, e);
      failures.add(directory);
    }
  }

  private static void archiveFilesConcurrently(Configuration conf, Path baseArchiveDir,
    List<File> files, Queue<File> failures, String startTime) {
    LOG.trace("Archiving {} files concurrently into directory: {}", files.size(), baseArchiveDir);
    Map<File, Future<Boolean>> futureMap = new HashMap<>();
    // Submit file archiving tasks
    // default is 16 which comes equal hbase.hstore.blockingStoreFiles default value
    int maxThreads = conf.getInt("hbase.hfilearchiver.per.region.thread.pool.max", 16);
    ThreadPoolExecutor hfilesArchiveExecutor = Threads.getBoundedCachedThreadPool(maxThreads, 30L,
      TimeUnit.SECONDS, getThreadFactory("HFileArchiverPerRegion-"));
    try {
      for (File file : files) {
        Future<Boolean> future = hfilesArchiveExecutor
          .submit(() -> resolveAndArchiveFile(baseArchiveDir, file, startTime));
        futureMap.put(file, future);
      }

      // Process results of each task
      for (Map.Entry<File, Future<Boolean>> entry : futureMap.entrySet()) {
        File file = entry.getKey();
        try {
          if (!entry.getValue().get()) {
            LOG.warn("Failed to archive file: {} into directory: {}", file, baseArchiveDir);
            failures.add(file);
          }
        } catch (InterruptedException e) {
          LOG.error("Archiving interrupted for file: {}", file, e);
          Thread.currentThread().interrupt(); // Restore interrupt status
          failures.add(file);
        } catch (ExecutionException e) {
          LOG.error("Archiving failed for file: {}", file, e);
          failures.add(file);
        }
      }
    } finally {
      hfilesArchiveExecutor.shutdown();
    }
  }

  /**
   * Attempt to archive the passed in file to the archive directory.
   * <p>
   * If the same file already exists in the archive, it is moved to a timestamped directory under
   * the archive directory and the new file is put in its place.
   * @param archiveDir       {@link Path} to the directory that stores the archives of the hfiles
   * @param currentFile      {@link Path} to the original HFile that will be archived
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
        LOG.error(
          "{} already exists in archive with different size than current {}."
            + " archiveLen: {} currentLen: {} archiveMtime: {} currentMtime: {}",
          archiveFile, currentFile, archiveLen, curLen, archiveMtime, curMtime);
        throw new IOException(
          archiveFile + " already exists in archive with different size" + " than " + currentFile);
      }

      LOG.error(
        "{} already exists in archive, moving to timestamped backup and overwriting"
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
        LOG.warn("Failed to archive " + currentFile
          + " because it does not exist! Skipping and continuing on.", fnfe);
        success = true;
      } catch (IOException e) {
        success = false;
        // When HFiles are placed on a filesystem other than HDFS a rename operation can be a
        // non-atomic file copy operation. It can take a long time to copy a large hfile and if
        // interrupted there may be a partially copied file present at the destination. We must
        // remove the partially copied file, if any, or otherwise the archive operation will fail
        // indefinitely from this point.
        LOG.warn("Failed to archive " + currentFile + " on try #" + i, e);
        try {
          fs.delete(archiveFile, false);
        } catch (FileNotFoundException fnfe) {
          // This case is fine.
        } catch (IOException ee) {
          // Complain about other IO exceptions
          LOG.warn("Failed to clean up from failure to archive " + currentFile + " on try #" + i,
            ee);
        }
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
   * @param fs        FileSystem from which to delete the region
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
   * Wrapper to handle file operations uniformly
   */
  private static abstract class File {
    protected final FileSystem fs;

    public File(FileSystem fs) {
      this.fs = fs;
    }

    /**
     * Check to see if this is a file or a directory
     * @return <tt>true</tt> if it is a file, <tt>false</tt> otherwise
     * @throws IOException on {@link FileSystem} connection error
     */
    abstract boolean isFile() throws IOException;

    /**
     * @return if this is a directory, returns all the children in the directory, otherwise returns
     *         an empty list
     */
    abstract Collection<File> getChildren() throws IOException;

    /**
     * close any outside readers of the file
     */
    abstract void close() throws IOException;

    /** Returns the name of the file (not the full fs path, just the individual file name) */
    abstract String getName();

    /** Returns the path to this file */
    abstract Path getPath();

    /**
     * Move the file to the given destination
     * @return <tt>true</tt> on success
     */
    public boolean moveAndClose(Path dest) throws IOException {
      this.close();
      Path p = this.getPath();
      return CommonFSUtils.renameAndSetModifyTime(fs, p, dest);
    }

    /** Returns the {@link FileSystem} on which this file resides */
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
}
