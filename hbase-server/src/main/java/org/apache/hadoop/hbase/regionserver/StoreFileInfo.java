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

package org.apache.hadoop.hbase.regionserver;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.HalfStoreFileReader;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileInfo;
import org.apache.hadoop.hbase.io.hfile.ReaderContext;
import org.apache.hadoop.hbase.io.hfile.ReaderContext.ReaderType;
import org.apache.hadoop.hbase.io.hfile.ReaderContextBuilder;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StoreFile info.
 * The info could be for a plain storefile/hfile or it could be a reference or link to a storefile.
 */
@InterfaceAudience.Private
public class StoreFileInfo {
  private static final Logger LOG = LoggerFactory.getLogger(StoreFileInfo.class);

  /**
   * A non-capture group, for hfiles, so that this can be embedded.
   * HFiles are uuid ([0-9a-z]+). Bulk loaded hfiles has (_SeqId_[0-9]+_) has suffix.
   * The mob del file has (_del) as suffix.
   */
  public static final String HFILE_NAME_REGEX = "[0-9a-f]+(?:(?:_SeqId_[0-9]+_)|(?:_del))?";

  /** Regex that will work for hfiles */
  private static final Pattern HFILE_NAME_PATTERN =
    Pattern.compile("^(" + HFILE_NAME_REGEX + ")");

  /**
   * A non-capture group, for del files, so that this can be embedded.
   * A del file has (_del) as suffix.
   */
  public static final String DELFILE_NAME_REGEX = "[0-9a-f]+(?:_del)";

  /** Regex that will work for del files */
  private static final Pattern DELFILE_NAME_PATTERN =
    Pattern.compile("^(" + DELFILE_NAME_REGEX + ")");

  /**
   * Regex that will work for straight reference names ({@code <hfile>.<parentEncRegion>})
   * and hfilelink reference names ({@code <table>=<region>-<hfile>.<parentEncRegion>})
   * If reference, then the regex has more than just one group.
   * Group 1, hfile/hfilelink pattern, is this file's id.
   * Group 2 '(.+)' is the reference's parent region name.
   */
  private static final Pattern REF_NAME_PATTERN =
    Pattern.compile(String.format("^(%s|%s)\\.(.+)$",
      HFILE_NAME_REGEX, HFileLink.LINK_NAME_REGEX));

  public static final String STORE_FILE_READER_NO_READAHEAD = "hbase.store.reader.no-readahead";
  public static final boolean DEFAULT_STORE_FILE_READER_NO_READAHEAD = false;

  // Configuration
  private final Configuration conf;

  // FileSystem handle
  private final FileSystem fs;

  // HDFS blocks distribution information
  private HDFSBlocksDistribution hdfsBlocksDistribution = null;

  private HFileInfo hfileInfo;

  // If this storefile references another, this is the reference instance.
  private final Reference reference;

  // If this storefile is a link to another, this is the link instance.
  private final HFileLink link;

  private final Path initialPath;

  private RegionCoprocessorHost coprocessorHost;

  // timestamp on when the file was created, is 0 and ignored for reference or link files
  private long createdTimestamp;

  private long size;

  private final boolean primaryReplica;

  private final boolean noReadahead;

  // Counter that is incremented every time a scanner is created on the
  // store file. It is decremented when the scan on the store file is
  // done.
  final AtomicInteger refCount = new AtomicInteger(0);

  /**
   * Cached fileStatus.
   * Used selectively. Cache the FileStatus if this StoreFileInfo is for a plain StoreFile.
   * Save on having to go to the filesystem every time (costly). We cannot cache FileStatus if this
   * StoreFileInfo is for a link or for a reference that might in turn be to a link. The file behind
   * a link can move during the lifetime of this StoreFileInfo invalidating what we might have
   * cached here; do a lookup of FileStatus every time when a link to be safe.
   */
  private FileStatus cachedFileStatus = null;

  /**
   * Create a Store File Info
   * @param conf the {@link Configuration} to use
   * @param fs The current file system to use.
   * @param initialPath The {@link Path} of the file
   * @param primaryReplica true if this is a store file for primary replica, otherwise false.
   */
  public StoreFileInfo(final Configuration conf, final FileSystem fs, final Path initialPath,
      final boolean primaryReplica) throws IOException {
    this(conf, fs, null, initialPath, primaryReplica);
  }

  private StoreFileInfo(final Configuration conf, final FileSystem fs, final FileStatus fileStatus,
      final Path initialPath, final boolean primaryReplica) throws IOException {
    this.fs = Objects.requireNonNull(fs);
    this.conf = Objects.requireNonNull(conf);
    this.initialPath = Objects.requireNonNull(initialPath);
    this.primaryReplica = primaryReplica;
    this.noReadahead = this.conf.getBoolean(STORE_FILE_READER_NO_READAHEAD,
        DEFAULT_STORE_FILE_READER_NO_READAHEAD);
    if (HFileLink.isHFileLink(initialPath)) {
      this.reference = null;
      this.link = HFileLink.buildFromHFileLinkPattern(conf, initialPath);
      LOG.trace("{} is a link", initialPath);
    } else if (isReference(initialPath)) {
      this.reference = Reference.read(fs, initialPath);
      Path referencedPath = getReferredToFile(initialPath);
      // Check if the referenced file is a link.
      this.link = HFileLink.isHFileLink(referencedPath)?
        HFileLink.buildFromHFileLinkPattern(conf, referencedPath): null;
      LOG.trace("{} is a {} reference to {} (link={})",
        initialPath, reference.getFileRegion(), referencedPath, link == null);
    } else if (isHFile(initialPath)) {
      // Safe to cache passed filestatus when NOT a link or reference; i.e. when it filestatus on
      // a plain storefile/hfile.
      assert fileStatus == null || fileStatus.getPath().equals(initialPath);
      this.cachedFileStatus = fileStatus != null?
        fileStatus: this.fs.getFileStatus(this.initialPath);
      this.createdTimestamp = this.cachedFileStatus.getModificationTime();
      this.size = this.cachedFileStatus.getLen();
      this.reference = null;
      this.link = null;
      LOG.trace("{}", initialPath);
    } else {
      throw new IOException("Path=" + initialPath + " doesn't look like a valid StoreFile; " +
        "it is not a link, a reference or an hfile.");
    }
  }

  /**
   * Create a Store File Info
   * @param conf the {@link Configuration} to use
   * @param fs The current file system to use.
   * @param fileStatus The {@link FileStatus} of the file
   */
  public StoreFileInfo(final Configuration conf, final FileSystem fs, final FileStatus fileStatus)
      throws IOException {
    this(conf, fs, fileStatus, fileStatus.getPath(), true);
  }

  /**
   * Create a Store File Info from an HFileLink
   * @param conf The {@link Configuration} to use
   * @param fs The current file system to use
   * @param fileStatus The {@link FileStatus} of the file
   */
  public StoreFileInfo(final Configuration conf, final FileSystem fs, final FileStatus fileStatus,
      final HFileLink link) {
    this(conf, fs, fileStatus, null, link);
  }

  /**
   * Create a Store File Info from an HFileLink
   * @param conf The {@link Configuration} to use
   * @param fs The current file system to use
   * @param fileStatus The {@link FileStatus} of the file
   * @param reference The reference instance
   */
  public StoreFileInfo(final Configuration conf, final FileSystem fs, final FileStatus fileStatus,
      final Reference reference) {
    this(conf, fs, fileStatus, reference, null);
  }

  /**
   * Create a Store File Info from an HFileLink and a Reference
   * @param conf The {@link Configuration} to use
   * @param fs The current file system to use
   * @param fileStatus The {@link FileStatus} of the file
   * @param reference The reference instance
   * @param link The link instance
   */
  public StoreFileInfo(final Configuration conf, final FileSystem fs, final FileStatus fileStatus,
      final Reference reference, final HFileLink link) {
    this.fs = Objects.requireNonNull(fs);
    this.conf = Objects.requireNonNull(conf);
    this.primaryReplica = false;
    this.initialPath = (fileStatus == null) ? null : fileStatus.getPath();
    this.createdTimestamp = (fileStatus == null) ? 0 :fileStatus.getModificationTime();
    this.reference = reference;
    this.link = link;
    this.noReadahead = this.conf.getBoolean(STORE_FILE_READER_NO_READAHEAD,
        DEFAULT_STORE_FILE_READER_NO_READAHEAD);
  }

  public StoreFileInfo(StoreFileInfo other, FileSystem fileSystem) {
    this.fs = fileSystem;
    this.conf = other.conf;
    this.primaryReplica = other.primaryReplica;
    this.initialPath = other.initialPath;
    this.createdTimestamp = other.createdTimestamp;
    this.link = other.link;
    this.reference = other.reference;
    this.noReadahead = other.noReadahead;
  }

  /**
   * Size of the Hfile
   * @return size
   */
  public long getSize() {
    return size;
  }

  /**
   * Sets the region coprocessor env.
   */
  public void setRegionCoprocessorHost(RegionCoprocessorHost coprocessorHost) {
    this.coprocessorHost = coprocessorHost;
  }

  /*
   * @return the Reference object associated to this StoreFileInfo.
   *         null if the StoreFile is not a reference.
   */
  public Reference getReference() {
    return this.reference;
  }

  /** @return True if the store file is a Reference */
  public boolean isReference() {
    return this.reference != null;
  }

  /** @return True if the store file is a top Reference */
  public boolean isTopReference() {
    return this.reference != null && Reference.isTopFileRegion(this.reference.getFileRegion());
  }

  /** @return True if the store file is a link */
  public boolean isLink() {
    return this.link != null && this.reference == null;
  }

  /** @return the HDFS block distribution */
  public HDFSBlocksDistribution getHDFSBlockDistribution() {
    return this.hdfsBlocksDistribution;
  }

  StoreFileReader createReader(ReaderContext context, CacheConfig cacheConf) throws IOException {
    return this.reference != null?
      new HalfStoreFileReader(context, hfileInfo, cacheConf, reference, refCount, conf):
      new StoreFileReader(context, hfileInfo, cacheConf, refCount, conf);
  }

  ReaderContext createReaderContext(boolean doDropBehind, long readahead, ReaderType type)
      throws IOException {
    FSDataInputStreamWrapper in;
    FileStatus status;
    if (this.link != null) {
      // HFileLink
      in = new FSDataInputStreamWrapper(fs, this.link, doDropBehind, readahead);
      status = this.link.getFileStatus(fs);
    } else if (this.reference != null) {
      // HFile Reference
      Path referencePath = getReferredToFile(this.getPath());
      try {
        in = new FSDataInputStreamWrapper(fs, referencePath, doDropBehind, readahead);
      } catch (FileNotFoundException fnfe) {
        throw decorateFileNotFoundException(fnfe);
      }
      status = fs.getFileStatus(referencePath);
    } else {
      in = new FSDataInputStreamWrapper(fs, this.getPath(), doDropBehind, readahead);
      if (this.cachedFileStatus == null) {
        // Safe to cache filestatus for a plain storefile/hfile.
        this.cachedFileStatus = this.fs.getFileStatus(this.initialPath);
      }
      status = this.cachedFileStatus;
    }
    long length = status.getLen();
    ReaderContextBuilder contextBuilder = new ReaderContextBuilder()
        .withInputStreamWrapper(in)
        .withFileSize(length)
        .withPrimaryReplicaReader(this.primaryReplica)
        .withReaderType(type)
        .withFileSystem(fs);
    if (this.reference != null) {
      contextBuilder.withFilePath(this.getPath());
    } else {
      contextBuilder.withFilePath(status.getPath());
    }
    return contextBuilder.build();
  }

  /**
   * Compute the HDFS Block Distribution for this StoreFile
   */
  public HDFSBlocksDistribution computeHDFSBlocksDistribution() throws IOException {
    if (this.link != null) {
      // Guard against case where file behind link has moved when we go to calculate distribution;
      // e.g. from data dir to archive dir. Retry up to number of locations under the link.
      FileNotFoundException exToThrow = null;
      for (int i = 0; i < this.link.getLocations().length; i++) {
        try {
          return computeHDFSBlocksDistributionInternal();
        } catch (FileNotFoundException fnfe) {
          // Try the other locations -- file behind link may have moved.
          exToThrow = fnfe;
        }
      }
      throw decorateFileNotFoundException(exToThrow);
    } else {
      return computeHDFSBlocksDistributionInternal();
    }
  }

  private HDFSBlocksDistribution computeHDFSBlocksDistributionInternal() throws IOException {
    FileStatus status = getFileStatus();
    if (this.reference != null) {
      return computeRefFileHDFSBlockDistribution(status);
    } else {
      return FSUtils.computeHDFSBlocksDistribution(this.fs, status, 0, status.getLen());
    }
  }

  /**
   * Get the {@link FileStatus} of the file referenced by this StoreFileInfo.
   * This {@link StoreFileInfo} could be for a link or a reference or a plain hfile/storefile; get
   * the filestatus for whatever the link or reference points to (or just the plain hfile/storefile
   * if not a link/reference). Info}. If a link, when you go to use the passed FileStatus, the file
   * may have moved; e.g. from data to archive... be aware.
   * @return The {@link FileStatus} of the file referenced by this StoreFileInfo
   */
  private FileStatus getReferencedFileStatus() throws IOException {
    if (this.cachedFileStatus != null) {
      return this.cachedFileStatus;
    }
    FileStatus status;
    if (this.reference != null) {
      if (this.link != null) {
        status = this.link.getFileStatus(this.fs);
      } else {
        try {
          Path referencePath = getReferredToFile(this.getPath());
          status = this.fs.getFileStatus(referencePath);
        } catch (FileNotFoundException ex) {
          throw decorateFileNotFoundException(ex);
        }
      }
    } else {
      try {
        if (this.link != null) {
          status = this.link.getFileStatus(this.fs);
        } else {
          status = this.fs.getFileStatus(this.initialPath);
          // Take this opportunity to cache the filestatus. It is safe to cache filestatus when NOT
          // a link or reference; i.e. when it filestatus on a plain storefile/hfile.
          this.cachedFileStatus = status;
        }
      } catch (FileNotFoundException fnfe) {
        throw decorateFileNotFoundException(fnfe);
      }
    }
    return status;
  }

  /** @return The {@link Path} of the file */
  public Path getPath() {
    return initialPath;
  }

  /**
   * @return A new FNFE with <param>fnfe</param> as cause but including info if reference or link.
   */
  private FileNotFoundException decorateFileNotFoundException(FileNotFoundException fnfe) {
    FileNotFoundException newFnfe = new FileNotFoundException(toString());
    newFnfe.initCause(fnfe);
    return newFnfe;
  }

  /**
   * @return {@link FileStatus} for the linked or referenced file or if not a link/reference, then
   *   the FileStatus for the plain storefile (Be aware, if a link, the file may have moved
   *   by the time you go to use the FileStatus).
   */
  public FileStatus getFileStatus() throws IOException {
    return getReferencedFileStatus();
  }

  /** @return Get the modification time of the file. */
  public long getModificationTime() throws IOException {
    return getFileStatus().getModificationTime();
  }

  @Override
  public String toString() {
    return isLink()? this.link.toString(): this.getPath() +
      (isReference()? "->" + getReferredToFile(this.getPath()) + "-" + reference: "");
  }

  /**
   * @param path Path to check.
   * @return True if the path has format of a HFile.
   */
  public static boolean isHFile(final Path path) {
    return isHFile(path.getName());
  }

  public static boolean isHFile(final String fileName) {
    Matcher m = HFILE_NAME_PATTERN.matcher(fileName);
    return m.matches() && m.groupCount() > 0;
  }

  /**
   * @param path Path to check.
   * @return True if the path has format of a del file.
   */
  public static boolean isDelFile(final Path path) {
    return isDelFile(path.getName());
  }

  /**
   * @param fileName Sting version of path to validate.
   * @return True if the file name has format of a del file.
   */
  public static boolean isDelFile(final String fileName) {
    Matcher m = DELFILE_NAME_PATTERN.matcher(fileName);
    return m.matches() && m.groupCount() > 0;
  }

  /**
   * @param path Path to check.
   * @return True if the path has format of a HStoreFile reference.
   */
  public static boolean isReference(final Path path) {
    return isReference(path.getName());
  }

  /**
   * @param name file name to check.
   * @return True if the path has format of a HStoreFile reference.
   */
  public static boolean isReference(final String name) {
    Matcher m = REF_NAME_PATTERN.matcher(name);
    return m.matches() && m.groupCount() > 1;
  }

  /**
   * @return timestamp when this file was created (as returned by filesystem)
   */
  public long getCreatedTimestamp() {
    return createdTimestamp;
  }

  /*
   * Return path to the file referred to by a Reference.  Presumes a directory
   * hierarchy of <code>${hbase.rootdir}/data/${namespace}/tablename/regionname/familyname</code>.
   * @param p Path to a Reference file.
   * @return Calculated path to parent region file.
   * @throws IllegalArgumentException when path regex fails to match.
   */
  public static Path getReferredToFile(final Path p) {
    Matcher m = REF_NAME_PATTERN.matcher(p.getName());
    if (m == null || !m.matches()) {
      LOG.warn("Failed match of store file name {}", p.toString());
      throw new IllegalArgumentException("Failed match of store file name " +
          p.toString());
    }

    // Other region name is suffix on the passed Reference file name
    String otherRegion = m.group(2);
    // Tabledir is up two directories from where Reference was written.
    Path tableDir = p.getParent().getParent().getParent();
    String nameStrippedOfSuffix = m.group(1);
    LOG.trace("reference {} to region={} hfile={}", p, otherRegion, nameStrippedOfSuffix);

    // Build up new path with the referenced region in place of our current
    // region in the reference path.  Also strip regionname suffix from name.
    return new Path(new Path(new Path(tableDir, otherRegion),
      p.getParent().getName()), nameStrippedOfSuffix);
  }

  /**
   * Validate the store file name.
   * @param fileName name of the file to validate
   * @return <tt>true</tt> if the file could be a valid store file, <tt>false</tt> otherwise
   */
  public static boolean validateStoreFileName(final String fileName) {
    return (HFileLink.isHFileLink(fileName) || isReference(fileName)) || !fileName.contains("-");
  }

  /**
   * Return if the specified file is a valid store file or not.
   * @param fileStatus The {@link FileStatus} of the file
   * @return <tt>true</tt> if the file is valid
   */
  public static boolean isValid(final FileStatus fileStatus)
      throws IOException {
    final Path p = fileStatus.getPath();

    if (fileStatus.isDirectory()) {
      return false;
    }

    // Check for empty hfile. Should never be the case but can happen
    // after data loss in hdfs for whatever reason (upgrade, etc.): HBASE-646
    // NOTE: that the HFileLink is just a name, so it's an empty file.
    if (!HFileLink.isHFileLink(p) && fileStatus.getLen() <= 0) {
      LOG.warn("Skipping {} because it is empty. HBASE-646 DATA LOSS?", p);
      return false;
    }

    return validateStoreFileName(p.getName());
  }

  /**
   * helper function to compute HDFS blocks distribution of a given reference
   * file. For reference file, we don't compute the exact value. We use an
   * estimate instead presuming it good enough. We assume bottom part
   * takes the first half of a reference file, top part takes the second half
   * of the reference file. This is just estimate, given
   * midkey ofregion != midkey of HFile, also the number and size of keys vary.
   * If this estimate isn't good enough, we can improve it later.
   * @param status  The reference FileStatus
   * @return HDFS blocks distribution
   */
  private HDFSBlocksDistribution computeRefFileHDFSBlockDistribution(final FileStatus status)
      throws IOException {
    if (status == null) {
      return null;
    }

    long start;
    long length;

    if (Reference.isTopFileRegion(this.reference.getFileRegion())) {
      start = status.getLen()/2;
      length = status.getLen() - status.getLen()/2;
    } else {
      start = 0;
      length = status.getLen()/2;
    }
    return FSUtils.computeHDFSBlocksDistribution(fs, status, start, length);
  }

  @Override
  public boolean equals(Object that) {
    if (this == that) {
      return true;
    }
    if (that == null) {
      return false;
    }

    if (!(that instanceof StoreFileInfo)) {
      return false;
    }

    StoreFileInfo o = (StoreFileInfo)that;
    if (initialPath != null && o.initialPath == null) {
      return false;
    }
    if (initialPath == null && o.initialPath != null) {
      return false;
    }
    if (initialPath != o.initialPath && initialPath != null &&
        !initialPath.equals(o.initialPath)) {
      return false;
    }
    if (reference != null && o.reference == null) {
      return false;
    }
    if (reference == null && o.reference != null) {
      return false;
    }
    if (reference != o.reference && reference != null &&
        !reference.equals(o.reference)) {
      return false;
    }

    if (link != null && o.link == null) {
      return false;
    }
    if (link == null && o.link != null) {
      return false;
    }
    if (link != o.link && link != null && !link.equals(o.link)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int hash = 17;
    hash = hash * 31 + ((reference == null) ? 0 : reference.hashCode());
    hash = hash * 31 + ((initialPath ==  null) ? 0 : initialPath.hashCode());
    hash = hash * 31 + ((link == null) ? 0 : link.hashCode());
    return  hash;
  }

  /**
   * Return the active file name that contains the real data.
   * <p>
   * For referenced hfile, we will return the name of the reference file as it will be used to
   * construct the StoreFileReader. And for linked hfile, we will return the name of the file being
   * linked.
   */
  public String getActiveFileName() {
    if (reference != null || link == null) {
      return initialPath.getName();
    } else {
      return HFileLink.getReferencedHFileName(initialPath.getName());
    }
  }

  FileSystem getFileSystem() {
    return this.fs;
  }

  /**
   * @return True if passed filesystem is same as the one this instance was created against.
   */
  public boolean isFileSystem(FileSystem other) {
    return this.fs.equals(other);
  }

  Configuration getConf() {
    return this.conf;
  }

  boolean isNoReadahead() {
    return this.noReadahead;
  }

  HFileInfo getHFileInfo() {
    return hfileInfo;
  }

  void initHDFSBlocksDistribution() throws IOException {
    hdfsBlocksDistribution = computeHDFSBlocksDistribution();
  }

  StoreFileReader preStoreFileReaderOpen(ReaderContext context, CacheConfig cacheConf)
      throws IOException {
    StoreFileReader reader = null;
    if (this.coprocessorHost != null) {
      reader = this.coprocessorHost.preStoreFileReaderOpen(fs, this.getPath(),
          context.getInputStreamWrapper(), context.getFileSize(),
          cacheConf, reference);
    }
    return reader;
  }

  StoreFileReader postStoreFileReaderOpen(ReaderContext context, CacheConfig cacheConf,
      StoreFileReader reader) throws IOException {
    StoreFileReader res = reader;
    if (this.coprocessorHost != null) {
      res = this.coprocessorHost.postStoreFileReaderOpen(fs, this.getPath(),
          context.getInputStreamWrapper(), context.getFileSize(),
          cacheConf, reference, reader);
    }
    return res;
  }

  public void initHFileInfo(ReaderContext context) throws IOException {
    this.hfileInfo = new HFileInfo(context, conf);
  }
}
