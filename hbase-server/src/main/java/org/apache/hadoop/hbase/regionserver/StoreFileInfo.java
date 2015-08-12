/**
 *
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
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
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * Describe a StoreFile (hfile, reference, link)
 */
@InterfaceAudience.Private
public class StoreFileInfo {
  private static final Log LOG = LogFactory.getLog(StoreFileInfo.class);

  /**
   * A non-capture group, for hfiles, so that this can be embedded.
   * HFiles are uuid ([0-9a-z]+). Bulk loaded hfiles has (_SeqId_[0-9]+_) has suffix.
   */
  public static final String HFILE_NAME_REGEX = "[0-9a-f]+(?:_SeqId_[0-9]+_)?";

  /** Regex that will work for hfiles */
  private static final Pattern HFILE_NAME_PATTERN =
    Pattern.compile("^(" + HFILE_NAME_REGEX + ")");

  /**
   * Regex that will work for straight reference names (<hfile>.<parentEncRegion>)
   * and hfilelink reference names (<table>=<region>-<hfile>.<parentEncRegion>)
   * If reference, then the regex has more than just one group.
   * Group 1, hfile/hfilelink pattern, is this file's id.
   * Group 2 '(.+)' is the reference's parent region name.
   */
  private static final Pattern REF_NAME_PATTERN =
    Pattern.compile(String.format("^(%s|%s)\\.(.+)$",
      HFILE_NAME_REGEX, HFileLink.LINK_NAME_REGEX));

  // Configuration
  private Configuration conf;

  // FileSystem handle
  private final FileSystem fs;

  // HDFS blocks distribution information
  private HDFSBlocksDistribution hdfsBlocksDistribution = null;

  // If this storefile references another, this is the reference instance.
  private final Reference reference;

  // If this storefile is a link to another, this is the link instance.
  private final HFileLink link;

  private final Path initialPath;

  private RegionCoprocessorHost coprocessorHost;

  /**
   * Create a Store File Info
   * @param conf the {@link Configuration} to use
   * @param fs The current file system to use.
   * @param initialPath The {@link Path} of the file
   */
  public StoreFileInfo(final Configuration conf, final FileSystem fs, final Path initialPath)
      throws IOException {
    assert fs != null;
    assert initialPath != null;
    assert conf != null;

    this.fs = fs;
    this.conf = conf;
    this.initialPath = initialPath;
    Path p = initialPath;
    if (HFileLink.isHFileLink(p)) {
      // HFileLink
      this.reference = null;
      this.link = HFileLink.buildFromHFileLinkPattern(conf, p);
      if (LOG.isTraceEnabled()) LOG.trace(p + " is a link");
    } else if (isReference(p)) {
      this.reference = Reference.read(fs, p);
      Path referencePath = getReferredToFile(p);
      if (HFileLink.isHFileLink(referencePath)) {
        // HFileLink Reference
        this.link = HFileLink.buildFromHFileLinkPattern(conf, referencePath);
      } else {
        // Reference
        this.link = null;
      }
      if (LOG.isTraceEnabled()) LOG.trace(p + " is a " + reference.getFileRegion() +
              " reference to " + referencePath);
    } else if (isHFile(p)) {
      // HFile
      this.reference = null;
      this.link = null;
    } else {
      throw new IOException("path=" + p + " doesn't look like a valid StoreFile");
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
    this(conf, fs, fileStatus.getPath());
  }

  /**
   * Create a Store File Info from an HFileLink
   * @param conf the {@link Configuration} to use
   * @param fs The current file system to use.
   * @param fileStatus The {@link FileStatus} of the file
   */
  public StoreFileInfo(final Configuration conf, final FileSystem fs, final FileStatus fileStatus,
      final HFileLink link)
      throws IOException {
    this.fs = fs;
    this.conf = conf;
    // initialPath can be null only if we get a link.
    this.initialPath = (fileStatus == null) ? null : fileStatus.getPath();
      // HFileLink
    this.reference = null;
    this.link = link;
  }

  /**
   * Create a Store File Info from an HFileLink
   * @param conf
   * @param fs
   * @param fileStatus
   * @param reference
   * @throws IOException
   */
  public StoreFileInfo(final Configuration conf, final FileSystem fs, final FileStatus fileStatus,
      final Reference reference)
      throws IOException {
    this.fs = fs;
    this.conf = conf;
    this.initialPath = fileStatus.getPath();
    this.reference = reference;
    this.link = null;
  }

  /**
   * Sets the region coprocessor env.
   * @param coprocessorHost
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

  /**
   * Open a Reader for the StoreFile
   * @param fs The current file system to use.
   * @param cacheConf The cache configuration and block cache reference.
   * @return The StoreFile.Reader for the file
   */
  public StoreFile.Reader open(final FileSystem fs,
      final CacheConfig cacheConf, final boolean canUseDropBehind) throws IOException {
    FSDataInputStreamWrapper in;
    FileStatus status;

    final boolean doDropBehind = canUseDropBehind && cacheConf.shouldDropBehindCompaction();
    if (this.link != null) {
      // HFileLink
      in = new FSDataInputStreamWrapper(fs, this.link, doDropBehind);
      status = this.link.getFileStatus(fs);
    } else if (this.reference != null) {
      // HFile Reference
      Path referencePath = getReferredToFile(this.getPath());
      in = new FSDataInputStreamWrapper(fs, referencePath,
          doDropBehind);
      status = fs.getFileStatus(referencePath);
    } else {
      in = new FSDataInputStreamWrapper(fs, this.getPath(),
          doDropBehind);
      status = fs.getFileStatus(initialPath);
    }
    long length = status.getLen();
    hdfsBlocksDistribution = computeHDFSBlocksDistribution(fs);

    StoreFile.Reader reader = null;
    if (this.coprocessorHost != null) {
      reader = this.coprocessorHost.preStoreFileReaderOpen(fs, this.getPath(), in, length,
        cacheConf, reference);
    }
    if (reader == null) {
      if (this.reference != null) {
        reader = new HalfStoreFileReader(fs, this.getPath(), in, length, cacheConf, reference,
          conf);
      } else {
        reader = new StoreFile.Reader(fs, status.getPath(), in, length, cacheConf, conf);
      }
    }
    if (this.coprocessorHost != null) {
      reader = this.coprocessorHost.postStoreFileReaderOpen(fs, this.getPath(), in, length,
        cacheConf, reference, reader);
    }
    return reader;
  }

  /**
   * Compute the HDFS Block Distribution for this StoreFile
   */
  public HDFSBlocksDistribution computeHDFSBlocksDistribution(final FileSystem fs)
      throws IOException {

    // guard against the case where we get the FileStatus from link, but by the time we
    // call compute the file is moved again
    if (this.link != null) {
      FileNotFoundException exToThrow = null;
      for (int i = 0; i < this.link.getLocations().length; i++) {
        try {
          return computeHDFSBlocksDistributionInternal(fs);
        } catch (FileNotFoundException ex) {
          // try the other location
          exToThrow = ex;
        }
      }
      throw exToThrow;
    } else {
      return computeHDFSBlocksDistributionInternal(fs);
    }
  }

  private HDFSBlocksDistribution computeHDFSBlocksDistributionInternal(final FileSystem fs)
      throws IOException {
    FileStatus status = getReferencedFileStatus(fs);
    if (this.reference != null) {
      return computeRefFileHDFSBlockDistribution(fs, reference, status);
    } else {
      return FSUtils.computeHDFSBlocksDistribution(fs, status, 0, status.getLen());
    }
  }

  /**
   * Get the {@link FileStatus} of the file referenced by this StoreFileInfo
   * @param fs The current file system to use.
   * @return The {@link FileStatus} of the file referenced by this StoreFileInfo
   */
  public FileStatus getReferencedFileStatus(final FileSystem fs) throws IOException {
    FileStatus status;
    if (this.reference != null) {
      if (this.link != null) {
        FileNotFoundException exToThrow = null;
        for (int i = 0; i < this.link.getLocations().length; i++) {
          // HFileLink Reference
          try {
            return link.getFileStatus(fs);
          } catch (FileNotFoundException ex) {
            // try the other location
            exToThrow = ex;
          }
        }
        throw exToThrow;
      } else {
        // HFile Reference
        Path referencePath = getReferredToFile(this.getPath());
        status = fs.getFileStatus(referencePath);
      }
    } else {
      if (this.link != null) {
        FileNotFoundException exToThrow = null;
        for (int i = 0; i < this.link.getLocations().length; i++) {
          // HFileLink
          try {
            return link.getFileStatus(fs);
          } catch (FileNotFoundException ex) {
            // try the other location
            exToThrow = ex;
          }
        }
        throw exToThrow;
      } else {
        status = fs.getFileStatus(initialPath);
      }
    }
    return status;
  }

  /** @return The {@link Path} of the file */
  public Path getPath() {
    return initialPath;
  }

  /** @return The {@link FileStatus} of the file */
  public FileStatus getFileStatus() throws IOException {
    return getReferencedFileStatus(fs);
  }

  /** @return Get the modification time of the file. */
  public long getModificationTime() throws IOException {
    return getFileStatus().getModificationTime();
  }

  @Override
  public String toString() {
    return this.getPath() +
      (isReference() ? "-" + getReferredToFile(this.getPath()) + "-" + reference : "");
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
      LOG.warn("Failed match of store file name " + p.toString());
      throw new IllegalArgumentException("Failed match of store file name " +
          p.toString());
    }

    // Other region name is suffix on the passed Reference file name
    String otherRegion = m.group(2);
    // Tabledir is up two directories from where Reference was written.
    Path tableDir = p.getParent().getParent().getParent();
    String nameStrippedOfSuffix = m.group(1);
    if (LOG.isDebugEnabled()) {
      LOG.debug("reference '" + p + "' to region=" + otherRegion
        + " hfile=" + nameStrippedOfSuffix);
    }

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
    if (HFileLink.isHFileLink(fileName) || isReference(fileName))
      return(true);
    return !fileName.contains("-");
  }

  /**
   * Return if the specified file is a valid store file or not.
   * @param fileStatus The {@link FileStatus} of the file
   * @return <tt>true</tt> if the file is valid
   */
  public static boolean isValid(final FileStatus fileStatus)
      throws IOException {
    final Path p = fileStatus.getPath();

    if (fileStatus.isDirectory())
      return false;

    // Check for empty hfile. Should never be the case but can happen
    // after data loss in hdfs for whatever reason (upgrade, etc.): HBASE-646
    // NOTE: that the HFileLink is just a name, so it's an empty file.
    if (!HFileLink.isHFileLink(p) && fileStatus.getLen() <= 0) {
      LOG.warn("Skipping " + p + " because it is empty. HBASE-646 DATA LOSS?");
      return false;
    }

    return validateStoreFileName(p.getName());
  }

  /**
   * helper function to compute HDFS blocks distribution of a given reference
   * file.For reference file, we don't compute the exact value. We use some
   * estimate instead given it might be good enough. we assume bottom part
   * takes the first half of reference file, top part takes the second half
   * of the reference file. This is just estimate, given
   * midkey ofregion != midkey of HFile, also the number and size of keys vary.
   * If this estimate isn't good enough, we can improve it later.
   * @param fs  The FileSystem
   * @param reference  The reference
   * @param status  The reference FileStatus
   * @return HDFS blocks distribution
   */
  private static HDFSBlocksDistribution computeRefFileHDFSBlockDistribution(
      final FileSystem fs, final Reference reference, final FileStatus status)
      throws IOException {
    if (status == null) {
      return null;
    }

    long start = 0;
    long length = 0;

    if (Reference.isTopFileRegion(reference.getFileRegion())) {
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
    if (this == that) return true;
    if (that == null) return false;

    if (!(that instanceof StoreFileInfo)) return false;

    StoreFileInfo o = (StoreFileInfo)that;
    if (initialPath != null && o.initialPath == null) return false;
    if (initialPath == null && o.initialPath != null) return false;
    if (initialPath != o.initialPath && initialPath != null
            && !initialPath.equals(o.initialPath)) return false;

    if (reference != null && o.reference == null) return false;
    if (reference == null && o.reference != null) return false;
    if (reference != o.reference && reference != null
            && !reference.equals(o.reference)) return false;

    if (link != null && o.link == null) return false;
    if (link == null && o.link != null) return false;
    if (link != o.link && link != null && !link.equals(o.link)) return false;

    return true;
  };


  @Override
  public int hashCode() {
    int hash = 17;
    hash = hash * 31 + ((reference == null) ? 0 : reference.hashCode());
    hash = hash * 31 + ((initialPath ==  null) ? 0 : initialPath.hashCode());
    hash = hash * 31 + ((link == null) ? 0 : link.hashCode());
    return  hash;
  }
}
