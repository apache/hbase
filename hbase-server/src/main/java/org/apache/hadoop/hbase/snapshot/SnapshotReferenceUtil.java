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

package org.apache.hadoop.hbase.snapshot;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;

/**
 * Utility methods for interacting with the snapshot referenced files.
 */
@InterfaceAudience.Private
public final class SnapshotReferenceUtil {
  private static final Log LOG = LogFactory.getLog(SnapshotReferenceUtil.class);

  public interface StoreFileVisitor {
    void storeFile(final HRegionInfo regionInfo, final String familyName,
       final SnapshotRegionManifest.StoreFile storeFile) throws IOException;
  }

  public interface SnapshotVisitor extends StoreFileVisitor {
  }

  private SnapshotReferenceUtil() {
    // private constructor for utility class
  }

  /**
   * Iterate over the snapshot store files
   *
   * @param conf The current {@link Configuration} instance.
   * @param fs {@link FileSystem}
   * @param snapshotDir {@link Path} to the Snapshot directory
   * @param visitor callback object to get the referenced files
   * @throws IOException if an error occurred while scanning the directory
   */
  public static void visitReferencedFiles(final Configuration conf, final FileSystem fs,
      final Path snapshotDir, final SnapshotVisitor visitor)
      throws IOException {
    SnapshotDescription desc = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);
    visitReferencedFiles(conf, fs, snapshotDir, desc, visitor);
  }

  /**
   * Iterate over the snapshot store files, restored.edits and logs
   *
   * @param conf The current {@link Configuration} instance.
   * @param fs {@link FileSystem}
   * @param snapshotDir {@link Path} to the Snapshot directory
   * @param desc the {@link SnapshotDescription} of the snapshot to verify
   * @param visitor callback object to get the referenced files
   * @throws IOException if an error occurred while scanning the directory
   */
  public static void visitReferencedFiles(final Configuration conf, final FileSystem fs,
      final Path snapshotDir, final SnapshotDescription desc, final SnapshotVisitor visitor)
      throws IOException {
    visitTableStoreFiles(conf, fs, snapshotDir, desc, visitor);
  }

  /**©
   * Iterate over the snapshot store files
   *
   * @param conf The current {@link Configuration} instance.
   * @param fs {@link FileSystem}
   * @param snapshotDir {@link Path} to the Snapshot directory
   * @param desc the {@link SnapshotDescription} of the snapshot to verify
   * @param visitor callback object to get the store files
   * @throws IOException if an error occurred while scanning the directory
   */
  static void visitTableStoreFiles(final Configuration conf, final FileSystem fs,
      final Path snapshotDir, final SnapshotDescription desc, final StoreFileVisitor visitor)
      throws IOException {
    SnapshotManifest manifest = SnapshotManifest.open(conf, fs, snapshotDir, desc);
    List<SnapshotRegionManifest> regionManifests = manifest.getRegionManifests();
    if (regionManifests == null || regionManifests.size() == 0) {
      LOG.debug("No manifest files present: " + snapshotDir);
      return;
    }

    for (SnapshotRegionManifest regionManifest: regionManifests) {
      visitRegionStoreFiles(regionManifest, visitor);
    }
  }

  /**
   * Iterate over the snapshot store files in the specified region
   *
   * @param manifest snapshot manifest to inspect
   * @param visitor callback object to get the store files
   * @throws IOException if an error occurred while scanning the directory
   */
  static void visitRegionStoreFiles(final SnapshotRegionManifest manifest,
      final StoreFileVisitor visitor) throws IOException {
    HRegionInfo regionInfo = HRegionInfo.convert(manifest.getRegionInfo());
    for (SnapshotRegionManifest.FamilyFiles familyFiles: manifest.getFamilyFilesList()) {
      String familyName = familyFiles.getFamilyName().toStringUtf8();
      for (SnapshotRegionManifest.StoreFile storeFile: familyFiles.getStoreFilesList()) {
        visitor.storeFile(regionInfo, familyName, storeFile);
      }
    }
  }

  /**
   * Verify the validity of the snapshot
   *
   * @param conf The current {@link Configuration} instance.
   * @param fs {@link FileSystem}
   * @param snapshotDir {@link Path} to the Snapshot directory of the snapshot to verify
   * @param snapshotDesc the {@link SnapshotDescription} of the snapshot to verify
   * @throws CorruptedSnapshotException if the snapshot is corrupted
   * @throws IOException if an error occurred while scanning the directory
   */
  public static void verifySnapshot(final Configuration conf, final FileSystem fs,
      final Path snapshotDir, final SnapshotDescription snapshotDesc) throws IOException {
    SnapshotManifest manifest = SnapshotManifest.open(conf, fs, snapshotDir, snapshotDesc);
    verifySnapshot(conf, fs, manifest);
  }

  /**
   * Verify the validity of the snapshot
   *
   * @param conf The current {@link Configuration} instance.
   * @param fs {@link FileSystem}
   * @param manifest snapshot manifest to inspect
   * @throws CorruptedSnapshotException if the snapshot is corrupted
   * @throws IOException if an error occurred while scanning the directory
   */
  public static void verifySnapshot(final Configuration conf, final FileSystem fs,
      final SnapshotManifest manifest) throws IOException {
    final SnapshotDescription snapshotDesc = manifest.getSnapshotDescription();
    final Path snapshotDir = manifest.getSnapshotDir();
    concurrentVisitReferencedFiles(conf, fs, manifest, "VerifySnapshot", new StoreFileVisitor() {
      @Override
      public void storeFile(final HRegionInfo regionInfo, final String family,
          final SnapshotRegionManifest.StoreFile storeFile) throws IOException {
        verifyStoreFile(conf, fs, snapshotDir, snapshotDesc, regionInfo, family, storeFile);
      }
    });
  }

  public static void concurrentVisitReferencedFiles(final Configuration conf, final FileSystem fs,
      final SnapshotManifest manifest, final String desc, final StoreFileVisitor visitor)
      throws IOException {

    final Path snapshotDir = manifest.getSnapshotDir();
    List<SnapshotRegionManifest> regionManifests = manifest.getRegionManifests();
    if (regionManifests == null || regionManifests.size() == 0) {
      LOG.debug("No manifest files present: " + snapshotDir);
      return;
    }

    ExecutorService exec = SnapshotManifest.createExecutor(conf, desc);

    try {
      concurrentVisitReferencedFiles(conf, fs, manifest, exec, visitor);
    } finally {
      exec.shutdown();
    }
  }

  public static void concurrentVisitReferencedFiles(final Configuration conf, final FileSystem fs,
      final SnapshotManifest manifest, final ExecutorService exec, final StoreFileVisitor visitor)
      throws IOException {
    final SnapshotDescription snapshotDesc = manifest.getSnapshotDescription();
    final Path snapshotDir = manifest.getSnapshotDir();

    List<SnapshotRegionManifest> regionManifests = manifest.getRegionManifests();
    if (regionManifests == null || regionManifests.size() == 0) {
      LOG.debug("No manifest files present: " + snapshotDir);
      return;
    }

    final ExecutorCompletionService<Void> completionService =
      new ExecutorCompletionService<Void>(exec);

    for (final SnapshotRegionManifest regionManifest : regionManifests) {
      completionService.submit(new Callable<Void>() {
        @Override public Void call() throws IOException {
          visitRegionStoreFiles(regionManifest, visitor);
          return null;
        }
      });
    }
    try {
      for (int i = 0; i < regionManifests.size(); ++i) {
        completionService.take().get();
      }
    } catch (InterruptedException e) {
      throw new InterruptedIOException(e.getMessage());
    } catch (ExecutionException e) {
      if (e.getCause() instanceof CorruptedSnapshotException) {
        throw new CorruptedSnapshotException(e.getCause().getMessage(),
            ProtobufUtil.createSnapshotDesc(snapshotDesc));
      } else {
        IOException ex = new IOException();
        ex.initCause(e.getCause());
        throw ex;
      }
    }
  }

  /**
   * Verify the validity of the snapshot store file
   *
   * @param conf The current {@link Configuration} instance.
   * @param fs {@link FileSystem}
   * @param snapshotDir {@link Path} to the Snapshot directory of the snapshot to verify
   * @param snapshot the {@link SnapshotDescription} of the snapshot to verify
   * @param regionInfo {@link HRegionInfo} of the region that contains the store file
   * @param family family that contains the store file
   * @param storeFile the store file to verify
   * @throws CorruptedSnapshotException if the snapshot is corrupted
   * @throws IOException if an error occurred while scanning the directory
   */
  private static void verifyStoreFile(final Configuration conf, final FileSystem fs,
      final Path snapshotDir, final SnapshotDescription snapshot, final HRegionInfo regionInfo,
      final String family, final SnapshotRegionManifest.StoreFile storeFile) throws IOException {
    TableName table = TableName.valueOf(snapshot.getTable());
    String fileName = storeFile.getName();

    Path refPath = null;
    if (StoreFileInfo.isReference(fileName)) {
      // If is a reference file check if the parent file is present in the snapshot
      refPath = new Path(new Path(regionInfo.getEncodedName(), family), fileName);
      refPath = StoreFileInfo.getReferredToFile(refPath);
      String refRegion = refPath.getParent().getParent().getName();
      refPath = HFileLink.createPath(table, refRegion, family, refPath.getName());
      if (!HFileLink.buildFromHFileLinkPattern(conf, refPath).exists(fs)) {
        throw new CorruptedSnapshotException(
            "Missing parent hfile for: " + fileName + " path=" + refPath,
            ProtobufUtil.createSnapshotDesc(snapshot));
      }

      if (storeFile.hasReference()) {
        // We don't really need to look for the file on-disk
        // we already have the Reference information embedded here.
        return;
      }
    }

    Path linkPath;
    if (refPath != null && HFileLink.isHFileLink(refPath)) {
      linkPath = new Path(family, refPath.getName());
    } else if (HFileLink.isHFileLink(fileName)) {
      linkPath = new Path(family, fileName);
    } else {
      linkPath = new Path(family, HFileLink.createHFileLinkName(
              table, regionInfo.getEncodedName(), fileName));
    }

    // check if the linked file exists (in the archive, or in the table dir)
    HFileLink link = null;
    if (MobUtils.isMobRegionInfo(regionInfo)) {
      // for mob region
      link = HFileLink.buildFromHFileLinkPattern(MobUtils.getQualifiedMobRootDir(conf),
          HFileArchiveUtil.getArchivePath(conf), linkPath);
    } else {
      // not mob region
      link = HFileLink.buildFromHFileLinkPattern(conf, linkPath);
    }
    try {
      FileStatus fstat = link.getFileStatus(fs);
      if (storeFile.hasFileSize() && storeFile.getFileSize() != fstat.getLen()) {
        String msg = "hfile: " + fileName + " size does not match with the expected one. " +
          " found=" + fstat.getLen() + " expected=" + storeFile.getFileSize();
        LOG.error(msg);
        throw new CorruptedSnapshotException(msg,
          ProtobufUtil.createSnapshotDesc(snapshot));
      }
    } catch (FileNotFoundException e) {
      String msg = "Can't find hfile: " + fileName + " in the real (" +
          link.getOriginPath() + ") or archive (" + link.getArchivePath()
          + ") directory for the primary table.";
      LOG.error(msg);
      throw new CorruptedSnapshotException(msg,
        ProtobufUtil.createSnapshotDesc(snapshot));
    }
  }

  /**
   * Returns the store file names in the snapshot.
   *
   * @param conf The current {@link Configuration} instance.
   * @param fs {@link FileSystem}
   * @param snapshotDir {@link Path} to the Snapshot directory
   * @throws IOException if an error occurred while scanning the directory
   * @return the names of hfiles in the specified snaphot
   */
  public static Set<String> getHFileNames(final Configuration conf, final FileSystem fs,
      final Path snapshotDir) throws IOException {
    SnapshotDescription desc = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);
    return getHFileNames(conf, fs, snapshotDir, desc);
  }

  /**
   * Returns the store file names in the snapshot.
   *
   * @param conf The current {@link Configuration} instance.
   * @param fs {@link FileSystem}
   * @param snapshotDir {@link Path} to the Snapshot directory
   * @param snapshotDesc the {@link SnapshotDescription} of the snapshot to inspect
   * @throws IOException if an error occurred while scanning the directory
   * @return the names of hfiles in the specified snaphot
   */
  private static Set<String> getHFileNames(final Configuration conf, final FileSystem fs,
      final Path snapshotDir, final SnapshotDescription snapshotDesc)
      throws IOException {
    final Set<String> names = new HashSet<String>();
    visitTableStoreFiles(conf, fs, snapshotDir, snapshotDesc, new StoreFileVisitor() {
      @Override
      public void storeFile(final HRegionInfo regionInfo, final String family,
            final SnapshotRegionManifest.StoreFile storeFile) throws IOException {
        String hfile = storeFile.getName();
        if (HFileLink.isHFileLink(hfile)) {
          names.add(HFileLink.getReferencedHFileName(hfile));
        } else {
          names.add(hfile);
        }
      }
    });
    return names;
  }
}
