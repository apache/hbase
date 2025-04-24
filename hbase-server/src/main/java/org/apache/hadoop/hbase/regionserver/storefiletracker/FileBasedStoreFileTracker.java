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
package org.apache.hadoop.hbase.regionserver.storefiletracker;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.regionserver.StoreContext;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.shaded.protobuf.generated.FSProtos.Reference.Range;
import org.apache.hadoop.hbase.shaded.protobuf.generated.StoreFileTrackerProtos.StoreFileEntry;
import org.apache.hadoop.hbase.shaded.protobuf.generated.StoreFileTrackerProtos.StoreFileList;

/**
 * A file based store file tracker.
 * <p/>
 * For this tracking way, the store file list will be persistent into a file, so we can write the
 * new store files directly to the final data directory, as we will not load the broken files. This
 * will greatly reduce the time for flush and compaction on some object storages as a rename is
 * actual a copy on them. And it also avoid listing when loading store file list, which could also
 * speed up the loading of store files as listing is also not a fast operation on most object
 * storages.
 */
@InterfaceAudience.Private
class FileBasedStoreFileTracker extends StoreFileTrackerBase {

  private final StoreFileListFile backedFile;

  private final Map<String, StoreFileInfo> storefiles = new HashMap<>();
  private static final Logger LOG = LoggerFactory.getLogger(FileBasedStoreFileTracker.class);

  public FileBasedStoreFileTracker(Configuration conf, boolean isPrimaryReplica, StoreContext ctx) {
    super(conf, isPrimaryReplica, ctx);
    // CreateTableProcedure needs to instantiate the configured SFT impl, in order to update table
    // descriptors with the SFT impl specific configs. By the time this happens, the table has no
    // regions nor stores yet, so it can't create a proper StoreContext.
    if (ctx != null) {
      backedFile = new StoreFileListFile(ctx);
    } else {
      backedFile = null;
    }
  }

  @Override
  protected List<StoreFileInfo> doLoadStoreFiles(boolean readOnly) throws IOException {
    StoreFileList list = backedFile.load(readOnly);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Loaded file list backed file, containing " + list.getStoreFileList().size()
        + " store file entries");
    }
    if (list == null) {
      return Collections.emptyList();
    }
    FileSystem fs = ctx.getRegionFileSystem().getFileSystem();
    List<StoreFileInfo> infos = new ArrayList<>();
    for (StoreFileEntry entry : list.getStoreFileList()) {
      infos.add(ServerRegionReplicaUtil.getStoreFileInfo(conf, fs, ctx.getRegionInfo(),
        ctx.getRegionFileSystem().getRegionInfoForFS(), ctx.getFamily().getNameAsString(),
        new Path(ctx.getFamilyStoreDirectoryPath(), entry.getName()), this));
    }
    // In general, for primary replica, the load method should only be called once when
    // initialization, so we do not need synchronized here. And for secondary replicas, though the
    // load method could be called multiple times, we will never call other methods so no
    // synchronized is also fine.
    // But we have a refreshStoreFiles method in the Region interface, which can be called by CPs,
    // and we have a RefreshHFilesEndpoint example to expose the refreshStoreFiles method as RPC, so
    // for safety, let's still keep the synchronized here.
    synchronized (storefiles) {
      for (StoreFileInfo info : infos) {
        storefiles.put(info.getPath().getName(), info);
      }
    }
    return infos;
  }

  @Override
  public boolean requireWritingToTmpDirFirst() {
    return false;
  }

  private StoreFileEntry toStoreFileEntry(StoreFileInfo info) {
    org.apache.hadoop.hbase.shaded.protobuf.generated.StoreFileTrackerProtos.StoreFileEntry.Builder entryBuilder =
      StoreFileEntry.newBuilder().setName(info.getPath().getName()).setSize(info.getSize());
    if (info.isReference()) {
      // TODO: Need a better way to fix the Protobuf generate enum Range to Reference.Range,
      // otherwise it would result in DATA LOSS
      org.apache.hadoop.hbase.shaded.protobuf.generated.FSProtos.Reference reference =
        org.apache.hadoop.hbase.shaded.protobuf.generated.FSProtos.Reference.newBuilder()
          .setSplitkey(ByteString.copyFrom(info.getReference().getSplitKey()))
          .setRange(Range.forNumber(info.getReference().getFileRegion().ordinal())).build();
      entryBuilder.setReference(reference);
    }
    return entryBuilder.build();
  }

  @Override
  protected void doAddNewStoreFiles(Collection<StoreFileInfo> newFiles) throws IOException {
    synchronized (storefiles) {
      StoreFileList.Builder builder = StoreFileList.newBuilder();
      for (StoreFileInfo info : storefiles.values()) {
        builder.addStoreFile(toStoreFileEntry(info));
      }
      for (StoreFileInfo info : newFiles) {
        if (!storefiles.containsKey(info.getPath().getName()))
          builder.addStoreFile(toStoreFileEntry(info));
      }
      backedFile.update(builder);
      if (LOG.isTraceEnabled()) {
        LOG.trace(newFiles.size() + " store files added to store file list file: " + newFiles);
      }
      for (StoreFileInfo info : newFiles) {
        storefiles.put(info.getPath().getName(), info);
      }
    }
  }

  @Override
  protected void doAddCompactionResults(Collection<StoreFileInfo> compactedFiles,
    Collection<StoreFileInfo> newFiles) throws IOException {
    Set<String> compactedFileNames =
      compactedFiles.stream().map(info -> info.getPath().getName()).collect(Collectors.toSet());
    synchronized (storefiles) {
      StoreFileList.Builder builder = StoreFileList.newBuilder();
      storefiles.forEach((name, info) -> {
        if (compactedFileNames.contains(name)) {
          return;
        }
        builder.addStoreFile(toStoreFileEntry(info));
      });
      for (StoreFileInfo info : newFiles) {
        builder.addStoreFile(toStoreFileEntry(info));
      }
      backedFile.update(builder);
      if (LOG.isTraceEnabled()) {
        LOG.trace(
          "replace compacted files: " + compactedFileNames + " with new store files: " + newFiles);
      }
      for (String name : compactedFileNames) {
        storefiles.remove(name);
      }
      for (StoreFileInfo info : newFiles) {
        storefiles.put(info.getPath().getName(), info);
      }
    }
  }

  @Override
  protected void doSetStoreFiles(Collection<StoreFileInfo> files) throws IOException {
    synchronized (storefiles) {
      storefiles.clear();
      StoreFileList.Builder builder = StoreFileList.newBuilder();
      for (StoreFileInfo info : files) {
        storefiles.put(info.getPath().getName(), info);
        builder.addStoreFile(toStoreFileEntry(info));
      }
      backedFile.update(builder);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Set store files in store file list file: " + files);
      }
    }
  }

  @Override
  public Reference readReference(Path p) throws IOException {
    String fileName = p.getName();
    StoreFileList list = backedFile.load(true);
    for (StoreFileEntry entry : list.getStoreFileList()) {
      if (entry.getName().equals(fileName)) {
        return Reference.convert(entry.getReference());
      }
    }
    throw new FileNotFoundException("Reference does not exist for path : " + p);
  }

  @Override
  public boolean hasReferences() throws IOException {
    StoreFileList list = backedFile.load(true);
    for (StoreFileEntry entry : list.getStoreFileList()) {
      if (entry.hasReference() || HFileLink.isHFileLink(entry.getName())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public HFileLink createHFileLink(TableName linkedTable, String linkedRegion, String hfileName,
    boolean createBackRef) throws IOException {
    // String name = HFileLink.createHFileLinkName(linkedTable, linkedRegion, hfileName);
    FileSystem fs = ctx.getRegionFileSystem().getFileSystem();
    HFileLink hfileLink = HFileLink.build(conf, linkedTable, linkedRegion,
      ctx.getFamily().getNameAsString(), hfileName);
    StoreFileInfo storeFileInfo =
      new StoreFileInfo(conf, fs, new Path(ctx.getFamilyStoreDirectoryPath(),
        HFileLink.createHFileLinkName(linkedTable, linkedRegion, hfileName)), hfileLink);
    // Path backRefPath = null;
    if (createBackRef) {
      // TODO: this should be done as part of commit
      Path archiveStoreDir = HFileArchiveUtil.getStoreArchivePath(conf, linkedTable, linkedRegion,
        ctx.getFamily().getNameAsString());
      Path backRefssDir = HFileLink.getBackReferencesDir(archiveStoreDir, hfileName);
      fs.mkdirs(backRefssDir);

      // Create the reference for the link
      String refName = HFileLink.createBackReferenceName(ctx.getTableName().toString(),
        ctx.getRegionInfo().getEncodedName());
      Path backRefPath = new Path(backRefssDir, refName);
      fs.createNewFile(backRefPath);
    }
    try {
      // TODO do not add to SFT as of now
      add(Collections.singletonList(storeFileInfo));
    } catch (Exception e) {
      // LOG.error("couldn't create the link=" + name + " for " + ctx.getFamilyStoreDirectoryPath(),
      // e);
      // // Revert the reference if the link creation failed
      // if (createBackRef) {
      // fs.delete(backRefPath, false);
      // }
    }
    return hfileLink;
  }

  @Override
  public Reference createReference(Reference reference, Path path) throws IOException {
    // NOOP
    return reference;
  }

  @Override
  public Reference createAndCommitReference(Reference reference, Path path) throws IOException {
    StoreFileInfo storeFileInfo =
      new StoreFileInfo(ctx.getRegionFileSystem().getFileSystem().getConf(),
        ctx.getRegionFileSystem().getFileSystem(), path, reference);
    add(Collections.singleton(storeFileInfo));
    return reference;
  }
}
