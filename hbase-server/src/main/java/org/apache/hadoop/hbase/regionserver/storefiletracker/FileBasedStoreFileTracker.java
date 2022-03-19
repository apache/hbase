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
package org.apache.hadoop.hbase.regionserver.storefiletracker;

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
import org.apache.hadoop.hbase.regionserver.StoreContext;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.apache.yetus.audience.InterfaceAudience;

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

  public FileBasedStoreFileTracker(Configuration conf, boolean isPrimaryReplica, StoreContext ctx) {
    super(conf, isPrimaryReplica, ctx);
    //CreateTableProcedure needs to instantiate the configured SFT impl, in order to update table
    //descriptors with the SFT impl specific configs. By the time this happens, the table has no
    //regions nor stores yet, so it can't create a proper StoreContext.
    if (ctx != null) {
      backedFile = new StoreFileListFile(ctx);
    } else {
      backedFile = null;
    }
  }

  @Override
  protected List<StoreFileInfo> doLoadStoreFiles(boolean readOnly) throws IOException {
    StoreFileList list = backedFile.load(readOnly);
    if (list == null) {
      return Collections.emptyList();
    }
    FileSystem fs = ctx.getRegionFileSystem().getFileSystem();
    List<StoreFileInfo> infos = new ArrayList<>();
    for (StoreFileEntry entry : list.getStoreFileList()) {
      infos.add(ServerRegionReplicaUtil.getStoreFileInfo(conf, fs, ctx.getRegionInfo(),
        ctx.getRegionFileSystem().getRegionInfoForFS(), ctx.getFamily().getNameAsString(),
        new Path(ctx.getFamilyStoreDirectoryPath(), entry.getName())));
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
    return StoreFileEntry.newBuilder().setName(info.getPath().getName()).setSize(info.getSize())
      .build();
  }

  @Override
  protected void doAddNewStoreFiles(Collection<StoreFileInfo> newFiles) throws IOException {
    synchronized (storefiles) {
      StoreFileList.Builder builder = StoreFileList.newBuilder();
      for (StoreFileInfo info : storefiles.values()) {
        builder.addStoreFile(toStoreFileEntry(info));
      }
      for (StoreFileInfo info : newFiles) {
        builder.addStoreFile(toStoreFileEntry(info));
      }
      backedFile.update(builder);
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
    }
  }
}
