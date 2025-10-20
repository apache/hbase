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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.regionserver.CreateStoreFileWriterParams;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * An interface to define how we track the store files for a give store.
 * <p/>
 * In the old time, we will write store to a tmp directory first, and then rename it to the actual
 * data file. And once a store file is under data directory, we will consider it as 'committed'. And
 * we need to do listing when loading store files.
 * <p/>
 * When cloud age is coming, now we want to store the store files on object storage, where rename
 * and list are not as cheap as on HDFS, especially rename. Although introducing a metadata
 * management layer for object storage could solve the problem, but we still want HBase to run on
 * pure object storage, so here we introduce this interface to abstract how we track the store
 * files. For the old implementation, we just persist nothing here, and do listing to load store
 * files. When running on object storage, we could persist the store file list in a system region,
 * or in a file on the object storage, to make it possible to write directly into the data directory
 * to avoid renaming, and also avoid listing when loading store files.
 * <p/>
 * The implementation requires to be thread safe as flush and compaction may occur as the same time,
 * and we could also do multiple compactions at the same time. As the implementation may choose to
 * persist the store file list to external storage, which could be slow, it is the duty for the
 * callers to not call it inside a lock which may block normal read/write requests.
 */
@InterfaceAudience.Private
public interface StoreFileTracker {
  /**
   * Load the store files list when opening a region.
   */
  List<StoreFileInfo> load() throws IOException;

  /**
   * Add new store files.
   * <p/>
   * Used for flush and bulk load.
   */
  void add(Collection<StoreFileInfo> newFiles) throws IOException;

  /**
   * Add new store files and remove compacted store files after compaction.
   */
  void replace(Collection<StoreFileInfo> compactedFiles, Collection<StoreFileInfo> newFiles)
    throws IOException;

  /**
   * Set the store files.
   */
  void set(List<StoreFileInfo> files) throws IOException;

  /**
   * Create a writer for writing new store files.
   * @return Writer for a new StoreFile
   */
  StoreFileWriter createWriter(CreateStoreFileWriterParams params) throws IOException;

  /**
   * Adds StoreFileTracker implementations specific configurations into the table descriptor.
   * <p/>
   * This is used to avoid accidentally data loss when changing the cluster level store file tracker
   * implementation, and also possible misconfiguration between master and region servers.
   * <p/>
   * See HBASE-26246 for more details.
   * @param builder The table descriptor builder for the given table.
   */
  TableDescriptorBuilder updateWithTrackerConfigs(TableDescriptorBuilder builder);

  /**
   * Whether the implementation of this tracker requires you to write to temp directory first, i.e,
   * does not allow broken store files under the actual data directory.
   */
  boolean requireWritingToTmpDirFirst();

  Reference createReference(Reference reference, Path path) throws IOException;

  /**
   * Reads the reference file from the given path.
   * @param path the {@link Path} to the reference file in the file system.
   * @return a {@link Reference} that points at top/bottom half of a an hfile
   */
  Reference readReference(Path path) throws IOException;

  /**
   * Returns true if the specified family has reference files
   * @return true if family contains reference files
   */
  boolean hasReferences() throws IOException;

  StoreFileInfo getStoreFileInfo(final FileStatus fileStatus, final Path initialPath,
    final boolean primaryReplica) throws IOException;

  StoreFileInfo getStoreFileInfo(final Path initialPath, final boolean primaryReplica)
    throws IOException;

}
