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
package org.apache.hadoop.hbase.master.cleaner;

import java.util.Map;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * General interface for cleaning files from a folder (generally an archive or backup folder). These
 * are chained via the {@link CleanerChore} to determine if a given file should be deleted.
 */
@InterfaceAudience.Private
public interface FileCleanerDelegate extends Configurable, Stoppable {

  /**
   * Determines which of the given files are safe to delete
   * @param files files to check for deletion
   * @return files that are ok to delete according to this cleaner
   */
  Iterable<FileStatus> getDeletableFiles(Iterable<FileStatus> files);

  /**
   * this method is used to pass some instance into subclass
   */
  void init(Map<String, Object> params);

  /**
   * Used to do some initialize work before every period clean
   */
  default void preClean() {
  }

  /**
   * Check if a empty directory with no subdirs or subfiles can be deleted
   * @param dir Path of the directory
   * @return True if the directory can be deleted, otherwise false
   */
  default boolean isEmptyDirDeletable(Path dir) {
    return true;
  }
}
