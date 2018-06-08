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
package org.apache.hadoop.hbase.master.cleaner;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
/**
 * This Chore, every time it runs, will clear the HFiles in the hfile archive
 * folder that are deletable for each HFile cleaner in the chain.
 */
@InterfaceAudience.Private
public class HFileCleaner extends CleanerChore<BaseHFileCleanerDelegate> {

  public static final String MASTER_HFILE_CLEANER_PLUGINS = "hbase.master.hfilecleaner.plugins";

  public HFileCleaner(final int period, final Stoppable stopper, Configuration conf, FileSystem fs,
      Path directory) {
    this(period, stopper, conf, fs, directory, null);
  }

  /**
   * @param period the period of time to sleep between each run
   * @param stopper the stopper
   * @param conf configuration to use
   * @param fs handle to the FS
   * @param directory directory to be cleaned
   * @param params params could be used in subclass of BaseHFileCleanerDelegate
   */
  public HFileCleaner(final int period, final Stoppable stopper, Configuration conf, FileSystem fs,
                      Path directory, Map<String, Object> params) {
    super("HFileCleaner", period, stopper, conf, fs,
      directory, MASTER_HFILE_CLEANER_PLUGINS, params);
  }

  @Override
  protected boolean validate(Path file) {
    if (HFileLink.isBackReferencesDir(file) || HFileLink.isBackReferencesDir(file.getParent())) {
      return true;
    }
    return StoreFileInfo.validateStoreFileName(file.getName());
  }

  /**
   * Exposed for TESTING!
   */
  public List<BaseHFileCleanerDelegate> getDelegatesForTesting() {
    return this.cleanersChain;
  }
}
