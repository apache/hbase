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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.StoreContext;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.yetus.audience.InterfaceAudience;

import java.io.IOException;
import java.util.List;

/**
 * Extends MigrationStoreFileTracker for Snapshot restore/clone specific case.
 * When restoring/cloning snapshots, new regions are created with reference files to the
 * original regions files. This work is done in snapshot specific classes. We need to somehow
 * initialize these reference files in the configured StoreFileTracker. Once snapshot logic has
 * cloned the store dir and created the references, it should set the list of reference files in
 * <code>SourceTracker.setReferenceFiles</code> then invoke <code>load</code> method.
 * <p/>
 */
@InterfaceAudience.Private
public class SnapshotStoreFileTracker extends MigrationStoreFileTracker {

  protected SnapshotStoreFileTracker(Configuration conf, boolean isPrimaryReplica,
    StoreContext ctx) {
    super(conf, isPrimaryReplica, ctx);
    Preconditions.checkArgument(src instanceof SourceTracker,
      "src for SnapshotStoreFileTracker should always be a SourceTracker!");
  }

  public SourceTracker getSourceTracker(){
    return (SourceTracker)this.src;
  }

  /**
   * The SFT impl to be set as source for SnapshotStoreFileTracker.
   */
  public static class SourceTracker extends DefaultStoreFileTracker {

    private List<StoreFileInfo> files;

    public SourceTracker(Configuration conf, boolean isPrimaryReplica, StoreContext ctx) {
      super(conf, isPrimaryReplica, ctx);
    }

    public void setReferenceFiles(List<StoreFileInfo> files) {
      this.files = files;
    }
    /**
     * Overrides <code>MigrationStoreFileTracker</code> to simply call <code>set</code> on destination
     * SFT implementation, passing the list of reference files.
     * @return
     * @throws IOException
     */
    @Override
    public List<StoreFileInfo> load() throws IOException {
      return files;
    }
  }
}
