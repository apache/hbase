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
package org.apache.hadoop.hbase.regionserver.wal;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.wal.WALProvider.Writer;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

@Category({ RegionServerServices.class, MediumTests.class })
public class TestFSHLogDurability extends WALDurabilityTestBase<CustomFSHLog> {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestFSHLogDurability.class);

  @Override
  protected CustomFSHLog getWAL(FileSystem fs, Path root, String logDir, Configuration conf)
    throws IOException {
    CustomFSHLog wal = new CustomFSHLog(fs, root, logDir, conf);
    wal.init();
    return wal;
  }

  @Override
  protected void resetSyncFlag(CustomFSHLog wal) {
    wal.resetSyncFlag();
  }

  @Override
  protected Boolean getSyncFlag(CustomFSHLog wal) {
    return wal.getSyncFlag();
  }

  @Override
  protected Boolean getWriterSyncFlag(CustomFSHLog wal) {
    return wal.getWriterSyncFlag();
  }
}

class CustomFSHLog extends FSHLog {
  private Boolean syncFlag;

  private Boolean writerSyncFlag;

  public CustomFSHLog(FileSystem fs, Path root, String logDir, Configuration conf)
    throws IOException {
    super(fs, root, logDir, conf);
  }

  @Override
  protected Writer createWriterInstance(Path path) throws IOException {
    Writer writer = super.createWriterInstance(path);
    return new Writer() {

      @Override
      public void close() throws IOException {
        writer.close();
      }

      @Override
      public long getLength() {
        return writer.getLength();
      }

      @Override
      public long getSyncedLength() {
        return writer.getSyncedLength();
      }

      @Override
      public void sync(boolean forceSync) throws IOException {
        writerSyncFlag = forceSync;
        writer.sync(forceSync);
      }

      @Override
      public void append(Entry entry) throws IOException {
        writer.append(entry);
      }
    };
  }

  @Override
  public void sync(boolean forceSync) throws IOException {
    syncFlag = forceSync;
    super.sync(forceSync);
  }

  @Override
  public void sync(long txid, boolean forceSync) throws IOException {
    syncFlag = forceSync;
    super.sync(txid, forceSync);
  }

  void resetSyncFlag() {
    this.syncFlag = null;
    this.writerSyncFlag = null;
  }

  Boolean getSyncFlag() {
    return syncFlag;
  }

  Boolean getWriterSyncFlag() {
    return writerSyncFlag;
  }
}