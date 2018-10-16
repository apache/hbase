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

package org.apache.hadoop.hbase.wal;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.regionserver.wal.FSHLog;
import org.apache.yetus.audience.InterfaceAudience;

// imports for things that haven't moved yet

/**
 * This is a utility class, used by tests, which fails operation specified by FailureType enum
 */
@InterfaceAudience.Private
public class FaultyFSLog extends FSHLog {
  public enum FailureType {
    NONE, APPEND, SYNC
  }
  FailureType ft = FailureType.NONE;

  public FaultyFSLog(FileSystem fs, Path rootDir, String logName, Configuration conf)
      throws IOException {
    super(fs, rootDir, logName, conf);
  }

  public void setFailureType(FailureType fType) {
    this.ft = fType;
  }

  @Override
  public void sync(long txid) throws IOException {
    sync(txid, false);
  }

  @Override
  public void sync(long txid, boolean forceSync) throws IOException {
    if (this.ft == FailureType.SYNC) {
      throw new IOException("sync");
    }
    super.sync(txid, forceSync);
  }

  @Override
  public long append(RegionInfo info, WALKeyImpl key,
      WALEdit edits, boolean inMemstore) throws IOException {
    if (this.ft == FailureType.APPEND) {
      throw new IOException("append");
    }
    return super.append(info, key, edits, inMemstore);
  }
}

