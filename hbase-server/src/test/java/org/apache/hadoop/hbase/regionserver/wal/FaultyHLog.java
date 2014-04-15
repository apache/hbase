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

package org.apache.hadoop.hbase.regionserver.wal;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;

/*
 * This is a utility class, used by tests, which fails operation specified by FailureType enum
 */
public class FaultyHLog extends FSHLog {
  public enum FailureType {
    NONE, APPENDNOSYNC, SYNC
  }
  FailureType ft = FailureType.NONE;

  public FaultyHLog(FileSystem fs, Path rootDir, String logName, Configuration conf)
      throws IOException {
    super(fs, rootDir, logName, conf);
  }
  
  public void setFailureType(FailureType fType) {
    this.ft = fType;
  }
  
  @Override
  public void sync(long txid) throws IOException {
    if (this.ft == FailureType.SYNC) {
      throw new IOException("sync");
    }
    super.sync(txid);
  }
  @Override
  public long appendNoSync(HRegionInfo info, TableName tableName, WALEdit edits,
      List<UUID> clusterIds, final long now, HTableDescriptor htd, AtomicLong sequenceId,
      boolean isInMemstore, long nonceGroup, long nonce) throws IOException {
    if (this.ft == FailureType.APPENDNOSYNC) {
      throw new IOException("appendNoSync");
    }
    return super.appendNoSync(info, tableName, edits, clusterIds, now, htd, sequenceId,
      isInMemstore, nonceGroup, nonce);
  }
}

