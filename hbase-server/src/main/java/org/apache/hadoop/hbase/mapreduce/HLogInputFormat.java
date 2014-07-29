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
package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Simple {@link InputFormat} for {@link WAL} files.
 * @deprecated use {@link WALInputFormat}
 */
@Deprecated
@InterfaceAudience.Public
public class HLogInputFormat extends InputFormat<HLogKey, WALEdit> {
  private static final Log LOG = LogFactory.getLog(HLogInputFormat.class);
  public static final String START_TIME_KEY = "hlog.start.time";
  public static final String END_TIME_KEY = "hlog.end.time";

  // Delegate to WALInputFormat for implementation.
  private final WALInputFormat delegate = new WALInputFormat();

  /**
   * {@link RecordReader} that pulls out the legacy HLogKey format directly.
   */
  static class HLogKeyRecordReader extends WALInputFormat.WALRecordReader<HLogKey> {
    @Override
    public HLogKey getCurrentKey() throws IOException, InterruptedException {
      if (!(currentEntry.getKey() instanceof HLogKey)) {
        final IllegalStateException exception = new IllegalStateException(
            "HLogInputFormat only works when given entries that have HLogKey for keys. This" +
            " one had '" + currentEntry.getKey().getClass() + "'");
        LOG.error("The deprecated HLogInputFormat has to work with the deprecated HLogKey class, " +
            " but HBase internals read the wal entry using some other class." +
            " This is a bug; please file an issue or email the developer mailing list. It is " +
            "likely that you would not have this problem if you updated to use WALInputFormat. " +
            "You will need the following exception details when seeking help from the HBase " +
            "community.",
            exception);
        throw exception;
      }
      return (HLogKey)currentEntry.getKey();
    }
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException,
      InterruptedException {
    return delegate.getSplits(context, START_TIME_KEY, END_TIME_KEY);
  }

  @Override
  public RecordReader<HLogKey, WALEdit> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {
    return new HLogKeyRecordReader();
  }
}
