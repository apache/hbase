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
package org.apache.hadoop.hbase.wal;

import java.io.Closeable;
import java.io.IOException;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A WAL reader which is designed for be able to tailing the WAL file which is currently being
 * written. It adds support
 */
@InterfaceAudience.Private
public interface WALTailingReader extends Closeable {

  enum State {
    /** This means we read an Entry without any error */
    NORMAL,
    /**
     * This means the WAL file has a trailer and we have reached it, which means we have finished
     * reading this file normally
     */
    EOF_WITH_TRAILER,
    /**
     * This means we meet an error so the upper layer need to reset to read again
     */
    ERROR_AND_RESET,
    /**
     * Mostly the same with the above {@link #ERROR_AND_RESET}, the difference is that here we also
     * mess up the compression dictionary when reading data, so the upper layer should also clear
     * the compression context when reseting, which means when calling resetTo method, we need to
     * skip to the position instead of just seek to, which will impact performance.
     */
    ERROR_AND_RESET_COMPRESSION,
    /**
     * This means we reach the EOF and the upper layer need to reset to see if there is more data.
     * Notice that this does not mean that there is necessarily more data, the upper layer should
     * determine whether they need to reset and read again.
     */
    EOF_AND_RESET,
    /**
     * Mostly the same with the above {@link #EOF_AND_RESET}, the difference is that here we also
     * mess up the compression dictionary when reading data, so the upper layer should also clear
     * the compression context when reseting, which means when calling resetTo method, we need to
     * skip to the position instead of just seek to, which will impact performance. The
     * implementation should try its best to not fall into this situation.
     */
    EOF_AND_RESET_COMPRESSION;

    /**
     * A dummy result for returning, as except {@link NORMAL}, for other state we do not need to
     * provide fields other than state in the returned {@link Result}.
     */
    private Result result = new Result(this, null, -1);

    public Result getResult() {
      return result;
    }

    public boolean resetCompression() {
      return this == ERROR_AND_RESET_COMPRESSION || this == EOF_AND_RESET_COMPRESSION;
    }

    public boolean eof() {
      return this == EOF_AND_RESET || this == EOF_AND_RESET_COMPRESSION || this == EOF_WITH_TRAILER;
    }
  }

  final class Result {

    private final State state;
    private final Entry entry;
    private final long entryEndPos;

    public Result(State state, Entry entry, long entryEndPos) {
      this.state = state;
      this.entry = entry;
      this.entryEndPos = entryEndPos;
    }

    public State getState() {
      return state;
    }

    public Entry getEntry() {
      return entry;
    }

    public long getEntryEndPos() {
      return entryEndPos;
    }
  }

  /**
   * Read the next entry and make sure the position after reading does not go beyond the given
   * {@code limit}.
   * <p/>
   * Notice that we will not throw any checked exception out, all the states are represented by the
   * return value. Of course we will log the exceptions out. The reason why we do this is that, for
   * tailing a WAL file which is currently being written, we will hit EOFException many times, so it
   * should not be considered as an 'exception' and also, creating an Exception is a bit expensive.
   * @param limit the position limit. See HBASE-14004 for more details about why we need this
   *              limitation. -1 means no limit.
   */
  Result next(long limit);

  /**
   * Get the current reading position.
   */
  long getPosition() throws IOException;

  /**
   * Reopen the reader to see if there is new data arrives, and also seek(or skip) to the given
   * position.
   * <p/>
   * If you want to read from the beginning instead of a given position, please pass -1 as
   * {@code position}, then the reader will locate to the first entry. Notice that, since we have a
   * magic header and a pb header, the first WAL entry is not located at position 0, so passing 0
   * will cause trouble.
   * @param position         the position we want to start reading from after resetting, or -1 if
   *                         you want to start reading from the beginning.
   * @param resetCompression whether we also need to clear the compression context. If {@code true},
   *                         we will use skip instead of seek after resetting.
   */
  void resetTo(long position, boolean resetCompression) throws IOException;

  /**
   * Override to remove the 'throws IOException' as we are just a reader.
   */
  @Override
  void close();
}
