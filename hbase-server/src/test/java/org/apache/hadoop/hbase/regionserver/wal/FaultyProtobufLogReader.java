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
package org.apache.hadoop.hbase.regionserver.wal;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import org.apache.hadoop.hbase.wal.WAL.Entry;

public class FaultyProtobufLogReader extends ProtobufLogReader {

  // public until class relocates to o.a.h.h.wal
  public enum FailureType {
    BEGINNING,
    MIDDLE,
    END,
    NONE
  }

  Queue<Entry> nextQueue = new LinkedList<>();
  int numberOfFileEntries = 0;

  FailureType getFailureType() {
    return FailureType.valueOf(conf.get("faultyprotobuflogreader.failuretype", "NONE"));
  }

  @Override
  public Entry next(Entry reuse) throws IOException {
    if (nextQueue.isEmpty()) { // Read the whole thing at once and fake reading
      boolean b;
      do {
        Entry e = new Entry();
        b = readNext(e);
        nextQueue.offer(e);
        numberOfFileEntries++;
      } while (b);
    }

    if (nextQueue.size() == this.numberOfFileEntries && getFailureType() == FailureType.BEGINNING) {
      throw new IOException("fake Exception");
    } else if (
      nextQueue.size() == this.numberOfFileEntries / 2 && getFailureType() == FailureType.MIDDLE
    ) {
      throw new IOException("fake Exception");
    } else if (nextQueue.size() == 1 && getFailureType() == FailureType.END) {
      throw new IOException("fake Exception");
    }

    if (nextQueue.peek() != null) {
      edit++;
    }

    Entry e = nextQueue.poll();

    if (e.getEdit().isEmpty()) {
      return null;
    }
    return e;
  }
}
