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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;

import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A implementation of DefaultStoreFlushContext that assumes committed store files were written
 * directly in the store dir, and therefore, doesn't perform a rename from tmp dir
 * into the store dir.
 *
 * To be used only when PersistedStoreEngine is configured as the StoreEngine implementation.
 */
@InterfaceAudience.Private
public class PersistedStoreFlushContext extends DefaultStoreFlushContext {

  private static final Logger LOG = LoggerFactory.getLogger(PersistedStoreFlushContext.class);

  public PersistedStoreFlushContext(HStore store, Long cacheFlushSeqNum,
    FlushLifeCycleTracker tracker) {
    super.init(store, cacheFlushSeqNum, tracker);
  }


  @Override
  public boolean commit(MonitoredTask status) throws IOException {
    return super.commit(p -> {
      status.setStatus("Flushing " + this.store + ": reopening file created directly in family dir");
      HStoreFile sf = store.createStoreFileAndReader(p);

      StoreFileReader r = sf.getReader();
      this.store.storeSize.addAndGet(r.length());
      this.store.totalUncompressedBytes.addAndGet(r.getTotalUncompressedBytes());

      if (LOG.isInfoEnabled()) {
        LOG.info("Added " + sf + ", entries=" + r.getEntries() +
          ", sequenceid=" + cacheFlushSeqNum +
          ", filesize=" +
          StringUtils.TraditionalBinaryPrefix.long2String(r.length(), "", 1));
      }
      return sf;
    });
  }

}
