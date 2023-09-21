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
package org.apache.hadoop.hbase.io.hfile.bucket;

import java.io.IOException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class BucketCachePersister extends Thread {
  private final BucketCache cache;
  private final long intervalMillis;
  private static final Logger LOG = LoggerFactory.getLogger(BucketCachePersister.class);

  public BucketCachePersister(BucketCache cache, long intervalMillis) {
    super("bucket-cache-persister");
    this.cache = cache;
    this.intervalMillis = intervalMillis;
    LOG.info("BucketCachePersister started with interval: " + intervalMillis);
  }

  public void run() {
    try {
      while (true) {
        try {
          Thread.sleep(intervalMillis);
          if (cache.isCacheInconsistent()) {
            LOG.debug("Cache is inconsistent, persisting to disk");
            cache.persistToFile();
            cache.setCacheInconsistent(false);
          }
        } catch (IOException e) {
          LOG.warn("Exception in BucketCachePersister.", e);
        }
      }
    } catch (InterruptedException e) {
      LOG.warn("Interrupting BucketCachePersister thread.", e);
    }
  }
}
