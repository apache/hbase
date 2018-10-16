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
package org.apache.hadoop.hbase.mob;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.DefaultStoreEngine;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * MobStoreEngine creates the mob specific compactor, and store flusher.
 */
@InterfaceAudience.Private
public class MobStoreEngine extends DefaultStoreEngine {

  @Override
  protected void createStoreFlusher(Configuration conf, HStore store) throws IOException {
    // When using MOB, we use DefaultMobStoreFlusher always
    // Just use the compactor and compaction policy as that in DefaultStoreEngine. We can have MOB
    // specific compactor and policy when that is implemented.
    storeFlusher = new DefaultMobStoreFlusher(conf, store);
  }

  /**
   * Creates the DefaultMobCompactor.
   */
  @Override
  protected void createCompactor(Configuration conf, HStore store) throws IOException {
    compactor = new DefaultMobStoreCompactor(conf, store);
  }
}
