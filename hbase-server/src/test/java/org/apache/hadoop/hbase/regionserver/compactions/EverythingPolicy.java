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
package org.apache.hadoop.hbase.regionserver.compactions;


import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;

/**
 * Test Policy to compact everything every time.
 */
public class EverythingPolicy extends RatioBasedCompactionPolicy {
  /**
   * Constructor.
   *
   * @param conf            The Conf.
   * @param storeConfigInfo Info about the store.
   */
  public EverythingPolicy(Configuration conf, StoreConfigInformation storeConfigInfo) {
    super(conf, storeConfigInfo);
  }

  @Override
  protected final ArrayList<HStoreFile> applyCompactionPolicy(ArrayList<HStoreFile> candidates,
      boolean mayUseOffPeak, boolean mayBeStuck) throws IOException {
    if (candidates.size() < comConf.getMinFilesToCompact()) {
      return new ArrayList<>(0);
    }
    return new ArrayList<>(candidates);
  }
}
