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
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.apache.hadoop.hbase.regionserver.StoreFile;

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
  public EverythingPolicy(final Configuration conf,
                          final StoreConfigInformation storeConfigInfo) {
    super(conf, storeConfigInfo);
  }

  @Override
  final ArrayList<StoreFile> applyCompactionPolicy(final ArrayList<StoreFile> candidates,
    final boolean mayUseOffPeak, final boolean mayBeStuck) throws IOException {

    if (candidates.size() < comConf.getMinFilesToCompact()) {
      return new ArrayList<StoreFile>(0);
    }

    return new ArrayList<StoreFile>(candidates);
  }
}
