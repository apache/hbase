/*
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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.Comparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionConfiguration;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;

/**
 * Default implementation of StoreFileManager. Not thread-safe.
 */
@InterfaceAudience.Private
class DefaultStoreFileManager extends AbstractStoreFileManager {

  public DefaultStoreFileManager(CellComparator cellComparator,
      Comparator<HStoreFile> storeFileComparator, Configuration conf,
      CompactionConfiguration comConf, HRegionFileSystem regionFs,
      String familyName) {
    super(cellComparator, storeFileComparator, conf, comConf, regionFs, familyName);
  }

  @Override
  protected void loadFilesHook(ImmutableList<HStoreFile> storefiles) throws IOException {
    // no-op
  }

  @Override
  protected void insertNewFilesHook(ImmutableList<HStoreFile> storefiles) throws IOException {
    // no-op
  }

  @Override
  protected void addCompactionResultsHook(ImmutableList<HStoreFile> storefiles)
    throws IOException {
    // no-op
  }
}

