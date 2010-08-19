/**
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.hadoop.hbase.master.handler;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaEditor;
import org.apache.hadoop.hbase.master.MasterFileSystem;

public class ModifyTableHandler extends TableEventHandler {
  public ModifyTableHandler(final byte [] tableName, final Server server,
      final CatalogTracker catalogTracker, final MasterFileSystem fileManager) {
    super(EventType.C2M_MODIFY_TABLE, tableName, server, catalogTracker,
        fileManager);
  }

  @Override
  protected void handleTableOperation(List<HRegionInfo> regions)
  throws IOException {
    for (HRegionInfo region : regions) {
      // Update region info in META
      MetaEditor.updateRegionInfo(catalogTracker, region);
      // Update region info in FS
      this.fileManager.updateRegionInfo(region);
    }
  }
}
