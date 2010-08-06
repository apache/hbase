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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.util.Progressable;

public interface RegionServerController extends Server {

  // this is unfortunate but otherwise all the implementation of region
  // open/close must happen in HRS itself and not in handlers
  // handlers could make calls to HRS methods and logic still in HRS, just
  // trying to reduce the already massive class

  public HRegion getOnlineRegion(String regionName);

  public void addToOnlineRegions(HRegion region);

  public HRegion instantiateRegion(final HRegionInfo regionInfo,
      final HLog wal, Progressable progressable) throws IOException;

  public HLog getLog();

  public CompactSplitThread getCompactSplitThread();

  public HServerInfo getServerInfo();

  public HRegion removeFromOnlineRegions(HRegionInfo regionInfo);
}
