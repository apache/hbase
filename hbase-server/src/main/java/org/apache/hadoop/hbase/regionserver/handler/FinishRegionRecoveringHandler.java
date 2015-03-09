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

package org.apache.hadoop.hbase.regionserver.handler;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;

public class FinishRegionRecoveringHandler extends EventHandler {
  private static final Log LOG = LogFactory.getLog(FinishRegionRecoveringHandler.class);

  protected final RegionServerServices rss;
  protected final String regionName;
  protected final String path;

  public FinishRegionRecoveringHandler(RegionServerServices rss,
      String regionName, String path) {
    // we are using the open region handlers, since this operation is in the region open lifecycle
    super(rss, EventType.M_RS_OPEN_REGION);
    this.rss = rss;
    this.regionName = regionName;
    this.path = path;
  }

  @Override
  public void process() throws IOException {
    HRegion region = this.rss.getRecoveringRegions().remove(regionName);
    if (region != null) {
      region.setRecovering(false);
      LOG.info(path + " deleted; " + regionName + " recovered.");
    }
  }

}
