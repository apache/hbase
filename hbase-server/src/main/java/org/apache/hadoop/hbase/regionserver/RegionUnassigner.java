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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Used to unssign a region when we hit FNFE.
 */
@InterfaceAudience.Private
class RegionUnassigner {

  private static final Log LOG = LogFactory.getLog(RegionUnassigner.class);

  private final RegionServerServices rsServices;

  private final HRegionInfo regionInfo;

  private boolean unassigning = false;

  RegionUnassigner(RegionServerServices rsServices, HRegionInfo regionInfo) {
    this.rsServices = rsServices;
    this.regionInfo = regionInfo;
  }

  synchronized void unassign() {
    if (unassigning) {
      return;
    }
    unassigning = true;
    new Thread("RegionUnassigner." + regionInfo.getEncodedName()) {
      @Override
      public void run() {
        LOG.info("Unassign " + regionInfo.getRegionNameAsString());
        try {
          rsServices.unassign(regionInfo.getRegionName());
        } catch (IOException e) {
          LOG.warn("Unassigned " + regionInfo.getRegionNameAsString() + " failed", e);
        } finally {
          synchronized (RegionUnassigner.this) {
            unassigning = false;
          }
        }
      }
    }.start();
  }
}
