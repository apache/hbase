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
package org.apache.hadoop.hbase.master;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.RemoteExceptionHandler;

import java.io.IOException;

/** Scanner for the <code>ROOT</code> HRegion. */
class RootScanner extends BaseScanner {
  /**
   * Constructor
   * @param master
   */
  public RootScanner(HMaster master) {
    super(master, true);
  }

  /**
   * Don't retry if we get an error while scanning. Errors are most often
   * caused by the server going away. Wait until next rescan interval when
   * things should be back to normal.
   * @return True if successfully scanned.
   */
  private boolean scanRoot() {
    master.getRegionManager().waitForRootRegionLocation();
    if (master.isClosed()) {
      return false;
    }

    try {
      // Don't interrupt us while we're working
      synchronized(scannerLock) {
        HServerAddress rootRegionLocation = master.getRegionManager().getRootRegionLocation();
        if (rootRegionLocation != null) {
          if (HTableDescriptor.isMetaregionSeqidRecordEnabled(master.getConfiguration())) {
            scanRegion(new MetaRegion(rootRegionLocation,
                HRegionInfo.ROOT_REGIONINFO_WITH_HISTORIAN_COLUMN));
          } else {
            scanRegion(new MetaRegion(rootRegionLocation, HRegionInfo.ROOT_REGIONINFO));
          }
        }
      }
    } catch (IOException e) {
      e = RemoteExceptionHandler.checkIOException(e);
      LOG.warn("Scan ROOT region", e);
      // Make sure the file system is still available, but don't do anything
      //  if it's not available.
      master.checkFileSystem(false);
      // TODO: we used to ignore this. Now, we'll enter an infinite loop if
      // this is an idempotent problem but the web ui will be up.
      // Revisit this later
      return false;
    } catch (Exception e) {
      // If for some reason we get some other kind of exception,
      // at least log it rather than go out silently.
      LOG.error("Unexpected exception", e);
      // TODO: See above
      return false;
    }
    return true;
  }

  @Override
  protected void maintenanceScan() {
    scanRoot();
  }
}
