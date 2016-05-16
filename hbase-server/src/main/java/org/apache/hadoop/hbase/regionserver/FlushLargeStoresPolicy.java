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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * A {@link FlushPolicy} that only flushes store larger a given threshold. If no store is large
 * enough, then all stores will be flushed.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public abstract class FlushLargeStoresPolicy extends FlushPolicy {

  private static final Log LOG = LogFactory.getLog(FlushLargeStoresPolicy.class);

  public static final String HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND =
      "hbase.hregion.percolumnfamilyflush.size.lower.bound";

  public static final String HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND_MIN =
      "hbase.hregion.percolumnfamilyflush.size.lower.bound.min";

  public static final long DEFAULT_HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND_MIN =
      1024 * 1024 * 16L;

  protected long flushSizeLowerBound = -1;

  protected long getFlushSizeLowerBound(HRegion region) {
    int familyNumber = region.getTableDesc().getFamilies().size();
    // For multiple families, lower bound is the "average flush size" by default
    // unless setting in configuration is larger.
    long flushSizeLowerBound = region.getMemstoreFlushSize() / familyNumber;
    long minimumLowerBound =
        getConf().getLong(HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND_MIN,
          DEFAULT_HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND_MIN);
    if (minimumLowerBound > flushSizeLowerBound) {
      flushSizeLowerBound = minimumLowerBound;
    }
    // use the setting in table description if any
    String flushedSizeLowerBoundString =
        region.getTableDesc().getValue(HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND);
    if (flushedSizeLowerBoundString == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("No " + HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND
            + " set in description of table " + region.getTableDesc().getTableName()
            + ", use config (" + flushSizeLowerBound + ") instead");
      }
    } else {
      try {
        flushSizeLowerBound = Long.parseLong(flushedSizeLowerBoundString);
      } catch (NumberFormatException nfe) {
        // fall back for fault setting
        LOG.warn("Number format exception when parsing "
            + HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND + " for table "
            + region.getTableDesc().getTableName() + ":" + flushedSizeLowerBoundString + ". " + nfe
            + ", use config (" + flushSizeLowerBound + ") instead");

      }
    }
    return flushSizeLowerBound;
  }

  protected boolean shouldFlush(Store store) {
    if (store.getMemStoreSize() > this.flushSizeLowerBound) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Flush Column Family " + store.getColumnFamilyName() + " of " +
            region.getRegionInfo().getEncodedName() + " because memstoreSize=" +
            store.getMemStoreSize() + " > lower bound=" + this.flushSizeLowerBound);
      }
      return true;
    }
    return false;
  }

}
