/*
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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Mocking utility for common hbase:meta functionality
 */
public class MetaMockingUtil {

  /**
   * Returns a Result object constructed from the given region information simulating a catalog
   * table result.
   * @param region the HRegionInfo object or null
   * @return A mocked up Result that fakes a Get on a row in the <code>hbase:meta</code> table.
   */
  public static Result getMetaTableRowResult(final HRegionInfo region) throws IOException {
    return getMetaTableRowResult(region, null, Collections.unmodifiableList(Arrays.asList(null, null)));
  }

  /**
   * Returns a Result object constructed from the given region information simulating a catalog
   * table result.
   * @param region the HRegionInfo object or null
   * @param sn     to use making startcode and server hostname:port in meta or null
   * @return A mocked up Result that fakes a Get on a row in the <code>hbase:meta</code> table.
   */
  public static Result getMetaTableRowResult(final HRegionInfo region, final ServerName sn)
    throws IOException {
    return getMetaTableRowResult(region, sn, Collections.unmodifiableList(Arrays.asList(null, null)));
  }

  /**
   * Returns a Result object constructed from the given region information simulating a catalog
   * table result.
   * @param region the HRegionInfo object or null
   * @param sn     to use making startcode and server hostname:port in meta or null
   * @param splits split daughter regions or null
   * @return A mocked up Result that fakes a Get on a row in the <code>hbase:meta</code> table.
   */
  public static Result getMetaTableRowResult(RegionInfo region, final ServerName sn, final List<RegionInfo> splits) throws IOException {
    List<Cell> kvs = new ArrayList<>();
    if (region != null) {
      kvs.add(new KeyValue(region.getRegionName(), HConstants.CATALOG_FAMILY,
        HConstants.REGIONINFO_QUALIFIER, RegionInfo.toByteArray(region)));
    }

    if (sn != null) {
      kvs.add(new KeyValue(region.getRegionName(), HConstants.CATALOG_FAMILY,
        HConstants.SERVER_QUALIFIER, Bytes.toBytes(sn.getAddress().toString())));
      kvs.add(new KeyValue(region.getRegionName(), HConstants.CATALOG_FAMILY,
        HConstants.STARTCODE_QUALIFIER, Bytes.toBytes(sn.getStartcode())));
    }

    if(splits != null) {
      for(RegionInfo split : splits) {
        if(split != null) {
          kvs.add(new KeyValue(region.getRegionName(), HConstants.CATALOG_FAMILY,
            Bytes.toBytes(HConstants.MULTIPLE_REGIONS_QUALIFIER_PREFIX_STR + region.getEncodedName()),
            RegionInfo.toByteArray(split)));
        }
      }
    }

    // important: sort the kvs so that binary search work
    Collections.sort(kvs, MetaCellComparator.META_COMPARATOR);

    return Result.create(kvs);
  }

}
