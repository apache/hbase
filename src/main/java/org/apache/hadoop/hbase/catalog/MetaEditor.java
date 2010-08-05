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
package org.apache.hadoop.hbase.catalog;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;

/**
 * Writes region and assignment information to <code>.META.</code>.
 * <p>
 * Uses the {@link CatalogTracker} to obtain locations and connections to
 * catalogs.
 */
public class MetaEditor {
  private static final Log LOG = LogFactory.getLog(MetaEditor.class);

  /**
   * Adds a META row for the specified new region.
   * @param info region information
   * @throws IOException if problem connecting or updating meta
   */
  public static void addRegionToMeta(CatalogTracker catalogTracker,
      HRegionInfo regionInfo)
  throws IOException {
    Put put = new Put(regionInfo.getRegionName());
    put.add(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER,
        Writables.getBytes(regionInfo));
    catalogTracker.waitForMetaServerConnectionDefault().put(
        CatalogTracker.META_REGION, put);
    LOG.info("Added region " + regionInfo + " to META");
  }

  /**
   * Updates the location of the specified META region in ROOT to be the
   * specified server hostname and startcode.
   * <p>
   * Uses passed catalog tracker to get a connection to the server hosting
   * ROOT and makes edits to that region.
   *
   * @param catalogTracker catalog tracker
   * @param regionInfo region to update location of
   * @param serverInfo server the region is located on
   * @throws IOException
   */
  public static void updateMetaLocation(CatalogTracker catalogTracker,
      HRegionInfo regionInfo, HServerInfo serverInfo)
  throws IOException {
    updateLocation(catalogTracker.waitForRootServerConnectionDefault(),
        CatalogTracker.ROOT_REGION, regionInfo, serverInfo);
  }

  /**
   * Updates the location of the specified region in META to be the specified
   * server hostname and startcode.
   * <p>
   * Uses passed catalog tracker to get a connection to the server hosting
   * META and makes edits to that region.
   *
   * @param catalogTracker catalog tracker
   * @param regionInfo region to update location of
   * @param serverInfo server the region is located on
   * @throws IOException
   */
  public static void updateRegionLocation(CatalogTracker catalogTracker,
      HRegionInfo regionInfo, HServerInfo serverInfo)
  throws IOException {
    updateLocation(catalogTracker.waitForMetaServerConnectionDefault(),
        CatalogTracker.META_REGION, regionInfo, serverInfo);
  }

  /**
   * Updates the location of the specified region to be the specified server.
   * <p>
   * Connects to the specified server which should be hosting the specified
   * catalog region name to perform the edit.
   *
   * @param server connection to server hosting catalog region
   * @param catalogRegionName name of catalog region being updated
   * @param regionInfo region to update location of
   * @param serverInfo server the region is located on
   * @throws IOException
   */
  public static void updateLocation(HRegionInterface server,
      byte [] catalogRegionName, HRegionInfo regionInfo, HServerInfo serverInfo)
  throws IOException {
    Put put = new Put(regionInfo.getRegionName());
    put.add(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER,
        Bytes.toBytes(serverInfo.getHostnamePort()));
    put.add(HConstants.CATALOG_FAMILY, HConstants.STARTCODE_QUALIFIER,
        Bytes.toBytes(serverInfo.getStartCode()));
    server.put(catalogRegionName, put);

    LOG.info("Updated row " + regionInfo.getRegionNameAsString() +
          " in region " + Bytes.toString(catalogRegionName) + " with " +
          "server=" + serverInfo.getHostnamePort() + ", " +
          "startcode=" + serverInfo.getStartCode());
  }

  /**
   * Deletes the specified region from META.
   * @param catalogTracker
   * @param regionInfo region to be deleted from META
   * @throws IOException
   */
  public static void deleteRegion(CatalogTracker catalogTracker,
      HRegionInfo regionInfo)
  throws IOException {
    Delete delete = new Delete(regionInfo.getRegionName());
    catalogTracker.waitForMetaServerConnectionDefault().delete(
        CatalogTracker.META_REGION, delete);

    LOG.info("Deleted region " + regionInfo + " from META");
  }

  /**
   * Updates the region information for the specified region in META.
   * @param catalogTracker
   * @param regionInfo region to be updated in META
   * @throws IOException
   */
  public static void updateRegionInfo(CatalogTracker catalogTracker,
      HRegionInfo regionInfo)
  throws IOException {
    Put put = new Put(regionInfo.getRegionName());
    put.add(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER,
        Writables.getBytes(regionInfo));
    catalogTracker.waitForMetaServerConnectionDefault().put(
        CatalogTracker.META_REGION, put);
    LOG.info("Updated region " + regionInfo + " in META");
  }
}
