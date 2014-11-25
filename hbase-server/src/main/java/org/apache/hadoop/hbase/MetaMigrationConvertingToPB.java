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

package org.apache.hadoop.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.MetaTableAccessor.Visitor;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A tool to migrate the data stored in hbase:meta table to pbuf serialization.
 * Supports migrating from 0.92.x and 0.94.x to 0.96.x for the catalog table.
 * @deprecated will be removed for the major release after 0.96.
 */
@Deprecated
public class MetaMigrationConvertingToPB {

  private static final Log LOG = LogFactory.getLog(MetaMigrationConvertingToPB.class);

  private static class ConvertToPBMetaVisitor implements Visitor {
    private final MasterServices services;
    private long numMigratedRows;

    public ConvertToPBMetaVisitor(MasterServices services) {
      this.services = services;
      numMigratedRows = 0;
    }

    @Override
    public boolean visit(Result r) throws IOException {
      if (r ==  null || r.isEmpty()) return true;
      // Check info:regioninfo, info:splitA, and info:splitB.  Make sure all
      // have migrated HRegionInfos.
      byte [] hriBytes = getBytes(r, HConstants.REGIONINFO_QUALIFIER);
      // Presumes that an edit updating all three cells either succeeds or
      // doesn't -- that we don't have case of info:regioninfo migrated but not
      // info:splitA.
      if (isMigrated(hriBytes)) return true;
      // OK. Need to migrate this row in meta.

      //This will 'migrate' the HRI from 092.x and 0.94.x to 0.96+ by reading the
      //writable serialization
      HRegionInfo hri = parseFrom(hriBytes);

      // Now make a put to write back to meta.
      Put p =  MetaTableAccessor.makePutFromRegionInfo(hri);

      // Now migrate info:splitA and info:splitB if they are not null
      migrateSplitIfNecessary(r, p, HConstants.SPLITA_QUALIFIER);
      migrateSplitIfNecessary(r, p, HConstants.SPLITB_QUALIFIER);

      MetaTableAccessor.putToMetaTable(this.services.getConnection(), p);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Migrated " + Bytes.toString(p.getRow()));
      }
      numMigratedRows++;
      return true;
    }
  }

  static void migrateSplitIfNecessary(final Result r, final Put p, final byte [] which)
      throws IOException {
    byte [] hriSplitBytes = getBytes(r, which);
    if (!isMigrated(hriSplitBytes)) {
      //This will 'migrate' the HRI from 092.x and 0.94.x to 0.96+ by reading the
      //writable serialization
      HRegionInfo hri = parseFrom(hriSplitBytes);
      p.addImmutable(HConstants.CATALOG_FAMILY, which, hri.toByteArray());
    }
  }

  static HRegionInfo parseFrom(byte[] hriBytes) throws IOException {
    try {
      return HRegionInfo.parseFrom(hriBytes);
    } catch (DeserializationException ex) {
      throw new IOException(ex);
    }
  }

  /**
   * @param r Result to dig in.
   * @param qualifier Qualifier to look at in the passed <code>r</code>.
   * @return Bytes for an HRegionInfo or null if no bytes or empty bytes found.
   */
  static byte [] getBytes(final Result r, final byte [] qualifier) {
    byte [] hriBytes = r.getValue(HConstants.CATALOG_FAMILY, qualifier);
    if (hriBytes == null || hriBytes.length <= 0) return null;
    return hriBytes;
  }

  static boolean isMigrated(final byte [] hriBytes) {
    if (hriBytes == null || hriBytes.length <= 0) return true;

    return ProtobufUtil.isPBMagicPrefix(hriBytes);
  }

  /**
   * Converting writable serialization to PB, if it is needed.
   * @param services MasterServices to get a handle on master
   * @return num migrated rows
   * @throws IOException or RuntimeException if something goes wrong
   */
  public static long updateMetaIfNecessary(final MasterServices services)
  throws IOException {
    if (isMetaTableUpdated(services.getConnection())) {
      LOG.info("META already up-to date with PB serialization");
      return 0;
    }
    LOG.info("META has Writable serializations, migrating hbase:meta to PB serialization");
    try {
      long rows = updateMeta(services);
      LOG.info("META updated with PB serialization. Total rows updated: " + rows);
      return rows;
    } catch (IOException e) {
      LOG.warn("Update hbase:meta with PB serialization failed." + "Master startup aborted.");
      throw e;
    }
  }

  /**
   * Update hbase:meta rows, converting writable serialization to PB
   * @return num migrated rows
   */
  static long updateMeta(final MasterServices masterServices) throws IOException {
    LOG.info("Starting update of META");
    ConvertToPBMetaVisitor v = new ConvertToPBMetaVisitor(masterServices);
    MetaTableAccessor.fullScan(masterServices.getConnection(), v);
    LOG.info("Finished update of META. Total rows updated:" + v.numMigratedRows);
    return v.numMigratedRows;
  }

  /**
   * @param hConnection connection to be used
   * @return True if the meta table has been migrated.
   * @throws IOException
   */
  static boolean isMetaTableUpdated(final HConnection hConnection) throws IOException {
    List<Result> results = MetaTableAccessor.fullScanOfMeta(hConnection);
    if (results == null || results.isEmpty()) {
      LOG.info("hbase:meta doesn't have any entries to update.");
      return true;
    }
    for (Result r : results) {
      byte[] value = r.getValue(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
      if (!isMigrated(value)) {
        return false;
      }
    }
    return true;
  }
}
