package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.util.StringUtils;

/**
 * Handles processing region splits. Put in a queue, owned by HRegionServer.
 */
class SplitRequest implements Runnable {
  static final Log LOG = LogFactory.getLog(SplitRequest.class);
  private final HRegion region;
  private final byte[] midKey;
  private static HTable root = null;
  private static HTable meta = null;

  SplitRequest(HRegion region, byte[] midKey) {
    this.region = region;
    this.midKey = midKey;
  }

  @Override
  public String toString() {
    return "regionName=" + region + ", midKey=" + Bytes.toStringBinary(midKey);
  }

  @Override
  public void run() {
    HRegionServer server = region.getRegionServer();
    try {
      Configuration conf = server.getConfiguration();
      final HRegionInfo oldRegionInfo = region.getRegionInfo();
      final long startTime = System.currentTimeMillis();
      final HRegion[] newRegions = region.splitRegion(midKey);
      if (newRegions == null) {
        // Didn't need to be split
        return;
      }
  
      // When a region is split, the META table needs to updated if we're
      // splitting a 'normal' region, and the ROOT table needs to be
      // updated if we are splitting a META region.
      HTable t = null;
      if (region.getRegionInfo().isMetaTable()) {
        // We need to update the root region
        if (root == null) {
          root = new HTable(conf, HConstants.ROOT_TABLE_NAME);
        }
        t = root;
      } else {
        // For normal regions we need to update the meta region
        if (meta == null) {
          meta = new HTable(conf, HConstants.META_TABLE_NAME);
        }
        t = meta;
      }
  
      // Mark old region as offline and split in META.
      // NOTE: there is no need for retry logic here. HTable does it for us.
      oldRegionInfo.setOffline(true);
      oldRegionInfo.setSplit(true);
      // Inform the HRegionServer that the parent HRegion is no-longer online.
      server.removeFromOnlineRegions(oldRegionInfo);
  
      Put put = new Put(oldRegionInfo.getRegionName());
      put.add(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER,
          Writables.getBytes(oldRegionInfo));
      put.add(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER,
          HConstants.EMPTY_BYTE_ARRAY);
      put.add(HConstants.CATALOG_FAMILY, HConstants.STARTCODE_QUALIFIER,
          HConstants.EMPTY_BYTE_ARRAY);
      put.add(HConstants.CATALOG_FAMILY, HConstants.SPLITA_QUALIFIER, Writables
          .getBytes(newRegions[0].getRegionInfo()));
      put.add(HConstants.CATALOG_FAMILY, HConstants.SPLITB_QUALIFIER, Writables
          .getBytes(newRegions[1].getRegionInfo()));
      t.put(put);
  
      // If we crash here, then the daughters will not be added and we'll have
      // and offlined parent but no daughters to take up the slack. hbase-2244
      // adds fixup to the metascanners.
  
      // Add new regions to META
      for (int i = 0; i < newRegions.length; i++) {
        put = new Put(newRegions[i].getRegionName());
        put.add(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER,
            Writables.getBytes(newRegions[i].getRegionInfo()));
        t.put(put);
      }
  
      // If we crash here, the master will not know of the new daughters and they
      // will not be assigned. The metascanner when it runs will notice and take
      // care of assigning the new daughters.
  
      // Now tell the master about the new regions
      server.reportSplit(oldRegionInfo, newRegions[0].getRegionInfo(),
          newRegions[1].getRegionInfo());
  
      LOG.info("region split, META updated, and report to master all"
          + " successful. Old region=" + oldRegionInfo.toString()
          + ", new regions: " + newRegions[0].toString() + ", "
          + newRegions[1].toString() + ". Split took "
          + StringUtils.formatTimeDiff(System.currentTimeMillis(), startTime));
      LOG.debug("Compaction/Split Status: " + server.compactSplitThread);
    } catch (IOException ex) {
      LOG.error("Split failed " + this, RemoteExceptionHandler
          .checkIOException(ex));
      server.checkFileSystem();
      // XXX should restructure the code above such that we don't have to abort
      server.abort("Split failed", ex);
    }
  }

}