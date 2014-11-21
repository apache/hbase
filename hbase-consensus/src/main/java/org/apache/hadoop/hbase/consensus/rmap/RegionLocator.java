package org.apache.hadoop.hbase.consensus.rmap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class RegionLocator {
  private static final Logger LOG = LoggerFactory.getLogger(
          RegionLocator.class);

  private Configuration conf;

  // regionInfoMap is a mapping from table name to region start key to
  // HRegionInfo. This will be used in locateRegion and in turn in
  // HConnection.locateRegion, so it needs to be thread-safe as the same
  // HConnection can be used from multiple threads at the same time
  ConcurrentHashMap<String, ConcurrentSkipListMap<byte[], HRegionInfo>>
          regionInfoMap = new ConcurrentHashMap<>();

  public RegionLocator(final Configuration conf) {
    this.conf = conf;
  }

  public HRegionInfo findRegion(byte[] tableName, byte[] row) {
    ConcurrentSkipListMap<byte[], HRegionInfo> regions =
            regionInfoMap.get(Bytes.toString(tableName));
    if (regions != null) {
      Map.Entry<byte[], HRegionInfo> entry = regions.floorEntry(row);
      if (entry != null) {
        return entry.getValue();
      }
    }
    return null;
  }

  public List<HTableDescriptor> getAllTables() {
    List<HTableDescriptor> tables = new ArrayList<>(regionInfoMap.size());
    for (ConcurrentSkipListMap<byte[], HRegionInfo> regionMapForTable :
            regionInfoMap.values()) {
      if (regionMapForTable.size() > 0) {
        tables.add(regionMapForTable.firstEntry().getValue().getTableDesc());
      }
    }
    return tables;
  }

  public List<List<HRegionInfo>> getAllRegionsGroupByTable() {
    List<List<HRegionInfo>> regions = new ArrayList<>(regionInfoMap.size());
    for (ConcurrentSkipListMap<byte[], HRegionInfo> regionMapForTable :
            regionInfoMap.values()) {
      regions.add(new ArrayList<>(regionMapForTable.values()));
    }
    return regions;
  }

  /**
   * Get all servers found in the regionInfo map. This method iterates over all
   * HRegionInfo entries and thus might be expensive.
   *
   * @return a set containing all servers found in the region map
   */
  public Set<HServerAddress> getAllServers() {
    Set<HServerAddress> servers = new HashSet<>();
    for (ConcurrentSkipListMap<byte[], HRegionInfo> regionMapForTable :
            regionInfoMap.values()) {
      for (HRegionInfo region : regionMapForTable.values()) {
        for (HServerAddress server : region.getPeersWithRank().keySet()) {
          servers.add(server);
        }
      }
    }
    return servers;
  }

  public List<HRegionInfo> getRegionsForTable(byte[] tableName) {
    ConcurrentSkipListMap<byte[], HRegionInfo> regions =
            regionInfoMap.get(Bytes.toString(tableName));
    if (regions != null) {
      return new ArrayList<>(regions.values());
    } else {
      return null;
    }
  }

  public List<HRegionInfo> getRegionsForServer(final HServerAddress address) {
    List<HRegionInfo> regions = new ArrayList<>();
    for (ConcurrentSkipListMap<byte[], HRegionInfo> regionMapForTable :
            regionInfoMap.values()) {
      for (HRegionInfo region : regionMapForTable.values()) {
        if (region.getPeersWithRank().containsKey(address)) {
          regions.add(region);
        }
      }
    }
    return regions;
  }

  private void updateRegionInfoMap(final List<HRegionInfo> regions) {
    for (HRegionInfo region : regions) {
      String tableName = region.getTableDesc().getNameAsString();
      ConcurrentSkipListMap<byte[], HRegionInfo> regionMapForTable
              = regionInfoMap.get(tableName);
      if (regionMapForTable == null) {
        regionMapForTable = new ConcurrentSkipListMap<>(Bytes.BYTES_COMPARATOR);
        regionInfoMap.put(tableName, regionMapForTable);
      }
      regionMapForTable.put(region.getStartKey(), region);
    }
  }

  public void refresh() throws IOException, RMapException {
    Parser parser = new Parser(conf);

    URI uri = RMapConfiguration.getRMapSubscription(conf);
    if (uri != null) {
      RMapReader reader = RMapConfiguration.getRMapReader(conf, uri);

      try {
        JSONObject encodedRMap = reader.readRMap(uri).getEncodedRMap();
        updateRegionInfoMap(parser.parseEncodedRMap(encodedRMap));
      } catch (JSONException e) {
        throw new RMapException("Failed to decode JSON for RMap: " + uri, e);
      }
    }
  }

  public boolean isEmpty() {
    return regionInfoMap.isEmpty();
  }
}
