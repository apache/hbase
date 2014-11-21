package org.apache.hadoop.hbase.consensus.rmap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.net.InetSocketAddress;
import java.util.*;

public class Parser {
  private Configuration conf;

  public Parser(final Configuration conf) {
    this.conf = conf;
  }

  public List<HRegionInfo> parseEncodedRMap(JSONObject encodedRMap)
          throws JSONException {
    List<HRegionInfo> regions = new ArrayList<>();
    JSONObject tables = encodedRMap.getJSONObject("tables");

    for (Iterator<String> names = tables.keys(); names.hasNext();) {
      String name = names.next();
      regions.addAll(parseTable(name, tables.getJSONObject(name)));
    }

    return regions;
  }

  public List<HRegionInfo> parseTable(String name, JSONObject table)
          throws JSONException {
    HTableDescriptor tableDesc = new HTableDescriptor(name);
    List<HRegionInfo> regions = Collections.emptyList();
    Iterator<String> keys = table.keys();
    while (keys.hasNext()) {
      String key = keys.next();
      if (key.equals("families")) {
        JSONObject families = table.getJSONObject(key);
        Iterator<String> familyKeys = families.keys();
        while (familyKeys.hasNext()) {
          String familyName = familyKeys.next();
          JSONObject familyJson = families.getJSONObject(familyName);
          tableDesc.addFamily(parseFamily(familyName, familyJson));
        }
      } else if (key.equals("regions")) {
        JSONArray regionsJson = table.getJSONArray(key);
        int length = regionsJson.length();
        regions = new ArrayList<>(length);
        for (int i = 0; i < length; ++i) {
          regions.add(parseRegion(tableDesc, regionsJson.getJSONObject(i)));
        }
      } else {
        String value = table.get(key).toString();
        tableDesc.setValue(key, value);
      }
    }
    return regions;
  }

  public HColumnDescriptor parseFamily(String name, JSONObject family)
          throws JSONException {
    HColumnDescriptor columnDesc = new HColumnDescriptor();
    columnDesc.setName(Bytes.toBytes(name));
    Iterator<String> keys = family.keys();
    while (keys.hasNext()) {
      String key = keys.next();
      String value = family.get(key).toString();
      columnDesc.setValue(key, value);
    }
    return columnDesc;
  }

  public HRegionInfo parseRegion(HTableDescriptor table, JSONObject region)
          throws JSONException {
    long id = region.getLong("id");
    byte[] startKey = Bytes.toBytes(region.getString("start_key"));
    byte[] endKey = Bytes.toBytes(region.getString("end_key"));
    Map<String, Map<HServerAddress, Integer>> peers = parsePeers(region
            .getJSONObject("peers"));
    Map<String, InetSocketAddress[]> favoredNodesMap = parseFavoredNodesMap(region
            .getJSONObject("favored_nodes"));
    return new HRegionInfo(table, startKey, endKey, false, id, peers,
            favoredNodesMap);
  }

  public Map<String, Map<HServerAddress, Integer>> parsePeers(JSONObject peersJson)
          throws JSONException {
    Map<String, Map<HServerAddress, Integer>> peers = new LinkedHashMap<>();
    Iterator<String> keys = peersJson.keys();
    while (keys.hasNext()) {
      String cellName = keys.next();
      JSONArray peersWithRank = peersJson.getJSONArray(cellName);
      peers.put(cellName, parsePeersWithRank(peersWithRank));
    }
    return peers;
  }

  public Map<HServerAddress, Integer> parsePeersWithRank(JSONArray peersJson)
          throws JSONException {
    Map<HServerAddress, Integer> peers = new LinkedHashMap<HServerAddress, Integer>();
    for (int i = 0; i < peersJson.length(); ++i) {
      String peer = peersJson.getString(i);
      int colonIndex = peer.lastIndexOf(':');
      peers.put(new HServerAddress(peer.substring(0, colonIndex)),
              Integer.valueOf(peer.substring(colonIndex + 1)));
    }
    return peers;
  }

  Map<String, InetSocketAddress[]> parseFavoredNodesMap(JSONObject favoredNodesJson)
          throws JSONException {
    Iterator<String> keys = favoredNodesJson.keys();

    HashMap<String, InetSocketAddress[]> favoredNodesMap = new HashMap<>();
    while (keys.hasNext()) {
      String cellName = keys.next();
      JSONArray peersWithRank = favoredNodesJson.getJSONArray(cellName);
      favoredNodesMap.put(cellName, parseFavoredNodes(peersWithRank));
    }
    return favoredNodesMap;
  }

  public InetSocketAddress[] parseFavoredNodes(JSONArray favoredNodesInCell)
          throws JSONException {
    if (favoredNodesInCell == null) {
      return null;
    } else {
      int length = favoredNodesInCell.length();
      InetSocketAddress[] favoredNodes = new InetSocketAddress[length];
      for (int i = 0; i < length; ++i) {
        String node = favoredNodesInCell.getString(i);
        int colonIndex = node.lastIndexOf(':');
        favoredNodes[i] = new InetSocketAddress(node.substring(0, colonIndex),
                Integer.parseInt(node.substring(colonIndex + 1)));

      }
      return favoredNodes;
    }
  }
}
