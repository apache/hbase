package org.apache.hadoop.hbase.consensus.rmap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TestParser {
  Configuration conf;
  Parser parser;
  JSONObject rmapAsJSON;

  @Before
  public void setUp() throws IOException, JSONException {
    conf = HBaseConfiguration.create();
    conf.set(HConstants.HYDRABASE_DCNAME, "DUMMYCLUSTER1");

    parser = new Parser(conf);
    rmapAsJSON = new JSONObject(new String(Files.readAllBytes(
            Paths.get(getClass().getResource("rmap.json").getPath()))));
  }

  @Test
  public void testParseRMap() throws IOException, JSONException {
    List<HRegionInfo> regions = parser.parseTable("RPCBenchmarkingTable",
            getTableObjectFromJSON("RPCBenchmarkingTable"));
    assertEquals(3, regions.size());
    HRegionInfo region = regions.get(0);
    HTableDescriptor table = region.getTableDesc();
    assertEquals("RPCBenchmarkingTable", table.getNameAsString());
    assertFalse(table.isMetaTable());
    assertFalse(table.isRootRegion());
    HColumnDescriptor[] columnFamilies = table.getColumnFamilies();
    assertEquals(1, columnFamilies.length);
    HColumnDescriptor cf0 = columnFamilies[0];
    assertEquals("cf", cf0.getNameAsString());
    assertEquals("true", cf0.getValue("BLOCKCACHE"));
    assertEquals("65536", cf0.getValue("BLOCKSIZE"));
    assertEquals("NONE", cf0.getValue("BLOOMFILTER"));
    assertEquals("0.01", cf0.getValue("BLOOMFILTER_ERRORRATE"));
    assertEquals("NONE", cf0.getValue("COMPRESSION"));
    assertEquals("NONE", cf0.getValue("DATA_BLOCK_ENCODING"));
    assertEquals("true", cf0.getValue("ENCODE_ON_DISK"));
    assertEquals("false", cf0.getValue("IN_MEMORY"));
    assertEquals("0", cf0.getValue("REPLICATION_SCOPE"));
    assertEquals("2147483647", cf0.getValue("TTL"));
    assertEquals("2147483647", cf0.getValue("VERSIONS"));

    assertEquals("aeeb54dc6fbca609443bd35796b59da5", region.getEncodedName());
    assertEquals("", Bytes.toString(region.getStartKey()));
    assertEquals("2aaaaaaa", Bytes.toString(region.getEndKey()));
    assertEquals(1373324048180L, region.getRegionId());

    InetSocketAddress[] favoredNodes =
            region.getFavoredNodesMap().get("DUMMYCLUSTER1");
    assertEquals(3, favoredNodes.length);
    assertEquals(new InetSocketAddress("10.159.9.49", 60020), favoredNodes[0]);
    assertEquals(new InetSocketAddress("10.159.9.45", 60020), favoredNodes[1]);
    assertEquals(new InetSocketAddress("10.159.9.47", 60020), favoredNodes[2]);

    Map<String, Map<HServerAddress, Integer>> peers = region.getPeers();
    assertEquals(1, peers.size());
    Map<HServerAddress, Integer> peersWithRank = region.getPeersWithRank();
    assertEquals(3, peersWithRank.size());
    assertEquals(new Integer(1),
            peersWithRank.get(new HServerAddress("10.159.9.41:60020")));
    assertEquals(new Integer(2),
            peersWithRank.get(new HServerAddress("10.159.9.45:60020")));
    assertEquals(new Integer(3),
            peersWithRank.get(new HServerAddress("10.159.9.47:60020")));
    assertEquals(peers.get("DUMMYCLUSTER1"), peersWithRank);

    assertEquals(null, peersWithRank.get(new HServerAddress("1.1.1.1:11111")));
  }

  private JSONObject getTableObjectFromJSON(final String name)
          throws JSONException {
    return rmapAsJSON.getJSONObject("tables").getJSONObject(name);
  }
}
