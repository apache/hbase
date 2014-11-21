package org.apache.hadoop.hbase.consensus.rmap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONException;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestRegionLocator {
  Configuration conf;
  RegionLocator locator;

  @Before
  public void setUp() throws Exception {
    conf = HBaseConfiguration.create();
    conf.set(HConstants.RMAP_SUBSCRIPTION,
            getClass().getResource("rmap.json").toURI().toString());
    conf.set(HConstants.HYDRABASE_DCNAME, "DUMMYCLUSTER1");

    locator = new RegionLocator(conf);
    locator.refresh();
  }

  @Test
  public void testGetRegionsForServer() throws IOException, JSONException {
    List<HRegionInfo> regions = locator.getRegionsForServer(
            new HServerAddress("10.159.9.45:60020"));
    assertNotNull(regions);
    assertEquals(28, regions.size());
    for (HRegionInfo region : regions) {
      HTableDescriptor table = region.getTableDesc();
      if ("VerificationTest_DummyTable".equals(table.getNameAsString())) {
        assertEquals(3, table.getColumnFamilies().length);
      } else if ("wutang".equals(table.getNameAsString())) {
        assertEquals(5, table.getColumnFamilies().length);
      }
    }
  }

  @Test
  public void testGetRegionsForTableWithPeersInMultipleCells() throws Exception {
    List<HRegionInfo> regions = locator.getRegionsForTable(
            Bytes.toBytes("RPCBenchmarkingTable"));
    assertNotNull(regions);
    assertEquals(3, regions.size());
    for (HRegionInfo region : regions) {
      if ("2aaaaaaa".equals(Bytes.toString(region.getStartKey()))) {
        Map<String, Map<HServerAddress, Integer>> expectedPeers = new HashMap<>();
        Map<HServerAddress, Integer> dummyCluster1 = new HashMap<>();
        dummyCluster1.put(new HServerAddress("10.159.9.45:60020"), 1);
        dummyCluster1.put(new HServerAddress("10.159.9.47:60020"), 2);
        expectedPeers.put("DUMMYCLUSTER1", dummyCluster1);
        Map<HServerAddress, Integer> dummyCluster2 = new HashMap<>();
        dummyCluster2.put(new HServerAddress("10.159.9.42:60020"), 3);
        dummyCluster2.put(new HServerAddress("10.159.9.43:60020"), 4);
        expectedPeers.put("DUMMYCLUSTER2", dummyCluster2);
        Map<HServerAddress, Integer> dummyCluster3 = new HashMap<>();
        dummyCluster3.put(new HServerAddress("10.159.9.49:60020"), 5);
        expectedPeers.put("DUMMYCLUSTER3", dummyCluster3);
        assertEquals(expectedPeers, region.getPeers());
        Map<HServerAddress, String> peersWithCluster = region
                .getPeersWithCluster();
        assertEquals("DUMMYCLUSTER1",
                peersWithCluster.get(new HServerAddress("10.159.9.45:60020")));
        assertEquals("DUMMYCLUSTER1",
                peersWithCluster.get(new HServerAddress("10.159.9.47:60020")));
        assertEquals("DUMMYCLUSTER2",
                peersWithCluster.get(new HServerAddress("10.159.9.42:60020")));
        assertEquals("DUMMYCLUSTER2",
                peersWithCluster.get(new HServerAddress("10.159.9.43:60020")));
        assertEquals("DUMMYCLUSTER3",
                peersWithCluster.get(new HServerAddress("10.159.9.49:60020")));

      }
    }
  }

  @Test
  public void testLocateRegionSingleRegion() throws IOException, JSONException {
    byte[] row = new byte[1];
    for (int i = 0; i < 256; ++i) {
      row[0] = (byte) i;
      HRegionInfo region = locator.findRegion(Bytes.toBytes("wutang"), row);
      assertNotNull(region);
      assertEquals(5, region.getTableDesc().getColumnFamilies().length);
      assertEquals("b2696f3faa4bd5767f2800bbcc2687c0", region.getEncodedName());
      assertEquals("", Bytes.toString(region.getStartKey()));
      assertEquals("", Bytes.toString(region.getEndKey()));
      assertEquals(1370994021138L, region.getRegionId());
      assertTrue(region.getFavoredNodes() == null ||
        region.getFavoredNodes().length == 0);
      assertEquals(1, region.getPeersWithRank().size());
      assertEquals(new Integer(1),
              region.getPeersWithRank().get(new HServerAddress("10.159.9.45:60020")));
      assertEquals(1, region.getPeers().size());
      assertEquals(1, region.getPeers().get("DUMMYCLUSTER1").size());
      assertEquals(new Integer(1), region.getPeers().get("DUMMYCLUSTER1")
              .get(new HServerAddress("10.159.9.45:60020")));

    }
  }

  @Test
  public void testLocateRegion() throws IOException, JSONException {
    HRegionInfo region;

    // Test first region
    region = locator.findRegion(Bytes.toBytes("loadtest"),
            Bytes.toBytes("0"));
    assertNotNull(region);
    assertEquals(1, region.getTableDesc().getColumnFamilies().length);
    assertEquals("", Bytes.toString(region.getStartKey()));
    assertEquals("11111111", Bytes.toString(region.getEndKey()));

    // Test last region
    region = locator.findRegion(Bytes.toBytes("loadtest"),
            Bytes.toBytes("f"));
    assertNotNull(region);
    assertEquals(1, region.getTableDesc().getColumnFamilies().length);
    assertEquals("eeeeeeee", Bytes.toString(region.getStartKey()));
    assertEquals("", Bytes.toString(region.getEndKey()));

    // Test regions in the middle
    region = locator.findRegion(Bytes.toBytes("loadtest"),
            Bytes.toBytes("9"));
    assertNotNull(region);
    assertEquals(1, region.getTableDesc().getColumnFamilies().length);
    assertEquals("88888888", Bytes.toString(region.getStartKey()));
    assertEquals("99999999", Bytes.toString(region.getEndKey()));

    region = locator.findRegion(Bytes.toBytes("loadtest"),
            Bytes.toBytes("9abcdefg"));
    assertNotNull(region);
    assertEquals(1, region.getTableDesc().getColumnFamilies().length);
    assertEquals("99999999", Bytes.toString(region.getStartKey()));
    assertEquals("aaaaaaaa", Bytes.toString(region.getEndKey()));

    // Test boundaries
    region = locator.findRegion(Bytes.toBytes("loadtest"),
            Bytes.toBytes("66666666"));
    assertNotNull(region);
    assertEquals(1, region.getTableDesc().getColumnFamilies().length);
    assertEquals("66666666", Bytes.toString(region.getStartKey()));
    assertEquals("77777777", Bytes.toString(region.getEndKey()));

    region = locator.findRegion(Bytes.toBytes("loadtest"),
            Bytes.toBytes("cccccccc0"));
    assertNotNull(region);
    assertEquals(1, region.getTableDesc().getColumnFamilies().length);
    assertEquals("cccccccc", Bytes.toString(region.getStartKey()));
    assertEquals("dddddddd", Bytes.toString(region.getEndKey()));

    region = locator.findRegion(Bytes.toBytes("loadtest"),
            Bytes.toBytes("2222222"));
    assertNotNull(region);
    assertEquals(1, region.getTableDesc().getColumnFamilies().length);
    assertEquals("11111111", Bytes.toString(region.getStartKey()));
    assertEquals("22222222", Bytes.toString(region.getEndKey()));
  }

  @Test
  public void shouldReturnAllRegionsGroupedByTable() {
    List<List<HRegionInfo>> regionsByTable =
            locator.getAllRegionsGroupByTable();
    assertTrue(regionsByTable.size() > 0);
    for (List<HRegionInfo> regions : regionsByTable) {
      assertTrue(regions.size() > 0);
    }
  }
}
