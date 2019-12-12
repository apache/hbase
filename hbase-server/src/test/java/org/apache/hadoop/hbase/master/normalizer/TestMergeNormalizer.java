package org.apache.hadoop.hbase.master.normalizer;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.MasterRpcServices;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.when;

@Category(SmallTests.class)
public class TestMergeNormalizer {
  private static final Log LOG = LogFactory.getLog(MergeNormalizer.class);

  private static RegionNormalizer normalizer;

  // mocks
  private static MasterServices masterServices;
  private static MasterRpcServices masterRpcServices;

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    normalizer = new MergeNormalizer();
  }

  @Test
  public void testNoNormalizationForMetaTable() throws HBaseIOException {
    TableName testTable = TableName.META_TABLE_NAME;
    List<HRegionInfo> hris = new ArrayList<>();
    Map<byte[], Integer> regionSizes = new HashMap<>();

    setupMocksForNormalizer(regionSizes, hris);
    List<NormalizationPlan> plans = normalizer.computePlanForTable(testTable);
    assertTrue(plans == null);
  }

  @Test
  public void testNoNormalizationIfTooFewRegions() throws HBaseIOException {
    TableName testTable = TableName.valueOf("testSplitOfSmallRegion");
    List<HRegionInfo> hris = new ArrayList<>();
    Map<byte[], Integer> regionSizes = new HashMap<>();

    HRegionInfo hri1 = new HRegionInfo(testTable, Bytes.toBytes("aaa"), Bytes.toBytes("bbb"));
    hris.add(hri1);
    regionSizes.put(hri1.getRegionName(), 10);

    HRegionInfo hri2 = new HRegionInfo(testTable, Bytes.toBytes("bbb"), Bytes.toBytes("ccc"));
    hris.add(hri2);
    regionSizes.put(hri2.getRegionName(), 15);

    setupMocksForNormalizer(regionSizes, hris);
    List<NormalizationPlan> plans = normalizer.computePlanForTable(testTable);
    assertTrue(plans == null);
  }

  @Test
  public void testNoNormalizationOnNormalizedCluster() throws HBaseIOException {
    TableName testTable = TableName.valueOf("testSplitOfSmallRegion");
    List<HRegionInfo> hris = new ArrayList<>();
    Map<byte[], Integer> regionSizes = new HashMap<>();

    HRegionInfo hri1 = new HRegionInfo(testTable, Bytes.toBytes("aaa"), Bytes.toBytes("bbb"));
    hris.add(hri1);
    regionSizes.put(hri1.getRegionName(), 10);

    HRegionInfo hri2 = new HRegionInfo(testTable, Bytes.toBytes("bbb"), Bytes.toBytes("ccc"));
    hris.add(hri2);
    regionSizes.put(hri2.getRegionName(), 15);

    HRegionInfo hri3 = new HRegionInfo(testTable, Bytes.toBytes("ccc"), Bytes.toBytes("ddd"));
    hris.add(hri3);
    regionSizes.put(hri3.getRegionName(), 8);

    HRegionInfo hri4 = new HRegionInfo(testTable, Bytes.toBytes("ddd"), Bytes.toBytes("eee"));
    hris.add(hri4);
    regionSizes.put(hri4.getRegionName(), 10);

    setupMocksForNormalizer(regionSizes, hris);
    List<NormalizationPlan> plans = normalizer.computePlanForTable(testTable);
    assertTrue(plans == null);
  }

  @Test
  public void testMergeOfSmallRegions() throws HBaseIOException {
    TableName testTable = TableName.valueOf("testMergeOfSmallRegions");
    List<HRegionInfo> hris = new ArrayList<>();
    Map<byte[], Integer> regionSizes = new HashMap<>();

    Timestamp currentTime = new Timestamp(System.currentTimeMillis());
    Timestamp threedaysBefore = new Timestamp(currentTime.getTime() - TimeUnit.DAYS.toMillis(3));

    HRegionInfo hri1 = new HRegionInfo(testTable, Bytes.toBytes("aaa"), Bytes.toBytes("bbb"),false,threedaysBefore.getTime());
    hris.add(hri1);
    regionSizes.put(hri1.getRegionName(), 15);

    HRegionInfo hri2 = new HRegionInfo(testTable, Bytes.toBytes("bbb"), Bytes.toBytes("ccc"),false, threedaysBefore.getTime());
    hris.add(hri2);
    regionSizes.put(hri2.getRegionName(), 5);

    HRegionInfo hri3 = new HRegionInfo(testTable, Bytes.toBytes("ccc"), Bytes.toBytes("ddd"),false, threedaysBefore.getTime());
    hris.add(hri3);
    regionSizes.put(hri3.getRegionName(), 5);

    HRegionInfo hri4 = new HRegionInfo(testTable, Bytes.toBytes("ddd"), Bytes.toBytes("eee"),false,threedaysBefore.getTime());
    hris.add(hri4);
    regionSizes.put(hri4.getRegionName(), 15);

    HRegionInfo hri5 = new HRegionInfo(testTable, Bytes.toBytes("eee"), Bytes.toBytes("fff"));
    hris.add(hri5);
    regionSizes.put(hri5.getRegionName(), 16);

    HRegionInfo hri6 = new HRegionInfo(testTable, Bytes.toBytes("fff"), Bytes.toBytes("ggg"),false,threedaysBefore.getTime());
    hris.add(hri6);
    regionSizes.put(hri6.getRegionName(), 0);

    HRegionInfo hri7 = new HRegionInfo(testTable, Bytes.toBytes("ggg"), Bytes.toBytes("hhh"),false);
    hris.add(hri7);
    regionSizes.put(hri7.getRegionName(), 0);

    setupMocksForNormalizer(regionSizes, hris);
    List<NormalizationPlan> plans = normalizer.computePlanForTable(testTable);

    NormalizationPlan plan = plans.get(0);
    assertTrue(plan instanceof MergeNormalizationPlan);
    assertEquals(hri2, ((MergeNormalizationPlan) plan).getFirstRegion());
    assertEquals(hri3, ((MergeNormalizationPlan) plan).getSecondRegion());
  }

  @Test
  public void testMergeOfNewSmallRegions() throws HBaseIOException {
    TableName testTable = TableName.valueOf("testMergeOfNewSmallRegions");
    List<HRegionInfo> hris = new ArrayList<>();
    Map<byte[], Integer> regionSizes = new HashMap<>();

    HRegionInfo hri1 = new HRegionInfo(testTable, Bytes.toBytes("aaa"), Bytes.toBytes("bbb"));
    hris.add(hri1);
    regionSizes.put(hri1.getRegionName(), 15);

    HRegionInfo hri2 = new HRegionInfo(testTable, Bytes.toBytes("bbb"), Bytes.toBytes("ccc"));
    hris.add(hri2);
    regionSizes.put(hri2.getRegionName(), 5);

    HRegionInfo hri3 = new HRegionInfo(testTable, Bytes.toBytes("ccc"), Bytes.toBytes("ddd"));
    hris.add(hri3);
    regionSizes.put(hri3.getRegionName(), 16);

    HRegionInfo hri4 = new HRegionInfo(testTable, Bytes.toBytes("ddd"), Bytes.toBytes("eee"));
    hris.add(hri4);
    regionSizes.put(hri4.getRegionName(), 15);

    HRegionInfo hri5 = new HRegionInfo(testTable, Bytes.toBytes("ddd"), Bytes.toBytes("eee"));
    hris.add(hri4);
    regionSizes.put(hri5.getRegionName(), 5);

    setupMocksForNormalizer(regionSizes, hris);
    List<NormalizationPlan> plans = normalizer.computePlanForTable(testTable);

    assertTrue(plans == null);
  }

  @SuppressWarnings("MockitoCast")
  protected void setupMocksForNormalizer(Map<byte[], Integer> regionSizes,
    List<HRegionInfo> hris) {
    masterServices = Mockito.mock(MasterServices.class, RETURNS_DEEP_STUBS);
    masterRpcServices = Mockito.mock(MasterRpcServices.class, RETURNS_DEEP_STUBS);

    // for simplicity all regions are assumed to be on one server; doesn't matter to us
    ServerName sn = ServerName.valueOf("localhost", 0, 1L);
    when(masterServices.getAssignmentManager().getRegionStates().
      getRegionsOfTable(any(TableName.class))).thenReturn(hris);
    when(masterServices.getAssignmentManager().getRegionStates().
      getRegionServerOfRegion(any(HRegionInfo.class))).thenReturn(sn);

    for (Map.Entry<byte[], Integer> region : regionSizes.entrySet()) {
      RegionLoad regionLoad = Mockito.mock(RegionLoad.class);
      when(regionLoad.getName()).thenReturn(region.getKey());
      when(regionLoad.getStorefileSizeMB()).thenReturn(region.getValue());

      // this is possibly broken with jdk9, unclear if false positive or not
      // suppress it for now, fix it when we get to running tests on 9
      // see: http://errorprone.info/bugpattern/MockitoCast
      when((Object) masterServices.getServerManager().getLoad(sn).
        getRegionsLoad().get(region.getKey())).thenReturn(regionLoad);
    }
    try {
      when(masterRpcServices.isSplitOrMergeEnabled(any(RpcController.class),
        any(MasterProtos.IsSplitOrMergeEnabledRequest.class))).thenReturn(
        MasterProtos.IsSplitOrMergeEnabledResponse.newBuilder().setEnabled(true).build());
    } catch (ServiceException se) {
      LOG.debug("error setting isSplitOrMergeEnabled switch", se);
    }

    normalizer.setMasterServices(masterServices);
    normalizer.setMasterRpcServices(masterRpcServices);
  }
}
