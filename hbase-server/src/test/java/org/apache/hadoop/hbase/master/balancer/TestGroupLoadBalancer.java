package org.apache.hadoop.hbase.master.balancer;

import static org.junit.Assert.assertTrue;

import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MediumTests.class})
public class TestGroupLoadBalancer extends BalancerTestBase {
  private static GroupLoadBalancer loadBalancer;
  private static final Log LOG = LogFactory.getLog(TestGroupLoadBalancer.class);
  private static Configuration conf;

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    loadBalancer = new GroupLoadBalancer();
  }

  @Test
  public void testTablesArePutInRightGroups() throws Exception {

    // Create a configuration
    conf = HBaseConfiguration.create();
    conf.set("hbase.master.balancer.grouploadbalancer.groups", "group1;group2");
    conf.set("hbase.master.balancer.grouploadbalancer.defaultgroup", "group1");
    conf.set("hbase.master.balancer.grouploadbalancer.servergroups.group1", "10.255.196.145,60020");
    conf.set("hbase.master.balancer.grouploadbalancer.servergroups.group2", "10.255.196.145,60021");
    conf.set("hbase.master.balancer.grouploadbalancer.tablegroups.group1",
        "test_table_1;test_table_2");
    conf.set("hbase.master.balancer.grouploadbalancer.tablegroups.group2",
        "test_table_3;test_table_4");
    loadBalancer.setConf(conf);

    // Create two test region servers
    Random random = new Random();
    long randomTimeDelta = (long)random.nextInt(60000);
    long currentTimeStamp = System.currentTimeMillis();
    ServerName serverName1 = ServerName.valueOf("10.255.196.145:60020", currentTimeStamp);
    ServerName serverName2 =
        ServerName.valueOf("10.255.196.145:60021", currentTimeStamp + randomTimeDelta);

    // Create 4 test tables
    TableName tableName1 = TableName.valueOf("test_table_1");
    TableName tableName2 = TableName.valueOf("test_table_2");
    TableName tableName3 = TableName.valueOf("test_table_3");
    TableName tableName4 = TableName.valueOf("test_table_4");
    HRegionInfo hri1 = new HRegionInfo(tableName1);
    HRegionInfo hri2 = new HRegionInfo(tableName2);
    HRegionInfo hri3 = new HRegionInfo(tableName3);
    HRegionInfo hri4 = new HRegionInfo(tableName4);

    // Create a cluster where 2 tables need to be moved to other groups
    Map<ServerName, List<HRegionInfo>> testCluster = new HashMap<>();
    List<HRegionInfo> hriList1 = new ArrayList<>();
    List<HRegionInfo> hriList2 = new ArrayList<>();
    hriList1.add(hri1);
    hriList1.add(hri3);
    hriList2.add(hri2);
    hriList2.add(hri4);
    testCluster.put(serverName1, hriList1);
    testCluster.put(serverName2, hriList2);

    List<RegionPlan> regionPlanList = loadBalancer.balanceCluster(testCluster);

    // Sort so they are in the same order every time so we know which server each region needs to be
    // moved to
    Collections.sort(regionPlanList, new Comparator<RegionPlan>() {
      @Override public int compare(RegionPlan o1, RegionPlan o2) {
        return o1.getRegionInfo().getRegionNameAsString().
            compareTo(o2.getRegionInfo().getRegionNameAsString());
      }
    });

    // Check to make sure that regions are being moved to the right servers
    assertTrue(regionPlanList.get(0).getRegionInfo().getRegionNameAsString().contains("test_table_2"));
    assertTrue(regionPlanList.get(0).getSource().toString().contains("10.255.196.145,60021"));
    assertTrue(regionPlanList.get(0).getDestination().toString().contains("10.255.196.145,60020"));

    assertTrue(regionPlanList.get(1).getRegionInfo().getRegionNameAsString().contains("test_table_3"));
    assertTrue(regionPlanList.get(1).getSource().toString().contains("10.255.196.145,60020"));
    assertTrue(regionPlanList.get(1).getDestination().toString().contains("10.255.196.145,60021"));
  }

  @Test (expected = IllegalArgumentException.class)
  public void testDefaultGroupIsAPreExistingGroup() throws Exception {

    // Create a configuration where the default group is not a pre-existing group
    conf = HBaseConfiguration.create();
    conf.set("hbase.master.balancer.grouploadbalancer.groups", "group1;group2");
    conf.set("hbase.master.balancer.grouploadbalancer.defaultgroup", "group3");
    conf.set("hbase.master.balancer.grouploadbalancer.servergroups.group1", "10.255.196.145,60020");
    conf.set("hbase.master.balancer.grouploadbalancer.servergroups.group2", "10.255.196.145,60021");
    conf.set("hbase.master.balancer.grouploadbalancer.tablegroups.group1",
        "test_table_1");
    conf.set("hbase.master.balancer.grouploadbalancer.tablegroups.group2",
        "test_table_2");
    loadBalancer.setConf(conf);

    // Create two test region servers
    Random random = new Random();
    long randomTimeDelta = (long)random.nextInt(60000);
    long currentTimeStamp = System.currentTimeMillis();
    ServerName serverName1 = ServerName.valueOf("10.255.196.145:60020", currentTimeStamp);
    ServerName serverName2 =
        ServerName.valueOf("10.255.196.145:60021", currentTimeStamp + randomTimeDelta);

    // Create 2 test tables
    TableName tableName1 = TableName.valueOf("test_table_1");
    TableName tableName2 = TableName.valueOf("test_table_2");
    HRegionInfo hri1 = new HRegionInfo(tableName1);
    HRegionInfo hri2 = new HRegionInfo(tableName2);

    // Create a cluster
    Map<ServerName, List<HRegionInfo>> testCluster = new HashMap<>();
    List<HRegionInfo> hriList1 = new ArrayList<>();
    List<HRegionInfo> hriList2 = new ArrayList<>();
    hriList1.add(hri1);
    hriList2.add(hri2);
    testCluster.put(serverName1, hriList1);
    testCluster.put(serverName2, hriList2);

    List<RegionPlan> regionPlanList = loadBalancer.balanceCluster(testCluster);
  }

  @Test (expected = IllegalArgumentException.class)
  public void testAllGroupsHaveAtLeastOneServer() throws Exception {

    // Create a configuration where group2 which has no servers assigned to it
    conf = HBaseConfiguration.create();
    conf.set("hbase.master.balancer.grouploadbalancer.groups", "group1;group2");
    conf.set("hbase.master.balancer.grouploadbalancer.defaultgroup", "group1");
    conf.set("hbase.master.balancer.grouploadbalancer.servergroups.group1", "10.255.196.145,60020");
    conf.set("hbase.master.balancer.grouploadbalancer.tablegroups.group1",
        "test_table_1");
    conf.set("hbase.master.balancer.grouploadbalancer.tablegroups.group2",
        "test_table_2");
    loadBalancer.setConf(conf);

    // Create a test region server
    Random random = new Random();
    long currentTimeStamp = System.currentTimeMillis();
    ServerName serverName1 = ServerName.valueOf("10.255.196.145:60020", currentTimeStamp);

    // Create a test table
    TableName tableName1 = TableName.valueOf("test_table_1");
    HRegionInfo hri1 = new HRegionInfo(tableName1);

    // Create a cluster
    Map<ServerName, List<HRegionInfo>> testCluster = new HashMap<>();
    List<HRegionInfo> hriList1 = new ArrayList<>();
    hriList1.add(hri1);
    testCluster.put(serverName1, hriList1);

    List<RegionPlan> regionPlanList = loadBalancer.balanceCluster(testCluster);
  }

  @Test (expected = IllegalArgumentException.class)
  public void testAllGroupsHaveAtLeastOneTable() throws Exception {

    // Create a configuration where group2 which has no tables assigned to it
    conf = HBaseConfiguration.create();
    conf.set("hbase.master.balancer.grouploadbalancer.groups", "group1;group2");
    conf.set("hbase.master.balancer.grouploadbalancer.defaultgroup", "group1");
    conf.set("hbase.master.balancer.grouploadbalancer.servergroups.group1", "10.255.196.145,60020");
    conf.set("hbase.master.balancer.grouploadbalancer.servergroups.group2", "10.255.196.145,60021");
    conf.set("hbase.master.balancer.grouploadbalancer.tablegroups.group1",
        "test_table_1");
    loadBalancer.setConf(conf);

    // Create a test region server
    Random random = new Random();
    long currentTimeStamp = System.currentTimeMillis();
    ServerName serverName1 = ServerName.valueOf("10.255.196.145:60020", currentTimeStamp);

    // Create a test table
    TableName tableName1 = TableName.valueOf("test_table_1");
    HRegionInfo hri1 = new HRegionInfo(tableName1);

    // Create a cluster
    Map<ServerName, List<HRegionInfo>> testCluster = new HashMap<>();
    List<HRegionInfo> hriList1 = new ArrayList<>();
    hriList1.add(hri1);
    testCluster.put(serverName1, hriList1);

    List<RegionPlan> regionPlanList = loadBalancer.balanceCluster(testCluster);
  }

}
