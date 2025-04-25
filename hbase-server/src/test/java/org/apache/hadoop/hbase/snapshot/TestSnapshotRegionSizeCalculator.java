package org.apache.hadoop.hbase.snapshot;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestSnapshotRegionSizeCalculator {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSnapshotRegionSizeCalculator.class);

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static FileSystem fs;
  private static Path rootDir;
  private static Path snapshotDir;
  private static SnapshotProtos.SnapshotDescription snapshotDesc;
  private static SnapshotManifest manifest;

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    fs = TEST_UTIL.getTestFileSystem();
    rootDir = TEST_UTIL.getDataTestDir("TestSnapshotRegionSizeCalculator");
    CommonFSUtils.setRootDir(conf, rootDir);

    // Create a mock snapshot with a region and store files
    SnapshotTestingUtils.SnapshotMock snapshotMock =
      new SnapshotTestingUtils.SnapshotMock(conf, fs, rootDir);
    SnapshotTestingUtils.SnapshotMock.SnapshotBuilder builder =
      snapshotMock.createSnapshotV2("snapshot", "testTable", 4);
    builder.addRegion();
    builder.addRegion();
    builder.addRegion();
    builder.addRegion();
    snapshotDir = builder.commit();
    snapshotDesc = builder.getSnapshotDescription();
    manifest = SnapshotManifest.open(conf, fs, snapshotDir, snapshotDesc);

  }

  @AfterClass
  public static void tearDown() throws Exception {
    fs.delete(rootDir, true);
  }

  @Test
  public void testCalculateRegionSizes() throws IOException {
    SnapshotRegionSizeCalculator calculator =
      new SnapshotRegionSizeCalculator(TEST_UTIL.getConfiguration(), manifest);
    Map<String, Long> regionSizes = calculator.calculateRegionSizes();

    // Verify that the region sizes are calculated correctly
    assertTrue("No regions found in the snapshot", !regionSizes.isEmpty());
    for (Map.Entry<String, Long> entry : regionSizes.entrySet()) {
      assertTrue("Region size should be non-negative", entry.getValue() == 0);
    }
  }
}
