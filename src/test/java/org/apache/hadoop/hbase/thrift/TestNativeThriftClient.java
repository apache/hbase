package org.apache.hadoop.hbase.thrift;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestNativeThriftClient {
  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Start mini cluster with enabled Native Thrift server
    TEST_UTIL.getConfiguration().setBoolean("hbase.regionserver.export.thrift",
        true);
    TEST_UTIL.startMiniCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testNativeClientUnitTest()
      throws IOException, InterruptedException {
    // Spawn the current version of client unit tests from fbcode.
    // Allow the developer to override
    // the default fbcode build location.
    String fbcodeDir = System.getenv("FBCODE_DIR");
    if (fbcodeDir == null) {
      fbcodeDir = "/home/engshare/contbuild/fbcode/hbase";
    }
    executeCommand(new String[] {
      fbcodeDir + "/_bin/hbase/src/testing/native_thrift",
      "--hbase",
      "localhost:" + TEST_UTIL.getConfiguration().getInt(
        HConstants.ZOOKEEPER_CLIENT_PORT,
        HConstants.DEFAULT_ZOOKEPER_CLIENT_PORT),
      "--permanent_test_table_name",
      ""});
  }

  private void executeCommand(String[] command)
      throws IOException, InterruptedException {
    LOG.debug("Command : " +  Arrays.toString(command));

    Process p = Runtime.getRuntime().exec(command);

    BufferedReader stdInput = new BufferedReader(
        new InputStreamReader(p.getInputStream()));
    BufferedReader stdError = new BufferedReader(
        new InputStreamReader(p.getErrorStream()));

    // read the output from the command
    String s = null;
    while ((s = stdInput.readLine()) != null) {
      System.out.println(s);
    }

    // read any errors from the attempted command
    while ((s = stdError.readLine()) != null) {
      System.out.println(s);
    }

    Assert.assertEquals(0, p.waitFor());
  }

}
