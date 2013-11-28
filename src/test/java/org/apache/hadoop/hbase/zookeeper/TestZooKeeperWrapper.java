package org.apache.hadoop.hbase.zookeeper;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper.NodeFilter;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestZooKeeperWrapper {

  private static final HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();

  private static final Log LOG = LogFactory.getLog(TestZooKeeperWrapper.class);

  private ZooKeeperWrapper zkWrapper;
  private RecoverableZooKeeper recoverableZK;

  @BeforeClass
  public static void setUpBeforeClass()
    throws IOException, InterruptedException {
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDownAfterClass()
    throws IOException {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUpBeforeTest()
    throws IOException {
    zkWrapper = ZooKeeperWrapper.createInstance(
        TEST_UTIL.getConfiguration(), "test instance");
    recoverableZK = zkWrapper.getRecoverableZooKeeper();
  }

  @After
  public void tearDownAfterTest() {
    // nothing to do
  }

  @SuppressWarnings("unused")
  private static void print(String[] lines) {
    System.out.println();
    for (String line : lines) {
      System.out.println(line);
    }
    System.out.println();
  }

  private String dump(String path)
    throws KeeperException, InterruptedException {
    List<String> result = zkWrapper.getChildrenRecursively(path);
    StringBuilder sb = new StringBuilder();
    for (String s : result) {
      if (sb.length() > 0) {
        sb.append(':');
      }
      sb.append(s);
    }
    return sb.toString();
  }

  private Stat create(String path)
    throws KeeperException, InterruptedException {
    recoverableZK.create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    return recoverableZK.exists(path, false);
  }

  private Stat create(String path, long lastTime)
    throws KeeperException, InterruptedException {
    Stat stat = create(path);
    while (stat.getMtime() <= lastTime) {
      Thread.sleep(1);
      stat = recoverableZK.setData(path, null, -1);
    }
    return stat;
  }

  private void delete(String path)
    throws KeeperException {
    if (zkWrapper.checkExists(path) == -1)
      return;
    zkWrapper.deleteNodeRecursively(path);
  }

  @Test
  public void testDeleteChildrenRecursively()
    throws KeeperException, InterruptedException {
    delete("/test");

    create("/test");
    create("/test/a");
    create("/test/a/b");
    create("/test/a/c");

    Assert.assertEquals("/test/a:/test/a/b:/test/a/c", dump("/test"));

    zkWrapper.deleteChildrenRecursively("/test", new NodeFilter());

    Assert.assertEquals("", dump("/test"));
  }

  @Test
  public void testDeleteTimeRange()
    throws KeeperException, InterruptedException {
    delete("/test");

    create("/test");
    long t1 = create("/test/a").getMtime();
    long t2 = create("/test/b", t1).getMtime();
    long t3 = create("/test/b/c", t2).getMtime();
    long t4 = create("/test/d", t3).getMtime();
    long t5 = create("/test/d/e", t4).getMtime();

    Assert.assertEquals("/test/a:/test/b:/test/b/c:/test/d:/test/d/e", dump("/test"));

    zkWrapper.deleteChildrenRecursively("/test", new NodeFilter(t2, t5));

    Assert.assertEquals("/test/a:/test/d:/test/d/e", dump("/test"));
  }
}
