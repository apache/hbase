/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.zookeeper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseZKTestingUtil;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ZKTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil.ZKUtilOp;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test ZooKeeper multi-update functionality.
 */
@Category({ ZKTests.class, MediumTests.class })
public class TestZKMulti {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestZKMulti.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestZKMulti.class);
  private final static HBaseZKTestingUtil TEST_UTIL = new HBaseZKTestingUtil();
  private static ZKWatcher zkw = null;

  private static class ZKMultiAbortable implements Abortable {
    @Override
    public void abort(String why, Throwable e) {
      LOG.info(why, e);
    }

    @Override
    public boolean isAborted() {
      return false;
    }
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniZKCluster();
    Configuration conf = TEST_UTIL.getConfiguration();
    Abortable abortable = new ZKMultiAbortable();
    zkw = new ZKWatcher(conf,
      "TestZKMulti", abortable, true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniZKCluster();
  }

  @Test
  public void testSimpleMulti() throws Exception {
    // null multi
    ZKUtil.multiOrSequential(zkw, null, false);

    // empty multi
    ZKUtil.multiOrSequential(zkw, new LinkedList<>(), false);

    // single create
    String path = ZNodePaths.joinZNode(zkw.getZNodePaths().baseZNode, "testSimpleMulti");
    LinkedList<ZKUtilOp> singleCreate = new LinkedList<>();
    singleCreate.add(ZKUtilOp.createAndFailSilent(path, new byte[0]));
    ZKUtil.multiOrSequential(zkw, singleCreate, false);
    assertTrue(ZKUtil.checkExists(zkw, path) != -1);

    // single setdata
    LinkedList<ZKUtilOp> singleSetData = new LinkedList<>();
    byte [] data = Bytes.toBytes("foobar");
    singleSetData.add(ZKUtilOp.setData(path, data));
    ZKUtil.multiOrSequential(zkw, singleSetData, false);
    assertTrue(Bytes.equals(ZKUtil.getData(zkw, path), data));

    // single delete
    LinkedList<ZKUtilOp> singleDelete = new LinkedList<>();
    singleDelete.add(ZKUtilOp.deleteNodeFailSilent(path));
    ZKUtil.multiOrSequential(zkw, singleDelete, false);
    assertEquals(-1, ZKUtil.checkExists(zkw, path));
  }

  @Test
  public void testComplexMulti() throws Exception {
    String path1 = ZNodePaths.joinZNode(zkw.getZNodePaths().baseZNode, "testComplexMulti1");
    String path2 = ZNodePaths.joinZNode(zkw.getZNodePaths().baseZNode, "testComplexMulti2");
    String path3 = ZNodePaths.joinZNode(zkw.getZNodePaths().baseZNode, "testComplexMulti3");
    String path4 = ZNodePaths.joinZNode(zkw.getZNodePaths().baseZNode, "testComplexMulti4");
    String path5 = ZNodePaths.joinZNode(zkw.getZNodePaths().baseZNode, "testComplexMulti5");
    String path6 = ZNodePaths.joinZNode(zkw.getZNodePaths().baseZNode, "testComplexMulti6");
    // create 4 nodes that we'll setData on or delete later
    LinkedList<ZKUtilOp> create4Nodes = new LinkedList<>();
    create4Nodes.add(ZKUtilOp.createAndFailSilent(path1, Bytes.toBytes(path1)));
    create4Nodes.add(ZKUtilOp.createAndFailSilent(path2, Bytes.toBytes(path2)));
    create4Nodes.add(ZKUtilOp.createAndFailSilent(path3, Bytes.toBytes(path3)));
    create4Nodes.add(ZKUtilOp.createAndFailSilent(path4, Bytes.toBytes(path4)));
    ZKUtil.multiOrSequential(zkw, create4Nodes, false);
    assertTrue(Bytes.equals(ZKUtil.getData(zkw, path1), Bytes.toBytes(path1)));
    assertTrue(Bytes.equals(ZKUtil.getData(zkw, path2), Bytes.toBytes(path2)));
    assertTrue(Bytes.equals(ZKUtil.getData(zkw, path3), Bytes.toBytes(path3)));
    assertTrue(Bytes.equals(ZKUtil.getData(zkw, path4), Bytes.toBytes(path4)));

    // do multiple of each operation (setData, delete, create)
    LinkedList<ZKUtilOp> ops = new LinkedList<>();
    // setData
    ops.add(ZKUtilOp.setData(path1, Bytes.add(Bytes.toBytes(path1), Bytes.toBytes(path1))));
    ops.add(ZKUtilOp.setData(path2, Bytes.add(Bytes.toBytes(path2), Bytes.toBytes(path2))));
    // delete
    ops.add(ZKUtilOp.deleteNodeFailSilent(path3));
    ops.add(ZKUtilOp.deleteNodeFailSilent(path4));
    // create
    ops.add(ZKUtilOp.createAndFailSilent(path5, Bytes.toBytes(path5)));
    ops.add(ZKUtilOp.createAndFailSilent(path6, Bytes.toBytes(path6)));
    ZKUtil.multiOrSequential(zkw, ops, false);
    assertTrue(Bytes.equals(ZKUtil.getData(zkw, path1),
      Bytes.add(Bytes.toBytes(path1), Bytes.toBytes(path1))));
    assertTrue(Bytes.equals(ZKUtil.getData(zkw, path2),
      Bytes.add(Bytes.toBytes(path2), Bytes.toBytes(path2))));
    assertEquals(-1, ZKUtil.checkExists(zkw, path3));
    assertEquals(-1, ZKUtil.checkExists(zkw, path4));
    assertTrue(Bytes.equals(ZKUtil.getData(zkw, path5), Bytes.toBytes(path5)));
    assertTrue(Bytes.equals(ZKUtil.getData(zkw, path6), Bytes.toBytes(path6)));
  }

  @Test
  public void testSingleFailure() throws Exception {
    // try to delete a node that doesn't exist
    boolean caughtNoNode = false;
    String path = ZNodePaths.joinZNode(zkw.getZNodePaths().baseZNode, "testSingleFailureZ");
    LinkedList<ZKUtilOp> ops = new LinkedList<>();
    ops.add(ZKUtilOp.deleteNodeFailSilent(path));
    try {
      ZKUtil.multiOrSequential(zkw, ops, false);
    } catch (KeeperException.NoNodeException nne) {
      caughtNoNode = true;
    }
    assertTrue(caughtNoNode);

    // try to setData on a node that doesn't exist
    caughtNoNode = false;
    ops = new LinkedList<>();
    ops.add(ZKUtilOp.setData(path, Bytes.toBytes(path)));
    try {
      ZKUtil.multiOrSequential(zkw, ops, false);
    } catch (KeeperException.NoNodeException nne) {
      caughtNoNode = true;
    }
    assertTrue(caughtNoNode);

    // try to create on a node that already exists
    boolean caughtNodeExists = false;
    ops = new LinkedList<>();
    ops.add(ZKUtilOp.createAndFailSilent(path, Bytes.toBytes(path)));
    ZKUtil.multiOrSequential(zkw, ops, false);
    try {
      ZKUtil.multiOrSequential(zkw, ops, false);
    } catch (KeeperException.NodeExistsException nee) {
      caughtNodeExists = true;
    }
    assertTrue(caughtNodeExists);
  }

  @Test
  public void testSingleFailureInMulti() throws Exception {
    // try a multi where all but one operation succeeds
    String pathA = ZNodePaths.joinZNode(zkw.getZNodePaths().baseZNode, "testSingleFailureInMultiA");
    String pathB = ZNodePaths.joinZNode(zkw.getZNodePaths().baseZNode, "testSingleFailureInMultiB");
    String pathC = ZNodePaths.joinZNode(zkw.getZNodePaths().baseZNode, "testSingleFailureInMultiC");
    LinkedList<ZKUtilOp> ops = new LinkedList<>();
    ops.add(ZKUtilOp.createAndFailSilent(pathA, Bytes.toBytes(pathA)));
    ops.add(ZKUtilOp.createAndFailSilent(pathB, Bytes.toBytes(pathB)));
    ops.add(ZKUtilOp.deleteNodeFailSilent(pathC));
    boolean caughtNoNode = false;
    try {
      ZKUtil.multiOrSequential(zkw, ops, false);
    } catch (KeeperException.NoNodeException nne) {
      caughtNoNode = true;
    }
    assertTrue(caughtNoNode);
    // assert that none of the operations succeeded
    assertEquals(-1, ZKUtil.checkExists(zkw, pathA));
    assertEquals(-1, ZKUtil.checkExists(zkw, pathB));
    assertEquals(-1, ZKUtil.checkExists(zkw, pathC));
  }

  @Test
  public void testMultiFailure() throws Exception {
    String pathX = ZNodePaths.joinZNode(zkw.getZNodePaths().baseZNode, "testMultiFailureX");
    String pathY = ZNodePaths.joinZNode(zkw.getZNodePaths().baseZNode, "testMultiFailureY");
    String pathZ = ZNodePaths.joinZNode(zkw.getZNodePaths().baseZNode, "testMultiFailureZ");
    // create X that we will use to fail create later
    LinkedList<ZKUtilOp> ops = new LinkedList<>();
    ops.add(ZKUtilOp.createAndFailSilent(pathX, Bytes.toBytes(pathX)));
    ZKUtil.multiOrSequential(zkw, ops, false);

    // fail one of each create ,setData, delete
    String pathV = ZNodePaths.joinZNode(zkw.getZNodePaths().baseZNode, "testMultiFailureV");
    String pathW = ZNodePaths.joinZNode(zkw.getZNodePaths().baseZNode, "testMultiFailureW");
    ops = new LinkedList<>();
    ops.add(ZKUtilOp.createAndFailSilent(pathX, Bytes.toBytes(pathX))); // fail  -- already exists
    ops.add(ZKUtilOp.setData(pathY, Bytes.toBytes(pathY))); // fail -- doesn't exist
    ops.add(ZKUtilOp.deleteNodeFailSilent(pathZ)); // fail -- doesn't exist
    ops.add(ZKUtilOp.createAndFailSilent(pathX, Bytes.toBytes(pathV))); // pass
    ops.add(ZKUtilOp.createAndFailSilent(pathX, Bytes.toBytes(pathW))); // pass
    boolean caughtNodeExists = false;
    try {
      ZKUtil.multiOrSequential(zkw, ops, false);
    } catch (KeeperException.NodeExistsException nee) {
      // check first operation that fails throws exception
      caughtNodeExists = true;
    }
    assertTrue(caughtNodeExists);
    // check that no modifications were made
    assertNotEquals(-1, ZKUtil.checkExists(zkw, pathX));
    assertEquals(-1, ZKUtil.checkExists(zkw, pathY));
    assertEquals(-1, ZKUtil.checkExists(zkw, pathZ));
    assertEquals(-1, ZKUtil.checkExists(zkw, pathW));
    assertEquals(-1, ZKUtil.checkExists(zkw, pathV));

    // test that with multiple failures, throws an exception corresponding to first failure in list
    ops = new LinkedList<>();
    ops.add(ZKUtilOp.setData(pathY, Bytes.toBytes(pathY))); // fail -- doesn't exist
    ops.add(ZKUtilOp.createAndFailSilent(pathX, Bytes.toBytes(pathX))); // fail -- exists
    boolean caughtNoNode = false;
    try {
      ZKUtil.multiOrSequential(zkw, ops, false);
    } catch (KeeperException.NoNodeException nne) {
      // check first operation that fails throws exception
      caughtNoNode = true;
    }
    assertTrue(caughtNoNode);
    // check that no modifications were made
    assertNotEquals(-1, ZKUtil.checkExists(zkw, pathX));
    assertEquals(-1, ZKUtil.checkExists(zkw, pathY));
    assertEquals(-1, ZKUtil.checkExists(zkw, pathZ));
    assertEquals(-1, ZKUtil.checkExists(zkw, pathW));
    assertEquals(-1, ZKUtil.checkExists(zkw, pathV));
  }

  @Test
  public void testRunSequentialOnMultiFailure() throws Exception {
    String path1 = ZNodePaths.joinZNode(zkw.getZNodePaths().baseZNode, "runSequential1");
    String path2 = ZNodePaths.joinZNode(zkw.getZNodePaths().baseZNode, "runSequential2");
    String path3 = ZNodePaths.joinZNode(zkw.getZNodePaths().baseZNode, "runSequential3");
    String path4 = ZNodePaths.joinZNode(zkw.getZNodePaths().baseZNode, "runSequential4");

    // create some nodes that we will use later
    LinkedList<ZKUtilOp> ops = new LinkedList<>();
    ops.add(ZKUtilOp.createAndFailSilent(path1, Bytes.toBytes(path1)));
    ops.add(ZKUtilOp.createAndFailSilent(path2, Bytes.toBytes(path2)));
    ZKUtil.multiOrSequential(zkw, ops, false);

    // test that, even with operations that fail, the ones that would pass will pass
    // with runSequentialOnMultiFailure
    ops = new LinkedList<>();
    // pass
    ops.add(ZKUtilOp.setData(path1, Bytes.add(Bytes.toBytes(path1), Bytes.toBytes(path1))));
    // pass
    ops.add(ZKUtilOp.deleteNodeFailSilent(path2));
    // fail -- node doesn't exist
    ops.add(ZKUtilOp.deleteNodeFailSilent(path3));
    // pass
    ops.add(
      ZKUtilOp.createAndFailSilent(path4, Bytes.add(Bytes.toBytes(path4), Bytes.toBytes(path4))));
    ZKUtil.multiOrSequential(zkw, ops, true);
    assertTrue(Bytes.equals(ZKUtil.getData(zkw, path1),
      Bytes.add(Bytes.toBytes(path1), Bytes.toBytes(path1))));
    assertEquals(-1, ZKUtil.checkExists(zkw, path2));
    assertEquals(-1, ZKUtil.checkExists(zkw, path3));
    assertNotEquals(-1, ZKUtil.checkExists(zkw, path4));
  }

  /**
   * Verifies that for the given root node, it should delete all the child nodes
   * recursively using multi-update api.
   */
  @Test
  public void testdeleteChildrenRecursivelyMulti() throws Exception {
    String parentZNode = "/testRootMulti";
    createZNodeTree(parentZNode);

    ZKUtil.deleteChildrenRecursivelyMultiOrSequential(zkw, true, parentZNode);

    assertTrue("Wrongly deleted parent znode!",
        ZKUtil.checkExists(zkw, parentZNode) > -1);
    List<String> children = zkw.getRecoverableZooKeeper().getChildren(
        parentZNode, false);
    assertEquals("Failed to delete child znodes!", 0, children.size());
  }

  /**
   * Verifies that for the given root node, it should delete all the nodes recursively using
   * multi-update api.
   */
  @Test
  public void testDeleteNodeRecursivelyMulti() throws Exception {
    String parentZNode = "/testdeleteNodeRecursivelyMulti";
    createZNodeTree(parentZNode);

    ZKUtil.deleteNodeRecursively(zkw, parentZNode);
    assertEquals("Parent znode should be deleted.", -1, ZKUtil.checkExists(zkw, parentZNode));
  }

  @Test
  public void testDeleteNodeRecursivelyMultiOrSequential() throws Exception {
    String parentZNode1 = "/testdeleteNode1";
    String parentZNode2 = "/testdeleteNode2";
    String parentZNode3 = "/testdeleteNode3";
    createZNodeTree(parentZNode1);
    createZNodeTree(parentZNode2);
    createZNodeTree(parentZNode3);

    ZKUtil.deleteNodeRecursivelyMultiOrSequential(zkw, false, parentZNode1, parentZNode2,
      parentZNode3);
    assertEquals("Parent znode 1 should be deleted.", -1, ZKUtil.checkExists(zkw, parentZNode1));
    assertEquals("Parent znode 2 should be deleted.", -1, ZKUtil.checkExists(zkw, parentZNode2));
    assertEquals("Parent znode 3 should be deleted.", -1, ZKUtil.checkExists(zkw, parentZNode3));
  }

  @Test
  public void testDeleteChildrenRecursivelyMultiOrSequential() throws Exception {
    String parentZNode1 = "/testdeleteChildren1";
    String parentZNode2 = "/testdeleteChildren2";
    String parentZNode3 = "/testdeleteChildren3";
    createZNodeTree(parentZNode1);
    createZNodeTree(parentZNode2);
    createZNodeTree(parentZNode3);

    ZKUtil.deleteChildrenRecursivelyMultiOrSequential(zkw, true, parentZNode1, parentZNode2,
      parentZNode3);

    assertTrue("Wrongly deleted parent znode 1!", ZKUtil.checkExists(zkw, parentZNode1) > -1);
    List<String> children = zkw.getRecoverableZooKeeper().getChildren(parentZNode1, false);
    assertEquals("Failed to delete child znodes of parent znode 1!", 0, children.size());

    assertTrue("Wrongly deleted parent znode 2!", ZKUtil.checkExists(zkw, parentZNode2) > -1);
    children = zkw.getRecoverableZooKeeper().getChildren(parentZNode2, false);
    assertEquals("Failed to delete child znodes of parent znode 1!", 0, children.size());

    assertTrue("Wrongly deleted parent znode 3!", ZKUtil.checkExists(zkw, parentZNode3) > -1);
    children = zkw.getRecoverableZooKeeper().getChildren(parentZNode3, false);
    assertEquals("Failed to delete child znodes of parent znode 1!", 0, children.size());
  }

  @Test
  public void testBatchedDeletesOfWideZNodes() throws Exception {
    // Batch every 50bytes
    final int batchSize = 50;
    Configuration localConf = new Configuration(TEST_UTIL.getConfiguration());
    localConf.setInt("zookeeper.multi.max.size", batchSize);
    try (ZKWatcher customZkw = new ZKWatcher(localConf,
      "TestZKMulti_Custom", new ZKMultiAbortable(), true)) {

      // With a parent znode like this, we'll get batches of 2-3 elements
      final String parent1 = "/batchedDeletes1";
      final String parent2 = "/batchedDeletes2";
      final byte[] EMPTY_BYTES = new byte[0];

      // Write one node
      List<Op> ops = new ArrayList<>();
      ops.add(Op.create(parent1, EMPTY_BYTES, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
      for (int i = 0; i < batchSize * 2; i++) {
        ops.add(Op.create(
            parent1 + "/" + i, EMPTY_BYTES, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
      }
      customZkw.getRecoverableZooKeeper().multi(ops);

      // Write into a second node
      ops.clear();
      ops.add(Op.create(parent2, EMPTY_BYTES, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
      for (int i = 0; i < batchSize * 4; i++) {
        ops.add(Op.create(
            parent2 + "/" + i, EMPTY_BYTES, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
      }
      customZkw.getRecoverableZooKeeper().multi(ops);

      // These should return successfully
      ZKUtil.deleteChildrenRecursively(customZkw, parent1);
      ZKUtil.deleteChildrenRecursively(customZkw, parent2);
    }
  }

  @Test
  public void testListPartitioning() {
    // 10 Bytes
    ZKUtilOp tenByteOp = ZKUtilOp.deleteNodeFailSilent("/123456789");

    // Simple, single element case
    assertEquals(Collections.singletonList(Collections.singletonList(tenByteOp)),
        ZKUtil.partitionOps(Collections.singletonList(tenByteOp), 15));

    // Simple case where we exceed the limit, but must make the list
    assertEquals(Collections.singletonList(Collections.singletonList(tenByteOp)),
        ZKUtil.partitionOps(Collections.singletonList(tenByteOp), 5));

    // Each gets its own bucket
    assertEquals(
        Arrays.asList(Collections.singletonList(tenByteOp), Collections.singletonList(tenByteOp),
            Collections.singletonList(tenByteOp)),
        ZKUtil.partitionOps(Arrays.asList(tenByteOp, tenByteOp, tenByteOp), 15));

    // Test internal boundary
    assertEquals(
        Arrays.asList(Arrays.asList(tenByteOp,tenByteOp), Collections.singletonList(tenByteOp)),
        ZKUtil.partitionOps(Arrays.asList(tenByteOp, tenByteOp, tenByteOp), 20));

    // Plenty of space for one partition
    assertEquals(
        Collections.singletonList(Arrays.asList(tenByteOp, tenByteOp, tenByteOp)),
        ZKUtil.partitionOps(Arrays.asList(tenByteOp, tenByteOp, tenByteOp), 50));
  }

  private void createZNodeTree(String rootZNode) throws KeeperException,
      InterruptedException {
    List<Op> opList = new ArrayList<>();
    opList.add(Op.create(rootZNode, new byte[0], Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT));
    int level = 0;
    String parentZNode = rootZNode;
    while (level < 10) {
      // define parent node
      parentZNode = parentZNode + "/" + level;
      opList.add(Op.create(parentZNode, new byte[0], Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT));
      int elements = 0;
      // add elements to the parent node
      while (elements < level) {
        opList.add(Op.create(parentZNode + "/" + elements, new byte[0],
            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
        elements++;
      }
      level++;
    }
    zkw.getRecoverableZooKeeper().multi(opList);
  }
}
