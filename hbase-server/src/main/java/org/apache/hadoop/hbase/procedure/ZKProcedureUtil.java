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
package org.apache.hadoop.hbase.procedure;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.zookeeper.ZKListener;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a shared ZooKeeper-based znode management utils for distributed procedure.  All znode
 * operations should go through the provided methods in coordinators and members.
 *
 * Layout of nodes in ZK is
 * /hbase/[op name]/acquired/
 *                    [op instance] - op data/
 *                        /[nodes that have acquired]
 *                 /reached/
 *                    [op instance]/
 *                        /[nodes that have completed]
 *                 /abort/
 *                    [op instance] - failure data
 *
 * NOTE: while acquired and completed are znode dirs, abort is actually just a znode.
 *
 * Assumption here that procedure names are unique
 */
@InterfaceAudience.Private
public abstract class ZKProcedureUtil
    extends ZKListener implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(ZKProcedureUtil.class);

  public static final String ACQUIRED_BARRIER_ZNODE_DEFAULT = "acquired";
  public static final String REACHED_BARRIER_ZNODE_DEFAULT = "reached";
  public static final String ABORT_ZNODE_DEFAULT = "abort";

  public final String baseZNode;
  protected final String acquiredZnode;
  protected final String reachedZnode;
  protected final String abortZnode;

  /**
   * Top-level watcher/controller for procedures across the cluster.
   * <p>
   * On instantiation, this ensures the procedure znodes exist.  This however requires the passed in
   *  watcher has been started.
   * @param watcher watcher for the cluster ZK. Owned by <tt>this</tt> and closed via
   *          {@link #close()}
   * @param procDescription name of the znode describing the procedure to run
   * @throws KeeperException when the procedure znodes cannot be created
   */
  public ZKProcedureUtil(ZKWatcher watcher, String procDescription)
      throws KeeperException {
    super(watcher);
    // make sure we are listening for events
    watcher.registerListener(this);
    // setup paths for the zknodes used in procedures
    this.baseZNode = ZNodePaths.joinZNode(watcher.getZNodePaths().baseZNode, procDescription);
    acquiredZnode = ZNodePaths.joinZNode(baseZNode, ACQUIRED_BARRIER_ZNODE_DEFAULT);
    reachedZnode = ZNodePaths.joinZNode(baseZNode, REACHED_BARRIER_ZNODE_DEFAULT);
    abortZnode = ZNodePaths.joinZNode(baseZNode, ABORT_ZNODE_DEFAULT);

    // first make sure all the ZK nodes exist
    // make sure all the parents exist (sometimes not the case in tests)
    ZKUtil.createWithParents(watcher, acquiredZnode);
    // regular create because all the parents exist
    ZKUtil.createAndFailSilent(watcher, reachedZnode);
    ZKUtil.createAndFailSilent(watcher, abortZnode);
  }

  @Override
  public void close() throws IOException {
    // the watcher is passed from either Master or Region Server
    // watcher.close() will be called by the owner so no need to call close() here
  }

  public String getAcquiredBarrierNode(String opInstanceName) {
    return ZKProcedureUtil.getAcquireBarrierNode(this, opInstanceName);
  }

  public String getReachedBarrierNode(String opInstanceName) {
    return ZKProcedureUtil.getReachedBarrierNode(this, opInstanceName);
  }

  public String getAbortZNode(String opInstanceName) {
    return ZKProcedureUtil.getAbortNode(this, opInstanceName);
  }

  public String getAbortZnode() {
    return abortZnode;
  }

  public String getBaseZnode() {
    return baseZNode;
  }

  public String getAcquiredBarrier() {
    return acquiredZnode;
  }

  /**
   * Get the full znode path for the node used by the coordinator to trigger a global barrier
   * acquire on each subprocedure.
   * @param controller controller running the procedure
   * @param opInstanceName name of the running procedure instance (not the procedure description).
   * @return full znode path to the prepare barrier/start node
   */
  public static String getAcquireBarrierNode(ZKProcedureUtil controller,
      String opInstanceName) {
    return ZNodePaths.joinZNode(controller.acquiredZnode, opInstanceName);
  }

  /**
   * Get the full znode path for the node used by the coordinator to trigger a global barrier
   * execution and release on each subprocedure.
   * @param controller controller running the procedure
   * @param opInstanceName name of the running procedure instance (not the procedure description).
   * @return full znode path to the commit barrier
   */
  public static String getReachedBarrierNode(ZKProcedureUtil controller,
      String opInstanceName) {
    return ZNodePaths.joinZNode(controller.reachedZnode, opInstanceName);
  }

  /**
   * Get the full znode path for the node used by the coordinator or member to trigger an abort
   * of the global barrier acquisition or execution in subprocedures.
   * @param controller controller running the procedure
   * @param opInstanceName name of the running procedure instance (not the procedure description).
   * @return full znode path to the abort znode
   */
  public static String getAbortNode(ZKProcedureUtil controller, String opInstanceName) {
    return ZNodePaths.joinZNode(controller.abortZnode, opInstanceName);
  }

  @Override
  public ZKWatcher getWatcher() {
    return watcher;
  }

  /**
   * Is this a procedure related znode path?
   *
   * TODO: this is not strict, can return true if had name just starts with same prefix but is
   * different zdir.
   *
   * @return true if starts with baseZnode
   */
  boolean isInProcedurePath(String path) {
    return path.startsWith(baseZNode);
  }

  /**
   * Is this the exact procedure barrier acquired znode
   */
  boolean isAcquiredNode(String path) {
    return path.equals(acquiredZnode);
  }


  /**
   * Is this in the procedure barrier acquired znode path
   */
  boolean isAcquiredPathNode(String path) {
    return path.startsWith(this.acquiredZnode) && !path.equals(acquiredZnode) &&
      isMemberNode(path, acquiredZnode);
  }

  /**
   * Is this the exact procedure barrier reached znode
   */
  boolean isReachedNode(String path) {
    return path.equals(reachedZnode);
  }

  /**
   * Is this in the procedure barrier reached znode path
   */
  boolean isReachedPathNode(String path) {
    return path.startsWith(this.reachedZnode) && !path.equals(reachedZnode) &&
      isMemberNode(path, reachedZnode);
  }

  /*
   * Returns true if the specified path is a member of the "statePath"
   *      /hbase/<ProcName>/<state>/<instance>/member
   *      |------ state path -----|
   *      |------------------ path ------------------|
   */
  private boolean isMemberNode(final String path, final String statePath) {
    int count = 0;
    for (int i = statePath.length(); i < path.length(); ++i) {
      count += (path.charAt(i) == ZNodePaths.ZNODE_PATH_SEPARATOR) ? 1 : 0;
    }
    return count == 2;
  }

  /**
   * Is this in the procedure barrier abort znode path
   */
  boolean isAbortNode(String path) {
    return path.equals(abortZnode);
  }

  /**
   * Is this in the procedure barrier abort znode path
   */
  public boolean isAbortPathNode(String path) {
    return path.startsWith(this.abortZnode) && !path.equals(abortZnode);
  }

  // --------------------------------------------------------------------------
  // internal debugging methods
  // --------------------------------------------------------------------------
  /**
   * Recursively print the current state of ZK (non-transactional)
   * @param root name of the root directory in zk to print
   * @throws KeeperException
   */
  void logZKTree(String root) {
    if (!LOG.isDebugEnabled()) return;
    LOG.debug("Current zk system:");
    String prefix = "|-";
    LOG.debug(prefix + root);
    try {
      logZKTree(root, prefix);
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Helper method to print the current state of the ZK tree.
   * @see #logZKTree(String)
   * @throws KeeperException if an unexpected exception occurs
   */
  protected void logZKTree(String root, String prefix) throws KeeperException {
    List<String> children = ZKUtil.listChildrenNoWatch(watcher, root);
    if (children == null) return;
    for (String child : children) {
      LOG.debug(prefix + child);
      String node = ZNodePaths.joinZNode(root.equals("/") ? "" : root, child);
      logZKTree(node, prefix + "---");
    }
  }

  public void clearChildZNodes() throws KeeperException {
    LOG.debug("Clearing all znodes {}, {}, {}", acquiredZnode, reachedZnode, abortZnode);

    // If the coordinator was shutdown mid-procedure, then we are going to lose
    // an procedure that was previously started by cleaning out all the previous state. Its much
    // harder to figure out how to keep an procedure going and the subject of HBASE-5487.
    ZKUtil.deleteChildrenRecursivelyMultiOrSequential(watcher, true, acquiredZnode, reachedZnode,
      abortZnode);

    if (LOG.isTraceEnabled()) {
      logZKTree(this.baseZNode);
    }
  }

  public void clearZNodes(String procedureName) throws KeeperException {
    LOG.info("Clearing all znodes for procedure " + procedureName + "including nodes "
        + acquiredZnode + " " + reachedZnode + " " + abortZnode);

    // Make sure we trigger the watches on these nodes by creating them. (HBASE-13885)
    String acquiredBarrierNode = getAcquiredBarrierNode(procedureName);
    String reachedBarrierNode = getReachedBarrierNode(procedureName);
    String abortZNode = getAbortZNode(procedureName);

    ZKUtil.createAndFailSilent(watcher, acquiredBarrierNode);
    ZKUtil.createAndFailSilent(watcher, abortZNode);

    ZKUtil.deleteNodeRecursivelyMultiOrSequential(watcher, true, acquiredBarrierNode,
      reachedBarrierNode, abortZNode);

    if (LOG.isTraceEnabled()) {
      logZKTree(this.baseZNode);
    }
  }
}
