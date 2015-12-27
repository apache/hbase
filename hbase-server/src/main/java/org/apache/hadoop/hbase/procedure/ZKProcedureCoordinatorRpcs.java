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

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * ZooKeeper based {@link ProcedureCoordinatorRpcs} for a {@link ProcedureCoordinator}
 */
@InterfaceAudience.Private
public class ZKProcedureCoordinatorRpcs implements ProcedureCoordinatorRpcs {
  private static final Log LOG = LogFactory.getLog(ZKProcedureCoordinatorRpcs.class);
  private ZKProcedureUtil zkProc = null;
  protected ProcedureCoordinator coordinator = null;  // if started this should be non-null

  ZooKeeperWatcher watcher;
  String procedureType;
  String coordName;

  /**
   * @param watcher zookeeper watcher. Owned by <tt>this</tt> and closed via {@link #close()}
   * @param procedureClass procedure type name is a category for when there are multiple kinds of
   *    procedures.-- this becomes a znode so be aware of the naming restrictions
   * @param coordName name of the node running the coordinator
   * @throws KeeperException if an unexpected zk error occurs
   */
  public ZKProcedureCoordinatorRpcs(ZooKeeperWatcher watcher,
      String procedureClass, String coordName) throws IOException {
    this.watcher = watcher;
    this.procedureType = procedureClass;
    this.coordName = coordName;
  }

  /**
   * The "acquire" phase.  The coordinator creates a new procType/acquired/ znode dir. If znodes
   * appear, first acquire to relevant listener or sets watch waiting for notification of
   * the acquire node
   *
   * @param proc the Procedure
   * @param info data to be stored in the acquire node
   * @param nodeNames children of the acquire phase
   * @throws IOException if any failure occurs.
   */
  @Override
  final public void sendGlobalBarrierAcquire(Procedure proc, byte[] info, List<String> nodeNames)
      throws IOException, IllegalArgumentException {
    String procName = proc.getName();
    // start watching for the abort node
    String abortNode = zkProc.getAbortZNode(procName);
    try {
      // check to see if the abort node already exists
      if (ZKUtil.watchAndCheckExists(zkProc.getWatcher(), abortNode)) {
        abort(abortNode);
      }
      // If we get an abort node watch triggered here, we'll go complete creating the acquired
      // znode but then handle the acquire znode and bail out
    } catch (KeeperException e) {
      String msg = "Failed while watching abort node:" + abortNode;
      LOG.error(msg, e);
      throw new IOException(msg, e);
    }

    // create the acquire barrier
    String acquire = zkProc.getAcquiredBarrierNode(procName);
    LOG.debug("Creating acquire znode:" + acquire);
    try {
      // notify all the procedure listeners to look for the acquire node
      byte[] data = ProtobufUtil.prependPBMagic(info);
      ZKUtil.createWithParents(zkProc.getWatcher(), acquire, data);
      // loop through all the children of the acquire phase and watch for them
      for (String node : nodeNames) {
        String znode = ZKUtil.joinZNode(acquire, node);
        LOG.debug("Watching for acquire node:" + znode);
        if (ZKUtil.watchAndCheckExists(zkProc.getWatcher(), znode)) {
          coordinator.memberAcquiredBarrier(procName, node);
        }
      }
    } catch (KeeperException e) {
      String msg = "Failed while creating acquire node:" + acquire;
      LOG.error(msg, e);
      throw new IOException(msg, e);
    }
  }

  @Override
  public void sendGlobalBarrierReached(Procedure proc, List<String> nodeNames) throws IOException {
    String procName = proc.getName();
    String reachedNode = zkProc.getReachedBarrierNode(procName);
    LOG.debug("Creating reached barrier zk node:" + reachedNode);
    try {
      // create the reached znode and watch for the reached znodes
      ZKUtil.createWithParents(zkProc.getWatcher(), reachedNode);
      // loop through all the children of the acquire phase and watch for them
      for (String node : nodeNames) {
        String znode = ZKUtil.joinZNode(reachedNode, node);
        if (ZKUtil.watchAndCheckExists(zkProc.getWatcher(), znode)) {
          byte[] dataFromMember = ZKUtil.getData(zkProc.getWatcher(), znode);
          // ProtobufUtil.isPBMagicPrefix will check null
          if (dataFromMember != null && dataFromMember.length > 0) {
            if (!ProtobufUtil.isPBMagicPrefix(dataFromMember)) {
              String msg =
                "Failed to get data from finished node or data is illegally formatted: " + znode;
              LOG.error(msg);
              throw new IOException(msg);
            } else {
              dataFromMember = Arrays.copyOfRange(dataFromMember, ProtobufUtil.lengthOfPBMagic(),
                dataFromMember.length);
              coordinator.memberFinishedBarrier(procName, node, dataFromMember);
            }
          } else {
            coordinator.memberFinishedBarrier(procName, node, dataFromMember);
          }
        }
      }
    } catch (KeeperException e) {
      String msg = "Failed while creating reached node:" + reachedNode;
      LOG.error(msg, e);
      throw new IOException(msg, e);
    } catch (InterruptedException e) {
      String msg = "Interrupted while creating reached node:" + reachedNode;
      LOG.error(msg, e);
      throw new InterruptedIOException(msg);
    }
  }


  /**
   * Delete znodes that are no longer in use.
   */
  @Override
  final public void resetMembers(Procedure proc) throws IOException {
    String procName = proc.getName();
    boolean stillGettingNotifications = false;
    do {
      try {
        LOG.debug("Attempting to clean out zk node for op:" + procName);
        zkProc.clearZNodes(procName);
        stillGettingNotifications = false;
      } catch (KeeperException.NotEmptyException e) {
        // recursive delete isn't transactional (yet) so we need to deal with cases where we get
        // children trickling in
        stillGettingNotifications = true;
      } catch (KeeperException e) {
        String msg = "Failed to complete reset procedure " + procName;
        LOG.error(msg, e);
        throw new IOException(msg, e);
      }
    } while (stillGettingNotifications);
  }

  /**
   * Start monitoring znodes in ZK - subclass hook to start monitoring znodes they are about.
   * @return true if succeed, false if encountered initialization errors.
   */
  final public boolean start(final ProcedureCoordinator coordinator) {
    if (this.coordinator != null) {
      throw new IllegalStateException(
        "ZKProcedureCoordinator already started and already has listener installed");
    }
    this.coordinator = coordinator;

    try {
      this.zkProc = new ZKProcedureUtil(watcher, procedureType) {
        @Override
        public void nodeCreated(String path) {
          if (!isInProcedurePath(path)) return;
          LOG.debug("Node created: " + path);
          logZKTree(this.baseZNode);
          if (isAcquiredPathNode(path)) {
            // node wasn't present when we created the watch so zk event triggers acquire
            coordinator.memberAcquiredBarrier(ZKUtil.getNodeName(ZKUtil.getParent(path)),
              ZKUtil.getNodeName(path));
          } else if (isReachedPathNode(path)) {
            // node was absent when we created the watch so zk event triggers the finished barrier.

            // TODO Nothing enforces that acquire and reached znodes from showing up in wrong order.
            String procName = ZKUtil.getNodeName(ZKUtil.getParent(path));
            String member = ZKUtil.getNodeName(path);
            // get the data from the procedure member
            try {
              byte[] dataFromMember = ZKUtil.getData(watcher, path);
              // ProtobufUtil.isPBMagicPrefix will check null
              if (dataFromMember != null && dataFromMember.length > 0) {
                if (!ProtobufUtil.isPBMagicPrefix(dataFromMember)) {
                  ForeignException ee = new ForeignException(coordName,
                    "Failed to get data from finished node or data is illegally formatted:"
                        + path);
                  coordinator.abortProcedure(procName, ee);
                } else {
                  dataFromMember = Arrays.copyOfRange(dataFromMember, ProtobufUtil.lengthOfPBMagic(),
                    dataFromMember.length);
                  LOG.debug("Finished data from procedure '" + procName
                    + "' member '" + member + "': " + new String(dataFromMember));
                  coordinator.memberFinishedBarrier(procName, member, dataFromMember);
                }
              } else {
                coordinator.memberFinishedBarrier(procName, member, dataFromMember);
              }
            } catch (KeeperException e) {
              ForeignException ee = new ForeignException(coordName, e);
              coordinator.abortProcedure(procName, ee);
            } catch (InterruptedException e) {
              ForeignException ee = new ForeignException(coordName, e);
              coordinator.abortProcedure(procName, ee);
            }
          } else if (isAbortPathNode(path)) {
            abort(path);
          } else {
            LOG.debug("Ignoring created notification for node:" + path);
          }
        }
      };
      zkProc.clearChildZNodes();
    } catch (KeeperException e) {
      LOG.error("Unable to start the ZK-based Procedure Coordinator rpcs.", e);
      return false;
    }

    LOG.debug("Starting the controller for procedure member:" + coordName);
    return true;
  }

  /**
   * This is the abort message being sent by the coordinator to member
   *
   * TODO this code isn't actually used but can be used to issue a cancellation from the
   * coordinator.
   */
  @Override
  final public void sendAbortToMembers(Procedure proc, ForeignException ee) {
    String procName = proc.getName();
    LOG.debug("Aborting procedure '" + procName + "' in zk");
    String procAbortNode = zkProc.getAbortZNode(procName);
    try {
      LOG.debug("Creating abort znode:" + procAbortNode);
      String source = (ee.getSource() == null) ? coordName : ee.getSource();
      byte[] errorInfo = ProtobufUtil.prependPBMagic(ForeignException.serialize(source, ee));
      // first create the znode for the procedure
      ZKUtil.createAndFailSilent(zkProc.getWatcher(), procAbortNode, errorInfo);
      LOG.debug("Finished creating abort node:" + procAbortNode);
    } catch (KeeperException e) {
      // possible that we get this error for the procedure if we already reset the zk state, but in
      // that case we should still get an error for that procedure anyways
      zkProc.logZKTree(zkProc.baseZNode);
      coordinator.rpcConnectionFailure("Failed to post zk node:" + procAbortNode
          + " to abort procedure '" + procName + "'", new IOException(e));
    }
  }

  /**
   * Receive a notification and propagate it to the local coordinator
   * @param abortNode full znode path to the failed procedure information
   */
  protected void abort(String abortNode) {
    String procName = ZKUtil.getNodeName(abortNode);
    ForeignException ee = null;
    try {
      byte[] data = ZKUtil.getData(zkProc.getWatcher(), abortNode);
      if (data == null || data.length == 0) {
        // ignore
        return;
      } else if (!ProtobufUtil.isPBMagicPrefix(data)) {
        LOG.warn("Got an error notification for op:" + abortNode
            + " but we can't read the information. Killing the procedure.");
        // we got a remote exception, but we can't describe it
        ee = new ForeignException(coordName,
          "Data in abort node is illegally formatted.  ignoring content.");
      } else {

        data = Arrays.copyOfRange(data, ProtobufUtil.lengthOfPBMagic(), data.length);
        ee = ForeignException.deserialize(data);
      }
    } catch (InvalidProtocolBufferException e) {
      LOG.warn("Got an error notification for op:" + abortNode
          + " but we can't read the information. Killing the procedure.");
      // we got a remote exception, but we can't describe it
      ee = new ForeignException(coordName, e);
    } catch (KeeperException e) {
      coordinator.rpcConnectionFailure("Failed to get data for abort node:" + abortNode
          + zkProc.getAbortZnode(), new IOException(e));
    } catch (InterruptedException e) {
      coordinator.rpcConnectionFailure("Failed to get data for abort node:" + abortNode
          + zkProc.getAbortZnode(), new IOException(e));
      Thread.currentThread().interrupt();
    }
    coordinator.abortProcedure(procName, ee);
  }

  @Override
  final public void close() throws IOException {
    zkProc.close();
  }

  /**
   * Used in testing
   */
  final ZKProcedureUtil getZkProcedureUtil() {
    return zkProc;
  }
}
