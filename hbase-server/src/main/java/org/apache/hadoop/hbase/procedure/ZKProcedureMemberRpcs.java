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
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * ZooKeeper based controller for a procedure member.
 * <p>
 * There can only be one {@link ZKProcedureMemberRpcs} per procedure type per member,
 * since each procedure type is bound to a single set of znodes. You can have multiple
 * {@link ZKProcedureMemberRpcs} on the same server, each serving a different member
 * name, but each individual rpcs is still bound to a single member name (and since they are
 * used to determine global progress, its important to not get this wrong).
 * <p>
 * To make this slightly more confusing, you can run multiple, concurrent procedures at the same
 * time (as long as they have different types), from the same controller, but the same node name
 * must be used for each procedure (though there is no conflict between the two procedure as long
 * as they have distinct names).
 * <p>
 * There is no real error recovery with this mechanism currently -- if any the coordinator fails,
 * its re-initialization will delete the znodes and require all in progress subprocedures to start
 * anew.
 */
@InterfaceAudience.Private
public class ZKProcedureMemberRpcs implements ProcedureMemberRpcs {
  private static final Log LOG = LogFactory.getLog(ZKProcedureMemberRpcs.class);

  private final ZKProcedureUtil zkController;

  protected ProcedureMember member;
  private String memberName;

  /**
   * Must call {@link #start(String, ProcedureMember)} before this can be used.
   * @param watcher {@link ZooKeeperWatcher} to be owned by <tt>this</tt>. Closed via
   *          {@link #close()}.
   * @param procType name of the znode describing the procedure type
   * @throws KeeperException if we can't reach zookeeper
   */
  public ZKProcedureMemberRpcs(final ZooKeeperWatcher watcher, final String procType)
      throws KeeperException {
    this.zkController = new ZKProcedureUtil(watcher, procType) {
      @Override
      public void nodeCreated(String path) {
        if (!isInProcedurePath(path)) {
          return;
        }

        LOG.info("Received created event:" + path);
        // if it is a simple start/end/abort then we just rewatch the node
        if (isAcquiredNode(path)) {
          waitForNewProcedures();
          return;
        } else if (isAbortNode(path)) {
          watchForAbortedProcedures();
          return;
        }
        String parent = ZKUtil.getParent(path);
        // if its the end barrier, the procedure can be completed
        if (isReachedNode(parent)) {
          receivedReachedGlobalBarrier(path);
          return;
        } else if (isAbortNode(parent)) {
          abort(path);
          return;
        } else if (isAcquiredNode(parent)) {
          startNewSubprocedure(path);
        } else {
          LOG.debug("Ignoring created notification for node:" + path);
        }
      }

      @Override
      public void nodeChildrenChanged(String path) {
        if (path.equals(this.acquiredZnode)) {
          LOG.info("Received procedure start children changed event: " + path);
          waitForNewProcedures();
        } else if (path.equals(this.abortZnode)) {
          LOG.info("Received procedure abort children changed event: " + path);
          watchForAbortedProcedures();
        }
      }
    };
  }

  public ZKProcedureUtil getZkController() {
    return zkController;
  }

  @Override
  public String getMemberName() {
    return memberName;
  }

  /**
   * Pass along the procedure global barrier notification to any listeners
   * @param path full znode path that cause the notification
   */
  private void receivedReachedGlobalBarrier(String path) {
    LOG.debug("Recieved reached global barrier:" + path);
    String procName = ZKUtil.getNodeName(path);
    this.member.receivedReachedGlobalBarrier(procName);
  }

  private void watchForAbortedProcedures() {
    LOG.debug("Checking for aborted procedures on node: '" + zkController.getAbortZnode() + "'");
    try {
      // this is the list of the currently aborted procedues
      for (String node : ZKUtil.listChildrenAndWatchForNewChildren(zkController.getWatcher(),
        zkController.getAbortZnode())) {
        String abortNode = ZKUtil.joinZNode(zkController.getAbortZnode(), node);
        abort(abortNode);
      }
    } catch (KeeperException e) {
      member.controllerConnectionFailure("Failed to list children for abort node:"
          + zkController.getAbortZnode(), new IOException(e));
    }
  }

  private void waitForNewProcedures() {
    // watch for new procedues that we need to start subprocedures for
    LOG.debug("Looking for new procedures under znode:'" + zkController.getAcquiredBarrier() + "'");
    List<String> runningProcedures = null;
    try {
      runningProcedures = ZKUtil.listChildrenAndWatchForNewChildren(zkController.getWatcher(),
        zkController.getAcquiredBarrier());
      if (runningProcedures == null) {
        LOG.debug("No running procedures.");
        return;
      }
    } catch (KeeperException e) {
      member.controllerConnectionFailure("General failure when watching for new procedures",
        new IOException(e));
    }
    if (runningProcedures == null) {
      LOG.debug("No running procedures.");
      return;
    }
    for (String procName : runningProcedures) {
      // then read in the procedure information
      String path = ZKUtil.joinZNode(zkController.getAcquiredBarrier(), procName);
      startNewSubprocedure(path);
    }
  }

  /**
   * Kick off a new sub-procedure on the listener with the data stored in the passed znode.
   * <p>
   * Will attempt to create the same procedure multiple times if an procedure znode with the same
   * name is created. It is left up the coordinator to ensure this doesn't occur.
   * @param path full path to the znode for the procedure to start
   */
  private synchronized void startNewSubprocedure(String path) {
    LOG.debug("Found procedure znode: " + path);
    String opName = ZKUtil.getNodeName(path);
    // start watching for an abort notification for the procedure
    String abortZNode = zkController.getAbortZNode(opName);
    try {
      if (ZKUtil.watchAndCheckExists(zkController.getWatcher(), abortZNode)) {
        LOG.debug("Not starting:" + opName + " because we already have an abort notification.");
        return;
      }
    } catch (KeeperException e) {
      member.controllerConnectionFailure("Failed to get the abort znode (" + abortZNode
          + ") for procedure :" + opName, new IOException(e));
      return;
    }

    // get the data for the procedure
    Subprocedure subproc = null;
    try {
      byte[] data = ZKUtil.getData(zkController.getWatcher(), path);
      if (!ProtobufUtil.isPBMagicPrefix(data)) {
        String msg = "Data in for starting procuedure " + opName +
          " is illegally formatted (no pb magic). " +
          "Killing the procedure: " + Bytes.toString(data);
        LOG.error(msg);
        throw new IllegalArgumentException(msg);
      }
      LOG.debug("start proc data length is " + data.length);
      data = Arrays.copyOfRange(data, ProtobufUtil.lengthOfPBMagic(), data.length);
      LOG.debug("Found data for znode:" + path);
      subproc = member.createSubprocedure(opName, data);
      member.submitSubprocedure(subproc);
    } catch (IllegalArgumentException iae ) {
      LOG.error("Illegal argument exception", iae);
      sendMemberAborted(subproc, new ForeignException(getMemberName(), iae));
    } catch (IllegalStateException ise) {
      LOG.error("Illegal state exception ", ise);
      sendMemberAborted(subproc, new ForeignException(getMemberName(), ise));
    } catch (KeeperException e) {
      member.controllerConnectionFailure("Failed to get data for new procedure:" + opName,
        new IOException(e));
    }
  }

  /**
   * This attempts to create an acquired state znode for the procedure (snapshot name).
   *
   * It then looks for the reached znode to trigger in-barrier execution.  If not present we
   * have a watcher, if present then trigger the in-barrier action.
   */
  @Override
  public void sendMemberAcquired(Subprocedure sub) throws IOException {
    String procName = sub.getName();
    try {
      LOG.debug("Member: '" + memberName + "' joining acquired barrier for procedure (" + procName
          + ") in zk");
      String acquiredZNode = ZKUtil.joinZNode(ZKProcedureUtil.getAcquireBarrierNode(
        zkController, procName), memberName);
      ZKUtil.createAndFailSilent(zkController.getWatcher(), acquiredZNode);

      // watch for the complete node for this snapshot
      String reachedBarrier = zkController.getReachedBarrierNode(procName);
      LOG.debug("Watch for global barrier reached:" + reachedBarrier);
      if (ZKUtil.watchAndCheckExists(zkController.getWatcher(), reachedBarrier)) {
        receivedReachedGlobalBarrier(reachedBarrier);
      }
    } catch (KeeperException e) {
      member.controllerConnectionFailure("Failed to acquire barrier for procedure: "
          + procName + " and member: " + memberName, new IOException(e));
    }
  }

  /**
   * This acts as the ack for a completed snapshot
   */
  @Override
  public void sendMemberCompleted(Subprocedure sub) throws IOException {
    String procName = sub.getName();
    LOG.debug("Marking procedure  '" + procName + "' completed for member '" + memberName
        + "' in zk");
    String joinPath = ZKUtil.joinZNode(zkController.getReachedBarrierNode(procName), memberName);
    try {
      ZKUtil.createAndFailSilent(zkController.getWatcher(), joinPath);
    } catch (KeeperException e) {
      member.controllerConnectionFailure("Failed to post zk node:" + joinPath
          + " to join procedure barrier.", new IOException(e));
    }
  }

  /**
   * This should be called by the member and should write a serialized root cause exception as
   * to the abort znode.
   */
  @Override
  public void sendMemberAborted(Subprocedure sub, ForeignException ee) {
    if (sub == null) {
      LOG.error("Failed due to null subprocedure", ee);
      return;
    }
    String procName = sub.getName();
    LOG.debug("Aborting procedure (" + procName + ") in zk");
    String procAbortZNode = zkController.getAbortZNode(procName);
    try {
      String source = (ee.getSource() == null) ? memberName: ee.getSource();
      byte[] errorInfo = ProtobufUtil.prependPBMagic(ForeignException.serialize(source, ee));
      ZKUtil.createAndFailSilent(zkController.getWatcher(), procAbortZNode, errorInfo);
      LOG.debug("Finished creating abort znode:" + procAbortZNode);
    } catch (KeeperException e) {
      // possible that we get this error for the procedure if we already reset the zk state, but in
      // that case we should still get an error for that procedure anyways
      zkController.logZKTree(zkController.getBaseZnode());
      member.controllerConnectionFailure("Failed to post zk node:" + procAbortZNode
          + " to abort procedure", new IOException(e));
    }
  }

  /**
   * Pass along the found abort notification to the listener
   * @param abortZNode full znode path to the failed procedure information
   */
  protected void abort(String abortZNode) {
    LOG.debug("Aborting procedure member for znode " + abortZNode);
    String opName = ZKUtil.getNodeName(abortZNode);
    try {
      byte[] data = ZKUtil.getData(zkController.getWatcher(), abortZNode);

      // figure out the data we need to pass
      ForeignException ee;
      try {
        if (!ProtobufUtil.isPBMagicPrefix(data)) {
          String msg = "Illegally formatted data in abort node for proc " + opName
              + ".  Killing the procedure.";
          LOG.error(msg);
          // we got a remote exception, but we can't describe it so just return exn from here
          ee = new ForeignException(getMemberName(), new IllegalArgumentException(msg));
        } else {
          data = Arrays.copyOfRange(data, ProtobufUtil.lengthOfPBMagic(), data.length);
          ee = ForeignException.deserialize(data);
        }
      } catch (InvalidProtocolBufferException e) {
        LOG.warn("Got an error notification for op:" + opName
            + " but we can't read the information. Killing the procedure.");
        // we got a remote exception, but we can't describe it so just return exn from here
        ee = new ForeignException(getMemberName(), e);
      }

      this.member.receiveAbortProcedure(opName, ee);
    } catch (KeeperException e) {
      member.controllerConnectionFailure("Failed to get data for abort znode:" + abortZNode
          + zkController.getAbortZnode(), new IOException(e));
    }
  }

  public void start(final String memberName, final ProcedureMember listener) {
    LOG.debug("Starting procedure member '" + memberName + "'");
    this.member = listener;
    this.memberName = memberName;
    watchForAbortedProcedures();
    waitForNewProcedures();
  }

  @Override
  public void close() throws IOException {
    zkController.close();
  }

}
