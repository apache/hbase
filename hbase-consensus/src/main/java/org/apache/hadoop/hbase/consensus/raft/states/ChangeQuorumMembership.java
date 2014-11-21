package org.apache.hadoop.hbase.consensus.raft.states;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.quorum.MutableRaftContext;
import org.apache.hadoop.hbase.consensus.quorum.QuorumInfo;
import org.apache.hadoop.hbase.consensus.quorum.QuorumMembershipChangeRequest;
import org.apache.hadoop.hbase.consensus.quorum.QuorumMembershipChangeRequest.QuorumMembershipChangeState;
import org.apache.hadoop.hbase.consensus.raft.events.ReplicateEntriesEvent;
import org.apache.hadoop.hbase.regionserver.RaftEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

public class ChangeQuorumMembership extends RaftAsyncState {
  private static Logger LOG = LoggerFactory.getLogger(Leader.class);
  private SettableFuture<?> changeQuorumMembershipFuture;

  public ChangeQuorumMembership(MutableRaftContext context) {
    super(RaftStateType.CHANGE_QUORUM_MEMBERSHIP, context);
  }

  @Override
  public boolean isComplete() {
    return changeQuorumMembershipFuture == null || changeQuorumMembershipFuture.isDone();
  }

  @Override
  public ListenableFuture<?> getAsyncCompletion() {
    return changeQuorumMembershipFuture;
  }

  @Override
  public void onEntry(final Event e) {
    super.onEntry(e);
    if (changeQuorumMembershipFuture != null) {
      assert changeQuorumMembershipFuture.isDone();
    }
    changeQuorumMembershipFuture = null;
    final QuorumMembershipChangeRequest request = c.getUpdateMembershipRequest();

    if (request == null) {
      LOG.warn("We should not be in this state if there is no membership change" +
        " in progress. ");
      return;
    }

    QuorumMembershipChangeRequest.QuorumMembershipChangeState currState =
      request.getCurrentState();

    if (currState == QuorumMembershipChangeRequest.QuorumMembershipChangeState.PENDING) {
      changeQuorumMembershipFuture = SettableFuture.create();

      LOG.debug("Updating the quorum " + c.getQuorumInfo().getQuorumName()
          + " to a joint quorum membership.");
      final List<QuorumInfo> configs = Arrays.asList(c.getQuorumInfo(), request.getConfig());
      // We need to send the new configuration to the quorum. And move to joint
      // membership state.

      try {
        c.updateToJointQuorumMembership(configs.get(1));
      } catch (IOException ioe) {
        LOG.error("Cannot upgrade to joint quorum membership. Will retry. " +
          "New config: " + configs.get(1) + " Error :", ioe);
      }

      // Send the config change entry
      ListenableFuture<Void> sendAppendReqFuture =
          (ListenableFuture<Void>) c.sendAppendRequest(new ReplicateEntriesEvent(false, QuorumInfo
              .serializeToBuffer(configs)));
      Futures.addCallback(sendAppendReqFuture, new FutureCallback<Void>() {

        @Override
        public void onSuccess(Void result) {
          request.setCurrentState(QuorumMembershipChangeState.JOINT_CONFIG_COMMIT_IN_PROGRESS);
          changeQuorumMembershipFuture.set(null);
        }

        @Override
        public void onFailure(Throwable t) {
          if (t instanceof InterruptedException) {
            LOG.info("Exception while sending append request to update configuration", t);
            Thread.currentThread().interrupt();
          } else if (t instanceof ExecutionException) {
            LOG.info("Exception while sending append request to update configuration", t);
          }
          changeQuorumMembershipFuture.set(null);
        }

      });
    } else if (currState == QuorumMembershipChangeRequest.QuorumMembershipChangeState.JOINT_CONFIG_COMMIT_IN_PROGRESS) {
      changeQuorumMembershipFuture = SettableFuture.create();
      LOG.debug("Updating the quorum " + c.getQuorumInfo().getQuorumName()
          + " to the new quorum membership.");

      // Send the new config now to both old and new set of peers.
      ListenableFuture<Void> sendAppendReqFuture =
          (ListenableFuture<Void>) c.sendAppendRequest(new ReplicateEntriesEvent(false, QuorumInfo
              .serializeToBuffer(Arrays.asList(request.getConfig()))));

      Futures.addCallback(sendAppendReqFuture, new FutureCallback<Void>() {

        @Override
        public void onSuccess(Void result) {
          LOG.info("Sent the append entries request");
          try {
            c.updateToNewQuorumMembership(request.getConfig());
          } catch (IOException e) {
            LOG.error("Cannot upgrade to new membership. Will retry. New config: "
                + request.getConfig() + " Error: " + e);
          }
          request.setCurrentState(QuorumMembershipChangeState.NEW_CONFIG_COMMIT_IN_PROGRESS);
          changeQuorumMembershipFuture.set(null);
        }

        @Override
        public void onFailure(Throwable t) {

          if (t instanceof InterruptedException) {
            LOG.info("Exception while sending append request to update configuration", t);
            Thread.currentThread().interrupt();
          } else if (t instanceof ExecutionException) {
            LOG.info("Exception while sending append request to update configuration", t);
          }
          try {
            c.updateToNewQuorumMembership(request.getConfig());
          } catch (IOException e) {
            LOG.error("Cannot upgrade to new membership. Will retry. New config: "
                + request.getConfig() + " Error: " + e);
          }
          changeQuorumMembershipFuture.set(null);
        }
      });

    } else if (currState ==
      QuorumMembershipChangeRequest.QuorumMembershipChangeState.NEW_CONFIG_COMMIT_IN_PROGRESS) {
      LOG.debug("Updating the quorum " + c.getQuorumInfo().getQuorumName() +
        " to the new quorum membership by setting the quorum membership " +
        " change request to COMPLETE.");
      request.setResponse(true);
      // We have completed the quorum membership change, lets go back to normal
      c.setUpdateMembershipRequest(null);
      c.cleanUpJointStates();

      request.setCurrentState(
        QuorumMembershipChangeRequest.QuorumMembershipChangeState.COMPLETE);

      // The leader is no longer the part of the quorum. HALT
      if (c.getQuorumInfo().getPeersWithRank().get(c.getServerAddress()) == null) {
        c.stop(false);

        RaftEventListener eventListener = c.getDataStoreEventListener();
        if (eventListener != null) {
          eventListener.closeDataStore();
        }
      }
    }
  }
}
