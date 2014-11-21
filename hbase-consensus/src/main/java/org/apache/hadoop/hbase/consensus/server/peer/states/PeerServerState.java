package org.apache.hadoop.hbase.consensus.server.peer.states;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.fsm.State;
import org.apache.hadoop.hbase.consensus.server.peer.PeerServerMutableContext;

public class PeerServerState extends State {
  private static Logger LOG = LoggerFactory.getLogger(PeerServerState.class);

  protected PeerServerMutableContext c;
  public PeerServerState(final PeerServerStateType t, PeerServerMutableContext c) {
    super(t);
    this.c = c;
  }

  @Override
  public void onEntry(final Event e) {}

  @Override
  public void onExit(final Event e) {}
}
