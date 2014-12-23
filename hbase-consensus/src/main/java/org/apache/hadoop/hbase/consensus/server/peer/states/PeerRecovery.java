package org.apache.hadoop.hbase.consensus.server.peer.states;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.server.peer.PeerServerMutableContext;

public class PeerRecovery extends PeerServerState {
  private static Logger LOG = LoggerFactory.getLogger(PeerRecovery.class);

  public PeerRecovery(PeerServerMutableContext context) {
    super(PeerServerStateType.RECOVERY, context);
  }

  @Override
  public void onEntry(final Event e) {
    super.onEntry(e);
    // TODO
  }
}
