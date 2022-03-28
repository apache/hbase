/*
 *
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

package org.apache.hadoop.hbase.namequeues.request;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.hbase.namequeues.NamedQueuePayload;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Request object to be used by ring buffer use-cases. Clients get records by sending
 * this request object.
 * For each ring buffer use-case, add request payload to this class, client should set
 * namedQueueEvent based on use-case.
 * Protobuf does not support inheritance, hence we need to work with
 */
@InterfaceAudience.Private
public class NamedQueueGetRequest {

  private AdminProtos.SlowLogResponseRequest slowLogResponseRequest;
  private NamedQueuePayload.NamedQueueEvent namedQueueEvent;
  private MasterProtos.BalancerDecisionsRequest balancerDecisionsRequest;
  private MasterProtos.BalancerRejectionsRequest balancerRejectionsRequest;

  public AdminProtos.SlowLogResponseRequest getSlowLogResponseRequest() {
    return slowLogResponseRequest;
  }

  public void setSlowLogResponseRequest(
      AdminProtos.SlowLogResponseRequest slowLogResponseRequest) {
    this.slowLogResponseRequest = slowLogResponseRequest;
  }

  public MasterProtos.BalancerDecisionsRequest getBalancerDecisionsRequest() {
    return balancerDecisionsRequest;
  }

  public MasterProtos.BalancerRejectionsRequest getBalancerRejectionsRequest() {
    return balancerRejectionsRequest;
  }

  public void setBalancerDecisionsRequest(
      MasterProtos.BalancerDecisionsRequest balancerDecisionsRequest) {
    this.balancerDecisionsRequest = balancerDecisionsRequest;
  }

  public void setBalancerRejectionsRequest(
    MasterProtos.BalancerRejectionsRequest balancerRejectionsRequest) {
    this.balancerRejectionsRequest = balancerRejectionsRequest;
  }

  public NamedQueuePayload.NamedQueueEvent getNamedQueueEvent() {
    return namedQueueEvent;
  }

  public void setNamedQueueEvent(int eventOrdinal) {
    this.namedQueueEvent = NamedQueuePayload.NamedQueueEvent.getEventByOrdinal(eventOrdinal);
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
      .append("slowLogResponseRequest", slowLogResponseRequest)
      .append("namedQueueEvent", namedQueueEvent)
      .append("balancerDecisionsRequest", balancerDecisionsRequest)
      .append("balancerRejectionsRequest", balancerRejectionsRequest)
      .toString();
  }
}
