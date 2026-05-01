/*
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
package org.apache.hadoop.hbase.io.hfile.cache;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Result of cache admission evaluation.
 */
@InterfaceAudience.Private
public final class AdmissionDecision {

  private static final AdmissionDecision ADMIT = new AdmissionDecision(true, "ADMIT");

  private static final AdmissionDecision REJECT = new AdmissionDecision(false, "REJECT");

  private final boolean admit;
  private final String reason;

  private AdmissionDecision(boolean admit, String reason) {
    this.admit = admit;
    this.reason = reason;
  }

  /**
   * Returns a normal-priority admission decision.
   * @return admit decision
   */
  public static AdmissionDecision admit() {
    return ADMIT;
  }

  /**
   * Returns an admission decision with a reason.
   * @param reason reason text
   * @return admit decision
   */
  public static AdmissionDecision admit(String reason) {
    return new AdmissionDecision(true, reason);
  }

  /**
   * Returns a rejection decision. * @return reject decision
   */
  public static AdmissionDecision reject() {
    return REJECT;
  }

  /**
   * Returns a rejection decision with a reason.
   * @param reason rejection reason
   * @return reject decision
   */
  public static AdmissionDecision reject(String reason) {
    return new AdmissionDecision(false, reason);
  }

  /**
   * Returns whether the block should be admitted.
   * @return true when admitted
   */
  public boolean isAdmitted() {
    return admit;
  }

  /**
   * Returns the decision reason.
   * @return decision reason
   */
  public String getReason() {
    return reason;
  }

}
