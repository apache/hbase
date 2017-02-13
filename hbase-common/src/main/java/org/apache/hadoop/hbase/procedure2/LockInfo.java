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

package org.apache.hadoop.hbase.procedure2;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.ProcedureInfo;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

@InterfaceAudience.Public
public class LockInfo {
  @InterfaceAudience.Public
  public enum ResourceType {
    SERVER, NAMESPACE, TABLE, REGION
  }

  @InterfaceAudience.Public
  public enum LockType {
    EXCLUSIVE, SHARED
  }

  @InterfaceAudience.Public
  public static class WaitingProcedure {
    private LockType lockType;
    private ProcedureInfo procedure;

    public WaitingProcedure() {
    }

    public LockType getLockType() {
      return lockType;
    }

    public void setLockType(LockType lockType) {
      this.lockType = lockType;
    }

    public ProcedureInfo getProcedure() {
      return procedure;
    }

    public void setProcedure(ProcedureInfo procedure) {
      this.procedure = procedure;
    }
  }

  private ResourceType resourceType;
  private String resourceName;
  private LockType lockType;
  private ProcedureInfo exclusiveLockOwnerProcedure;
  private int sharedLockCount;
  private final List<WaitingProcedure> waitingProcedures;

  public LockInfo() {
    waitingProcedures = new ArrayList<>();
  }

  public ResourceType getResourceType() {
    return resourceType;
  }

  public void setResourceType(ResourceType resourceType) {
    this.resourceType = resourceType;
  }

  public String getResourceName() {
    return resourceName;
  }

  public void setResourceName(String resourceName) {
    this.resourceName = resourceName;
  }

  public LockType getLockType() {
    return lockType;
  }

  public void setLockType(LockType lockType) {
    this.lockType = lockType;
  }

  public ProcedureInfo getExclusiveLockOwnerProcedure() {
    return exclusiveLockOwnerProcedure;
  }

  public void setExclusiveLockOwnerProcedure(
      ProcedureInfo exclusiveLockOwnerProcedure) {
    this.exclusiveLockOwnerProcedure = exclusiveLockOwnerProcedure;
  }

  public int getSharedLockCount() {
    return sharedLockCount;
  }

  public void setSharedLockCount(int sharedLockCount) {
    this.sharedLockCount = sharedLockCount;
  }

  public List<WaitingProcedure> getWaitingProcedures() {
    return waitingProcedures;
  }

  public void setWaitingProcedures(List<WaitingProcedure> waitingProcedures) {
    this.waitingProcedures.clear();
    this.waitingProcedures.addAll(waitingProcedures);
  }

  public void addWaitingProcedure(WaitingProcedure waitingProcedure) {
    waitingProcedures.add(waitingProcedure);
  }
}
