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

import java.util.List;

import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class LockedResource {
  private final LockedResourceType resourceType;
  private final String resourceName;
  private final LockType lockType;
  private final Procedure<?> exclusiveLockOwnerProcedure;
  private final int sharedLockCount;
  private final List<Procedure<?>> waitingProcedures;

  public LockedResource(LockedResourceType resourceType, String resourceName,
      LockType lockType, Procedure<?> exclusiveLockOwnerProcedure,
      int sharedLockCount, List<Procedure<?>> waitingProcedures) {
    this.resourceType = resourceType;
    this.resourceName = resourceName;
    this.lockType = lockType;
    this.exclusiveLockOwnerProcedure = exclusiveLockOwnerProcedure;
    this.sharedLockCount = sharedLockCount;
    this.waitingProcedures = waitingProcedures;
  }

  public LockedResourceType getResourceType() {
    return resourceType;
  }

  public String getResourceName() {
    return resourceName;
  }

  public LockType getLockType() {
    return lockType;
  }

  public Procedure<?> getExclusiveLockOwnerProcedure() {
    return exclusiveLockOwnerProcedure;
  }

  public int getSharedLockCount() {
    return sharedLockCount;
  }

  public List<Procedure<?>> getWaitingProcedures() {
    return waitingProcedures;
  }
}
