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
package org.apache.hadoop.hbase.util;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * An environment edge that uses a manually set value. This is useful for testing events that are supposed to
 * happen in the same millisecond.
 */
@InterfaceAudience.Private
public class ManualEnvironmentEdge extends BaseEnvironmentEdge {

  class ManualFixedClock implements EnvironmentEdge.Clock {

    private HashedBytes name;

    public ManualFixedClock(HashedBytes name) {
      this.name = name;
    }

    @Override
    public HashedBytes getName() {
      return name;
    }

    @Override
    public long currentTime() {
      return value;
    }

    @Override
    public long currentTimeAdvancing() {
      return value;
    }

    @Override
    public void get() {
    }

    @Override
    public boolean remove() {
      return true;
    }

  }

  protected long value;

  public ManualEnvironmentEdge() {
    // Sometimes 0 ts might have a special value, so lets start with 1
    this(1L);
  }

  public ManualEnvironmentEdge(long value) {
    this.value = value;
  }

  public void setValue(long newValue) {
    value = newValue;
  }

  public void incValue(long addedValue) {
    value += addedValue;
  }

  @Override
  public long currentTime() {
    return this.value;
  }

  @Override
  public Clock getClock(HashedBytes name) {
    return new ManualFixedClock(name);
  }

  @Override
  public boolean removeClock(Clock clock) {
    return true;
  }

}
