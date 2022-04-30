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
 * Has some basic interaction with the environment. Alternate implementations
 * can be used where required (eg in tests).
 *
 * @see EnvironmentEdgeManager
 */
@InterfaceAudience.Private
public interface EnvironmentEdge {

  /**
   * Returns the current time using the default clock.
   * <p>
   * This is almost always what you want, unless managing timekeeping with a named clock.
   *
   * @return The current time.
   */
  long currentTime();

  /**
   * Get the clock associated with the given identifier.
   * @param name clock identifier
   * @return the clock instance for the given identifier
   */
  Clock getClock(HashedBytes name);

  /**
   * Release the reference to and possible remove this clock.
   * @param clock the clock
   * @return true if the clock was removed, false if it did not exist
   */
  boolean removeClock(Clock clock);

  /**
   * Abstraction for an environment's time source.
   */
  public interface Clock {

    /**
     * Returns the clock's identifier.
     */
    HashedBytes getName();

    /**
     * Returns the current time using a named clock.
     * @return The current time, according to the given named clock.
     */
    long currentTime();

    /**
     * Returns the current time using a named clock. Ensure the clock advanced by
     * at least one tick before returning.
     * <p>
     * This method may block the current thread's execution or cause it to yield.
     * @return The current time, according to the given named clock.
     * @throws InterruptedException if interrupted while waiting for the clock to advance
     */
    default long currentTimeAdvancing() throws InterruptedException {
      throw new UnsupportedOperationException("BaseClock does not implement currentTimeAdvancing");
    }

    /**
     * Called to increment the reference count of the clock.
     */
    void get();

    /**
     * Called when the clock is removed.
     * @return true if the reference count is zero, false otherwise.
     */
    boolean remove();

  }

}
