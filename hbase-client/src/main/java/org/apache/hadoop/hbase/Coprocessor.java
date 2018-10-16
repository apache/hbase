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

package org.apache.hadoop.hbase;

import java.io.IOException;
import java.util.Collections;

import com.google.protobuf.Service;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Base interface for the 4 coprocessors - MasterCoprocessor, RegionCoprocessor,
 * RegionServerCoprocessor, and WALCoprocessor.
 * Do NOT implement this interface directly. Unless an implementation implements one (or more) of
 * the above mentioned 4 coprocessors, it'll fail to be loaded by any coprocessor host.
 *
 * Example:
 * Building a coprocessor to observe Master operations.
 * <pre>
 * class MyMasterCoprocessor implements MasterCoprocessor {
 *   &#64;Override
 *   public Optional&lt;MasterObserver> getMasterObserver() {
 *     return new MyMasterObserver();
 *   }
 * }
 *
 * class MyMasterObserver implements MasterObserver {
 *   ....
 * }
 * </pre>
 *
 * Building a Service which can be loaded by both Master and RegionServer
 * <pre>
 * class MyCoprocessorService implements MasterCoprocessor, RegionServerCoprocessor {
 *   &#64;Override
 *   public Optional&lt;Service> getServices() {
 *     return new ...;
 *   }
 * }
 * </pre>
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public interface Coprocessor {
  int VERSION = 1;

  /** Highest installation priority */
  int PRIORITY_HIGHEST = 0;
  /** High (system) installation priority */
  int PRIORITY_SYSTEM = Integer.MAX_VALUE / 4;
  /** Default installation priority for user coprocessors */
  int PRIORITY_USER = Integer.MAX_VALUE / 2;
  /** Lowest installation priority */
  int PRIORITY_LOWEST = Integer.MAX_VALUE;

  /**
   * Lifecycle state of a given coprocessor instance.
   */
  enum State {
    UNINSTALLED,
    INSTALLED,
    STARTING,
    ACTIVE,
    STOPPING,
    STOPPED
  }

  /**
   * Called by the {@link CoprocessorEnvironment} during it's own startup to initialize the
   * coprocessor.
   */
  default void start(CoprocessorEnvironment env) throws IOException {}

  /**
   * Called by the {@link CoprocessorEnvironment} during it's own shutdown to stop the
   * coprocessor.
   */
  default void stop(CoprocessorEnvironment env) throws IOException {}

  /**
   * Coprocessor endpoints providing protobuf services should override this method.
   * @return Iterable of {@link Service}s or empty collection. Implementations should never
   * return null.
   */
  default Iterable<Service> getServices() {
    return Collections.EMPTY_SET;
  }
}
