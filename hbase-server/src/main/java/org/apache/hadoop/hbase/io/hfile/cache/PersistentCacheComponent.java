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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Defines persistence capability for a cache component.
 * <p>
 * Implementing this interface indicates that a cache component owns runtime state which can be
 * saved and restored across process restarts. Persistence is optional; cache components that do not
 * support persistence simply do not implement this interface.
 * </p>
 * <p>
 * A persistent component is constructed and configured normally before
 * {@link #restore(InputStream)} is invoked. Persistence restores runtime state into the existing
 * component instance and must not construct, configure, or replace the component.
 * </p>
 * <p>
 * Current configuration remains authoritative when persisted state is restored. Implementations are
 * responsible for applying their current configuration and runtime constraints to restored state.
 * </p>
 * <p>
 * Each component persists only state that it directly owns. A topology, for example, must not
 * recursively persist its cache engines, and a cache engine must not persist topology or placement
 * policy state.
 * </p>
 * <p>
 * Input and output streams are supplied and owned by the caller. Implementations must not close
 * either stream.
 * </p>
 */
@InterfaceAudience.Private
public interface PersistentCacheComponent {

  /**
   * Returns a stable identifier for this component's persistence format.
   * <p>
   * The identifier is used together with the component's role in the cache hierarchy to locate
   * persisted state. It must remain stable across process restarts and should remain unchanged
   * while the component remains compatible with previously persisted state.
   * </p>
   * <p>
   * The identifier describes the persistent component type or persistence format, not its placement
   * in a topology. For example, an engine should return an identifier such as
   * {@code bucket-cache-engine}, not {@code L2}.
   * </p>
   * <p>
   * The identifier must be non-empty and must not contain path separators.
   * </p>
   * @return stable persistence identifier
   */
  String getPersistenceId();

  /**
   * Restores persisted runtime state into this component.
   * <p>
   * The component must already be constructed and initialized using the current configuration.
   * Restored state is subject to the component's current configuration and constraints.
   * </p>
   * <p>
   * This method must not close the supplied input stream.
   * </p>
   * @param input input stream containing persisted component state
   * @throws IOException if persisted state cannot be read or restored
   */
  void restore(InputStream input) throws IOException;

  /**
   * Saves this component's runtime state to the supplied output stream.
   * <p>
   * Only state directly owned by this component should be written. Child cache components must not
   * be persisted recursively by this method.
   * </p>
   * <p>
   * This method must not close the supplied output stream.
   * </p>
   * @param output output stream to which component state is written
   * @throws IOException if component state cannot be saved
   */
  void save(OutputStream output) throws IOException;
}
