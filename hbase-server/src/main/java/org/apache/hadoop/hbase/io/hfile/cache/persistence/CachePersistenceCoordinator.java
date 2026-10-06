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
package org.apache.hadoop.hbase.io.hfile.cache.persistence;

import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import org.apache.hadoop.hbase.io.hfile.cache.CacheEngine;
import org.apache.hadoop.hbase.io.hfile.cache.CachePlacementAdmissionPolicy;
import org.apache.hadoop.hbase.io.hfile.cache.CacheTier;
import org.apache.hadoop.hbase.io.hfile.cache.CacheTopology;
import org.apache.hadoop.hbase.io.hfile.cache.PersistentCacheComponent;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Coordinates persistence of the components participating in a cache hierarchy.
 * <p>
 * Persistence capability is discovered dynamically. A cache topology, placement/admission policy,
 * or cache engine participates in persistence only when its concrete implementation implements
 * {@link PersistentCacheComponent}.
 * </p>
 * <p>
 * Components are persisted independently. In particular, a persistent topology is responsible only
 * for topology-owned state and must not persist its cache engines. The coordinator discovers and
 * persists each engine separately.
 * </p>
 * <p>
 * The coordinator assigns every persistent component a storage key composed from the component's
 * role in the cache hierarchy and its stable {@link PersistentCacheComponent#getPersistenceId()
 * persistence identifier}. This prevents state belonging to one engine implementation from being
 * restored into another implementation occupying the same cache tier after a configuration change.
 * </p>
 * <p>
 * All components must already have been constructed from the current configuration before
 * {@link #restore(CachePersistenceStorage)} is called. Restore modifies only the runtime state of
 * those existing component instances.
 * </p>
 */
@InterfaceAudience.Private
public final class CachePersistenceCoordinator {

  private static final String TOPOLOGY_ROLE = "topology";
  private static final String POLICY_ROLE = "policy";
  private static final String ENGINE_ROLE = "engine";

  private final CacheTopology topology;
  private final CachePlacementAdmissionPolicy policy;

  /**
   * Creates a persistence coordinator for the supplied cache topology and placement/admission
   * policy.
   * @param topology active cache topology
   * @param policy   active cache placement/admission policy
   */
  public CachePersistenceCoordinator(CacheTopology topology, CachePlacementAdmissionPolicy policy) {
    this.topology = Objects.requireNonNull(topology, "topology must not be null");
    this.policy = Objects.requireNonNull(policy, "policy must not be null");
  }

  /**
   * Saves the state of all persistent components participating in the cache hierarchy.
   * <p>
   * Components that do not implement {@link PersistentCacheComponent} are skipped. The topology,
   * placement/admission policy, and each cache engine are considered independently.
   * </p>
   * <p>
   * This method owns and closes all streams obtained from the supplied storage. Persistent cache
   * components must not close those streams themselves.
   * </p>
   * @param storage storage used to persist component state
   * @throws IOException if component state cannot be saved
   */
  public void save(CachePersistenceStorage storage) throws IOException {
    Objects.requireNonNull(storage, "storage must not be null");

    saveComponent(storage, TOPOLOGY_ROLE, topology);
    saveComponent(storage, POLICY_ROLE, policy);

    for (CacheTier tier : topology.getTiers()) {
      Optional<CacheEngine> engine = topology.getEngine(tier);
      if (engine.isPresent()) {
        saveComponent(storage, getEngineRole(tier), engine.get());
      }
    }
  }

  /**
   * Restores the state of all persistent components participating in the cache hierarchy.
   * <p>
   * Components that do not implement {@link PersistentCacheComponent} are skipped. Missing
   * persisted state is also ignored, allowing newly configured components to start without a
   * previous checkpoint.
   * </p>
   * <p>
   * Restore operates on the existing topology, policy, and engine instances. No component is
   * constructed or replaced by this method.
   * </p>
   * <p>
   * This method owns and closes all streams obtained from the supplied storage. Persistent cache
   * components must not close those streams themselves.
   * </p>
   * @param storage storage from which persisted component state is restored
   * @throws IOException if persisted component state exists but cannot be restored
   */
  public void restore(CachePersistenceStorage storage) throws IOException {
    Objects.requireNonNull(storage, "storage must not be null");

    restoreComponent(storage, TOPOLOGY_ROLE, topology);
    restoreComponent(storage, POLICY_ROLE, policy);

    for (CacheTier tier : topology.getTiers()) {
      Optional<CacheEngine> engine = topology.getEngine(tier);
      if (engine.isPresent()) {
        restoreComponent(storage, getEngineRole(tier), engine.get());
      }
    }
  }

  /**
   * Saves a component when that component implements {@link PersistentCacheComponent}.
   * @param storage   persistence storage
   * @param role      component role within the cache hierarchy
   * @param component component to inspect and persist
   * @throws IOException if component state cannot be saved
   */
  private void saveComponent(CachePersistenceStorage storage, String role, Object component)
    throws IOException {
    if (!(component instanceof PersistentCacheComponent)) {
      return;
    }

    PersistentCacheComponent persistent = (PersistentCacheComponent) component;
    String key = getStorageKey(role, persistent);

    try (CachePersistenceOutput output = storage.create(key)) {
      persistent.save(output.getOutputStream());
      output.commit();
    }
  }

  /**
   * Restores a component when that component implements {@link PersistentCacheComponent} and
   * persisted state exists for it.
   * @param storage   persistence storage
   * @param role      component role within the cache hierarchy
   * @param component component to inspect and restore
   * @throws IOException if persisted state exists but cannot be restored
   */
  private void restoreComponent(CachePersistenceStorage storage, String role, Object component)
    throws IOException {
    if (!(component instanceof PersistentCacheComponent)) {
      return;
    }

    PersistentCacheComponent persistent = (PersistentCacheComponent) component;
    String key = getStorageKey(role, persistent);
    Optional<InputStream> input = storage.open(key);

    if (!input.isPresent()) {
      return;
    }

    try (InputStream stream = input.get()) {
      persistent.restore(stream);
    }
  }

  /**
   * Builds the storage key for a persistent component.
   * @param role      component role within the cache hierarchy
   * @param component persistent component
   * @return stable storage key for the component
   */
  private String getStorageKey(String role, PersistentCacheComponent component) {
    String persistenceId =
      Objects.requireNonNull(component.getPersistenceId(), "persistenceId must not be null");

    validatePersistenceId(persistenceId);
    return role + "/" + persistenceId;
  }

  /**
   * Returns the persistence role assigned to an engine in the specified cache tier.
   * @param tier cache tier occupied by the engine
   * @return persistence role for the engine
   */
  private String getEngineRole(CacheTier tier) {
    Objects.requireNonNull(tier, "tier must not be null");
    return ENGINE_ROLE + "/" + tier.name().toLowerCase(Locale.ROOT);
  }

  /**
   * Validates a component persistence identifier.
   * <p>
   * Component persistence identifiers are single path components. Placement information such as
   * topology tier is supplied separately by the coordinator and therefore must not be encoded in
   * the component identifier.
   * </p>
   * @param persistenceId persistence identifier to validate
   * @throws IllegalArgumentException if the identifier is empty or contains a path separator
   */
  private void validatePersistenceId(String persistenceId) {
    if (persistenceId.isEmpty()) {
      throw new IllegalArgumentException("persistenceId must not be empty");
    }
    if (persistenceId.indexOf('/') >= 0 || persistenceId.indexOf('\\') >= 0) {
      throw new IllegalArgumentException(
        "persistenceId must not contain a path separator: " + persistenceId);
    }
  }
}
