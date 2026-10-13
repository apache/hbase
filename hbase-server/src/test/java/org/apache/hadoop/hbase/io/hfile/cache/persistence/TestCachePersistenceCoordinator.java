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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.hbase.io.hfile.cache.CacheEngine;
import org.apache.hadoop.hbase.io.hfile.cache.CachePlacementAdmissionPolicy;
import org.apache.hadoop.hbase.io.hfile.cache.CacheTier;
import org.apache.hadoop.hbase.io.hfile.cache.CacheTopology;
import org.apache.hadoop.hbase.io.hfile.cache.PersistentCacheComponent;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(IOTests.TAG)
@Tag(SmallTests.TAG)
public class TestCachePersistenceCoordinator {

  /**
   * Verifies that non-persistent topology, policy, and cache engines are skipped.
   * @throws IOException if persistence unexpectedly fails
   */
  @Test
  public void testNonPersistentComponentsAreSkipped() throws IOException {
    CacheTopology topology = mock(CacheTopology.class);
    CachePlacementAdmissionPolicy policy = mock(CachePlacementAdmissionPolicy.class);
    CacheEngine engine = mock(CacheEngine.class);
    CachePersistenceStorage storage = mock(CachePersistenceStorage.class);

    when(topology.getTiers()).thenReturn(List.of(CacheTier.SINGLE));
    when(topology.getEngine(CacheTier.SINGLE)).thenReturn(Optional.of(engine));

    CachePersistenceCoordinator coordinator = new CachePersistenceCoordinator(topology, policy);

    coordinator.save(storage);
    coordinator.restore(storage);

    verifyNoInteractions(storage);
  }

  /**
   * Verifies that a persistent engine in a single-tier topology is saved using its engine role and
   * persistence identifier.
   * @throws IOException if persistence fails
   */
  @Test
  public void testPersistentSingleTierEngineIsSaved() throws IOException {
    CacheTopology topology = mock(CacheTopology.class);
    CachePlacementAdmissionPolicy policy = mock(CachePlacementAdmissionPolicy.class);
    CacheEngine engine = createPersistentEngine("test-engine");
    PersistentCacheComponent persistentEngine = (PersistentCacheComponent) engine;
    CachePersistenceStorage storage = mock(CachePersistenceStorage.class);
    CachePersistenceOutput persistenceOutput = mock(CachePersistenceOutput.class);
    OutputStream output = mock(OutputStream.class);

    when(topology.getTiers()).thenReturn(List.of(CacheTier.SINGLE));
    when(topology.getEngine(CacheTier.SINGLE)).thenReturn(Optional.of(engine));
    when(storage.create("engine/single/test-engine")).thenReturn(persistenceOutput);
    when(persistenceOutput.getOutputStream()).thenReturn(output);

    CachePersistenceCoordinator coordinator = new CachePersistenceCoordinator(topology, policy);

    coordinator.save(storage);

    verify(storage).create("engine/single/test-engine");
    verify(persistentEngine).save(output);
    verify(persistenceOutput).commit();
    verify(persistenceOutput).close();
  }

  /**
   * Verifies that persisted state is restored into the existing engine instance.
   * @throws IOException if restoration fails
   */
  @Test
  public void testPersistentSingleTierEngineIsRestored() throws IOException {
    CacheTopology topology = mock(CacheTopology.class);
    CachePlacementAdmissionPolicy policy = mock(CachePlacementAdmissionPolicy.class);
    CacheEngine engine = createPersistentEngine("test-engine");
    PersistentCacheComponent persistentEngine = (PersistentCacheComponent) engine;
    CachePersistenceStorage storage = mock(CachePersistenceStorage.class);
    InputStream input = mock(InputStream.class);

    when(topology.getTiers()).thenReturn(List.of(CacheTier.SINGLE));
    when(topology.getEngine(CacheTier.SINGLE)).thenReturn(Optional.of(engine));
    when(storage.open("engine/single/test-engine")).thenReturn(Optional.of(input));

    CachePersistenceCoordinator coordinator = new CachePersistenceCoordinator(topology, policy);

    coordinator.restore(storage);

    verify(storage).open("engine/single/test-engine");
    verify(persistentEngine).restore(input);
    verify(input).close();
  }

  /**
   * Verifies that absence of previously persisted state is not treated as an error.
   * @throws IOException if restoration unexpectedly fails
   */
  @Test
  public void testMissingPersistentStateIsIgnored() throws IOException {
    CacheTopology topology = mock(CacheTopology.class);
    CachePlacementAdmissionPolicy policy = mock(CachePlacementAdmissionPolicy.class);
    CacheEngine engine = createPersistentEngine("test-engine");
    PersistentCacheComponent persistentEngine = (PersistentCacheComponent) engine;
    CachePersistenceStorage storage = mock(CachePersistenceStorage.class);

    when(topology.getTiers()).thenReturn(List.of(CacheTier.SINGLE));
    when(topology.getEngine(CacheTier.SINGLE)).thenReturn(Optional.of(engine));
    when(storage.open("engine/single/test-engine")).thenReturn(Optional.empty());

    CachePersistenceCoordinator coordinator = new CachePersistenceCoordinator(topology, policy);

    coordinator.restore(storage);

    verify(storage).open("engine/single/test-engine");
    verify(persistentEngine, never()).restore(org.mockito.ArgumentMatchers.any());
  }

  /**
   * Verifies that topology, policy, and cache engine persistence are coordinated independently.
   * <p>
   * The L1 engine deliberately does not support persistence, while the L2 engine does.
   * </p>
   * @throws IOException if persistence fails
   */
  @Test
  public void testTieredComponentsArePersistedIndependently() throws IOException {
    CacheTopology topology = createPersistentTopology("test-topology");
    CachePlacementAdmissionPolicy policy = createPersistentPolicy("test-policy");
    CacheEngine l1 = mock(CacheEngine.class);
    CacheEngine l2 = createPersistentEngine("test-l2");

    PersistentCacheComponent persistentTopology = (PersistentCacheComponent) topology;
    PersistentCacheComponent persistentPolicy = (PersistentCacheComponent) policy;
    PersistentCacheComponent persistentL2 = (PersistentCacheComponent) l2;

    CachePersistenceStorage storage = mock(CachePersistenceStorage.class);

    CachePersistenceOutput topologyOutput = mock(CachePersistenceOutput.class);
    CachePersistenceOutput policyOutput = mock(CachePersistenceOutput.class);
    CachePersistenceOutput l2Output = mock(CachePersistenceOutput.class);

    OutputStream topologyStream = mock(OutputStream.class);
    OutputStream policyStream = mock(OutputStream.class);
    OutputStream l2Stream = mock(OutputStream.class);

    when(topology.getTiers()).thenReturn(List.of(CacheTier.L1, CacheTier.L2));
    when(topology.getEngine(CacheTier.L1)).thenReturn(Optional.of(l1));
    when(topology.getEngine(CacheTier.L2)).thenReturn(Optional.of(l2));

    when(storage.create("topology/test-topology")).thenReturn(topologyOutput);
    when(storage.create("policy/test-policy")).thenReturn(policyOutput);
    when(storage.create("engine/l2/test-l2")).thenReturn(l2Output);

    when(topologyOutput.getOutputStream()).thenReturn(topologyStream);
    when(policyOutput.getOutputStream()).thenReturn(policyStream);
    when(l2Output.getOutputStream()).thenReturn(l2Stream);

    CachePersistenceCoordinator coordinator = new CachePersistenceCoordinator(topology, policy);

    coordinator.save(storage);

    verify(persistentTopology).save(topologyStream);
    verify(persistentPolicy).save(policyStream);
    verify(persistentL2).save(l2Stream);

    verify(topologyOutput).commit();
    verify(policyOutput).commit();
    verify(l2Output).commit();

    verify(storage, never()).create(org.mockito.ArgumentMatchers.startsWith("engine/l1/"));
  }

  /**
   * Verifies that a component save failure does not commit incomplete persisted state.
   * @throws IOException if mock setup unexpectedly fails
   */
  @Test
  public void testSaveFailureDoesNotCommit() throws IOException {
    CacheTopology topology = mock(CacheTopology.class);
    CachePlacementAdmissionPolicy policy = mock(CachePlacementAdmissionPolicy.class);
    CacheEngine engine = createPersistentEngine("test-engine");
    PersistentCacheComponent persistentEngine = (PersistentCacheComponent) engine;
    CachePersistenceStorage storage = mock(CachePersistenceStorage.class);
    CachePersistenceOutput persistenceOutput = mock(CachePersistenceOutput.class);
    OutputStream output = mock(OutputStream.class);

    when(topology.getTiers()).thenReturn(List.of(CacheTier.SINGLE));
    when(topology.getEngine(CacheTier.SINGLE)).thenReturn(Optional.of(engine));
    when(storage.create("engine/single/test-engine")).thenReturn(persistenceOutput);
    when(persistenceOutput.getOutputStream()).thenReturn(output);

    IOException failure = new IOException("Expected persistence failure");
    org.mockito.Mockito.doThrow(failure).when(persistentEngine).save(output);

    CachePersistenceCoordinator coordinator = new CachePersistenceCoordinator(topology, policy);

    assertThrows(IOException.class, () -> coordinator.save(storage));

    verify(persistenceOutput, never()).commit();
    verify(persistenceOutput).close();
  }

  /**
   * Verifies that invalid persistence identifiers are rejected before storage is accessed.
   */
  @Test
  public void testInvalidPersistenceIdIsRejected() {
    CacheTopology topology = mock(CacheTopology.class);
    CachePlacementAdmissionPolicy policy = mock(CachePlacementAdmissionPolicy.class);
    CacheEngine engine = createPersistentEngine("../invalid");
    CachePersistenceStorage storage = mock(CachePersistenceStorage.class);

    when(topology.getTiers()).thenReturn(List.of(CacheTier.SINGLE));
    when(topology.getEngine(CacheTier.SINGLE)).thenReturn(Optional.of(engine));

    CachePersistenceCoordinator coordinator = new CachePersistenceCoordinator(topology, policy);

    assertThrows(IllegalArgumentException.class, () -> coordinator.save(storage));

    verifyNoInteractions(storage);
  }

  /**
   * Creates a cache engine mock that also implements {@link PersistentCacheComponent}.
   * @param persistenceId persistence identifier returned by the mock
   * @return persistent cache engine mock
   */
  private CacheEngine createPersistentEngine(String persistenceId) {
    CacheEngine engine =
      mock(CacheEngine.class, withSettings().extraInterfaces(PersistentCacheComponent.class));
    PersistentCacheComponent persistent = (PersistentCacheComponent) engine;

    when(persistent.getPersistenceId()).thenReturn(persistenceId);
    return engine;
  }

  /**
   * Creates a cache topology mock that also implements {@link PersistentCacheComponent}.
   * @param persistenceId persistence identifier returned by the mock
   * @return persistent cache topology mock
   */
  private CacheTopology createPersistentTopology(String persistenceId) {
    CacheTopology topology =
      mock(CacheTopology.class, withSettings().extraInterfaces(PersistentCacheComponent.class));
    PersistentCacheComponent persistent = (PersistentCacheComponent) topology;

    when(persistent.getPersistenceId()).thenReturn(persistenceId);
    return topology;
  }

  /**
   * Creates a placement/admission policy mock that also implements
   * {@link PersistentCacheComponent}.
   * @param persistenceId persistence identifier returned by the mock
   * @return persistent placement/admission policy mock
   */
  private CachePlacementAdmissionPolicy createPersistentPolicy(String persistenceId) {
    CachePlacementAdmissionPolicy policy = mock(CachePlacementAdmissionPolicy.class,
      withSettings().extraInterfaces(PersistentCacheComponent.class));
    PersistentCacheComponent persistent = (PersistentCacheComponent) policy;

    when(persistent.getPersistenceId()).thenReturn(persistenceId);
    return policy;
  }
}
