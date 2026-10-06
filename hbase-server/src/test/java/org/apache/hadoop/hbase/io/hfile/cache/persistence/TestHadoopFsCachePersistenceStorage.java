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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.cache.CacheEngine;
import org.apache.hadoop.hbase.io.hfile.cache.CachePlacementAdmissionPolicy;
import org.apache.hadoop.hbase.io.hfile.cache.CacheTier;
import org.apache.hadoop.hbase.io.hfile.cache.CacheTopology;
import org.apache.hadoop.hbase.io.hfile.cache.PersistentCacheComponent;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Tag(IOTests.TAG)
@Tag(SmallTests.TAG)
public class TestHadoopFsCachePersistenceStorage {

  private static final String KEY = "engine/l2/bucket-cache-engine";

  @TempDir
  private java.nio.file.Path temporaryDirectory;

  private FileSystem fileSystem;
  private Path rootPath;
  private HadoopFsCachePersistenceStorage storage;

  /**
   * Creates an isolated Hadoop local filesystem instance and persistence root for each test.
   * @throws IOException if the local filesystem cannot be created
   */
  @BeforeEach
  public void setUp() throws IOException {
    Configuration conf = new Configuration(false);

    fileSystem = FileSystem.newInstance(URI.create("file:///"), conf);
    rootPath = new Path(temporaryDirectory.toUri());
    storage = new HadoopFsCachePersistenceStorage(fileSystem, rootPath);
  }

  /**
   * Closes the isolated local filesystem after each test.
   * @throws IOException if the filesystem cannot be closed
   */
  @AfterEach
  public void tearDown() throws IOException {
    if (fileSystem != null) {
      fileSystem.close();
    }
  }

  /**
   * Verifies that committed state can be opened and read.
   * @throws IOException if persistence fails
   */
  @Test
  public void testCommitPublishesState() throws IOException {
    byte[] expected = "persistent-state".getBytes(StandardCharsets.UTF_8);

    writeState(KEY, expected);

    assertArrayEquals(expected, readState(KEY));
  }

  /**
   * Verifies that committing a replacement makes the new state visible.
   * @throws IOException if persistence fails
   */
  @Test
  public void testCommitReplacesExistingState() throws IOException {
    byte[] original = "original-state".getBytes(StandardCharsets.UTF_8);
    byte[] replacement = "replacement-state".getBytes(StandardCharsets.UTF_8);

    writeState(KEY, original);
    writeState(KEY, replacement);

    assertArrayEquals(replacement, readState(KEY));
  }

  /**
   * Verifies that aborting a replacement leaves previously committed state unchanged.
   * @throws IOException if persistence fails
   */
  @Test
  public void testAbortPreservesExistingState() throws IOException {
    byte[] original = "original-state".getBytes(StandardCharsets.UTF_8);
    byte[] replacement = "incomplete-state".getBytes(StandardCharsets.UTF_8);

    writeState(KEY, original);

    try (CachePersistenceOutput output = storage.create(KEY)) {
      output.getOutputStream().write(replacement);
      output.abort();
    }

    assertArrayEquals(original, readState(KEY));
    assertNoTemporaryFiles(KEY);
  }

  /**
   * Verifies that closing an uncommitted output aborts the write automatically.
   * @throws IOException if persistence fails
   */
  @Test
  public void testCloseWithoutCommitAbortsWrite() throws IOException {
    byte[] original = "original-state".getBytes(StandardCharsets.UTF_8);
    byte[] replacement = "uncommitted-state".getBytes(StandardCharsets.UTF_8);

    writeState(KEY, original);

    try (CachePersistenceOutput output = storage.create(KEY)) {
      output.getOutputStream().write(replacement);
    }

    assertArrayEquals(original, readState(KEY));
    assertNoTemporaryFiles(KEY);
  }

  /**
   * Verifies that closing an uncommitted first write leaves no committed or temporary state.
   * @throws IOException if persistence fails
   */
  @Test
  public void testAbortedInitialWriteLeavesNoState() throws IOException {
    try (CachePersistenceOutput output = storage.create(KEY)) {
      output.getOutputStream().write("incomplete".getBytes(StandardCharsets.UTF_8));
    }
    assertFalse(storage.open(KEY).isPresent());
    assertNoPersistenceFiles(KEY);
  }

  /**
   * Verifies that opening a key with no persisted state returns an empty optional.
   * @throws IOException if storage access fails
   */
  @Test
  public void testOpenMissingStateReturnsEmpty() throws IOException {
    Optional<InputStream> input = storage.open("engine/l1/missing-engine");

    assertFalse(input.isPresent());
  }

  /**
   * Verifies that nested component keys cause their parent directories to be created.
   * @throws IOException if persistence fails
   */
  @Test
  public void testNestedStorageKeyCreatesParentDirectories() throws IOException {
    String key = "engine/l2/test-engine";

    writeState(key, "state".getBytes(StandardCharsets.UTF_8));

    Path parent = new Path(rootPath, "engine/l2");
    assertTrue(fileSystem.exists(parent));
    assertTrue(fileSystem.getFileStatus(parent).isDirectory());
  }

  /**
   * Verifies that invalid relative storage keys are rejected.
   */
  @Test
  public void testInvalidStorageKeysAreRejected() {
    assertThrows(IllegalArgumentException.class, () -> storage.open(""));
    assertThrows(IllegalArgumentException.class, () -> storage.open("/absolute"));
    assertThrows(IllegalArgumentException.class, () -> storage.open("../outside"));
    assertThrows(IllegalArgumentException.class, () -> storage.open("engine//l2"));
    assertThrows(IllegalArgumentException.class, () -> storage.open("engine\\l2"));
    assertThrows(IllegalArgumentException.class, () -> storage.open("file://state"));
  }

  /**
   * Verifies that commit leaves only the final state file and no temporary or backup files.
   * @throws IOException if persistence fails
   */
  @Test
  public void testSuccessfulCommitCleansTemporaryFiles() throws IOException {
    writeState(KEY, "first".getBytes(StandardCharsets.UTF_8));
    writeState(KEY, "second".getBytes(StandardCharsets.UTF_8));

    Path parent = new Path(rootPath, "engine/l2");
    FileStatus[] files = fileSystem.listStatus(parent);

    assertEquals(1, files.length);
    assertEquals("bucket-cache-engine.state", files[0].getPath().getName());
  }

  /**
   * Verifies that a component save failure does not replace previously committed state.
   * @throws IOException if persistence setup or verification fails
   */
  @Test
  public void testFailedComponentSavePreservesPreviouslyCommittedState() throws IOException {
    byte[] original = "original-state".getBytes(StandardCharsets.UTF_8);
    writeState(KEY, original);

    CacheTopology topology = mock(CacheTopology.class);
    CachePlacementAdmissionPolicy policy = mock(CachePlacementAdmissionPolicy.class);
    CacheEngine engine =
      mock(CacheEngine.class, withSettings().extraInterfaces(PersistentCacheComponent.class));
    PersistentCacheComponent persistent = (PersistentCacheComponent) engine;

    when(persistent.getPersistenceId()).thenReturn("bucket-cache-engine");
    when(topology.getTiers()).thenReturn(List.of(CacheTier.L2));
    when(topology.getEngine(CacheTier.L2)).thenReturn(Optional.of(engine));

    doAnswer(invocation -> {
      OutputStream output = invocation.getArgument(0);
      output.write("partial-state".getBytes(StandardCharsets.UTF_8));
      throw new IOException("Expected save failure");
    }).when(persistent).save(any(OutputStream.class));

    CachePersistenceCoordinator coordinator = new CachePersistenceCoordinator(topology, policy);

    assertThrows(IOException.class, () -> coordinator.save(storage));

    assertArrayEquals(original, readState(KEY));
    assertOnlyCommittedStateFile(KEY);
  }

  /**
   * Writes and commits state for the specified persistence key.
   * @param key  persistence storage key
   * @param data state to write
   * @throws IOException if state cannot be written or committed
   */
  private void writeState(String key, byte[] data) throws IOException {
    try (CachePersistenceOutput output = storage.create(key)) {
      output.getOutputStream().write(data);
      output.commit();
    }
  }

  /**
   * Reads all committed state associated with the specified persistence key.
   * @param key persistence storage key
   * @return persisted state bytes
   * @throws IOException if persisted state does not exist or cannot be read
   */
  private byte[] readState(String key) throws IOException {
    Optional<InputStream> input = storage.open(key);
    assertTrue(input.isPresent(), "Expected persisted state for " + key);

    try (InputStream stream = input.get()) {
      return stream.readAllBytes();
    }
  }

  /**
   * Verifies that no temporary or backup persistence files remain next to the committed state.
   * @param key persistence storage key
   * @throws IOException if the persistence directory cannot be inspected
   */
  private void assertNoTemporaryFiles(String key) throws IOException {
    int separator = key.lastIndexOf('/');
    String parentKey = separator >= 0 ? key.substring(0, separator) : "";
    String stateName = separator >= 0 ? key.substring(separator + 1) : key;

    Path parent = parentKey.isEmpty() ? rootPath : new Path(rootPath, parentKey);
    FileStatus[] files = fileSystem.listStatus(parent);

    assertEquals(1, files.length);
    assertEquals(stateName + ".state", files[0].getPath().getName());
  }

  /**
   * Verifies that no persistence files remain for the specified storage key.
   * @param key persistence storage key
   * @throws IOException if the persistence directory cannot be inspected
   */
  private void assertNoPersistenceFiles(String key) throws IOException {
    Path parent = getParentPath(key);
    if (!fileSystem.exists(parent)) {
      return;
    }
    FileStatus[] files = fileSystem.listStatus(parent);
    assertEquals(0, files.length);
  }

  /**
   * Returns the persistence directory containing the specified storage key.
   * @param key persistence storage key
   * @return parent persistence directory
   */
  private Path getParentPath(String key) {
    int separator = key.lastIndexOf('/');
    if (separator < 0) {
      return rootPath;
    }
    return new Path(rootPath, key.substring(0, separator));
  }

  /**
   * Verifies that only the committed state file remains for the specified storage key.
   * @param key persistence storage key
   * @throws IOException if the persistence directory cannot be inspected
   */
  private void assertOnlyCommittedStateFile(String key) throws IOException {
    int separator = key.lastIndexOf('/');
    String stateName = separator >= 0 ? key.substring(separator + 1) : key;
    Path parent = getParentPath(key);

    FileStatus[] files = fileSystem.listStatus(parent);

    assertEquals(1, files.length);
    assertEquals(stateName + ".state", files[0].getPath().getName());
  }
}
