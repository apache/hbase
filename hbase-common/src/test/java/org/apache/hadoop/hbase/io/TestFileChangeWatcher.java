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
package org.apache.hadoop.hbase.io;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.endsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.io.crypto.tls.X509Util;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This file was originally copied from the Apache ZooKeeper project, but has been modified
 * @see <a href=
 *      "https://github.com/apache/zookeeper/blob/391cb4aa6b54e19a028215e1340232a114c23ed3/zookeeper-server/src/test/java/org/apache/zookeeper/common/FileChangeWatcherTest.java">Base
 *      revision</a>
 */
@Tag(IOTests.TAG)
@Tag(SmallTests.TAG)
public class TestFileChangeWatcher {

  private static final Logger LOG = LoggerFactory.getLogger(TestFileChangeWatcher.class);
  private static final HBaseCommonTestingUtility UTIL = new HBaseCommonTestingUtility();

  private static File dir;

  private static final Duration POLL_INTERVAL = Duration.ofMillis(100);

  private File tempFile;

  private FileChangeWatcher watcher;

  @BeforeAll
  public static void setUpBeforeAll() throws IOException {
    dir = new File(UTIL.getDataTestDir().toString()).getAbsoluteFile();
    if (!dir.mkdirs()) {
      throw new IOException("can not mkdir " + dir);
    }
  }

  @AfterAll
  public static void cleanupTempDir() {
    UTIL.cleanupTestDir();
  }

  @BeforeEach
  public void setUp(TestInfo testInfo) throws IOException {
    tempFile = new File(dir, "file_change_test_" + testInfo.getDisplayName());
    if (!tempFile.createNewFile()) {
      throw new IOException("failed to create new empty file " + tempFile);
    }
  }

  @AfterEach
  public void tearDown() throws InterruptedException {
    if (watcher != null) {
      watcher.stop();
      watcher.waitForState(FileChangeWatcher.State.STOPPED);
      watcher = null;
    }
  }

  @Test
  public void testEnableCertFileReloading() throws IOException {
    Configuration myConf = new Configuration();
    String sharedPath = File.createTempFile("foo", "foo.jks").getAbsolutePath();
    myConf.set(X509Util.TLS_CONFIG_KEYSTORE_LOCATION, sharedPath);
    myConf.set(X509Util.TLS_CONFIG_TRUSTSTORE_LOCATION, sharedPath);
    AtomicReference<FileChangeWatcher> keystoreWatcher = new AtomicReference<>();
    AtomicReference<FileChangeWatcher> truststoreWatcher = new AtomicReference<>();
    X509Util.enableCertFileReloading(myConf, keystoreWatcher, truststoreWatcher, () -> {
    });
    assertNotNull(keystoreWatcher.get());
    assertThat(keystoreWatcher.get().getWatcherThread().getName(), endsWith("foo.jks"));
    assertNull(truststoreWatcher.get());

    keystoreWatcher.getAndSet(null).stop();
    truststoreWatcher.set(null);

    String truststorePath = File.createTempFile("bar", "bar.jks").getAbsolutePath();
    myConf.set(X509Util.TLS_CONFIG_TRUSTSTORE_LOCATION, truststorePath);
    X509Util.enableCertFileReloading(myConf, keystoreWatcher, truststoreWatcher, () -> {
    });

    assertNotNull(keystoreWatcher.get());
    assertThat(keystoreWatcher.get().getWatcherThread().getName(), endsWith("foo.jks"));
    assertNotNull(truststoreWatcher.get());
    assertThat(truststoreWatcher.get().getWatcherThread().getName(), endsWith("bar.jks"));

    keystoreWatcher.getAndSet(null).stop();
    truststoreWatcher.getAndSet(null).stop();
  }

  // wait until watcher thread finish loading the last modified time, we check this by checking
  // whether the watcher thread has been in TIMED_WAITING state, i.e, waiting for the next runLoop
  private void awaitWatcherThreadInitialized() throws InterruptedException {
    watcher.waitForState(FileChangeWatcher.State.RUNNING);
    await().atMost(Duration.ofSeconds(2)).pollInSameThread().pollInterval(Duration.ofMillis(10))
      .until(() -> watcher.getWatcherThread().getState() == Thread.State.TIMED_WAITING);
  }

  @Test
  public void testNoFalseNotifications() throws Exception {
    final List<Path> notifiedPaths = new ArrayList<>();
    watcher = new FileChangeWatcher(tempFile.toPath(), "test", POLL_INTERVAL, path -> {
      LOG.info("Got an update on path {}", path);
      synchronized (notifiedPaths) {
        notifiedPaths.add(path);
        notifiedPaths.notifyAll();
      }
    });
    watcher.start();
    awaitWatcherThreadInitialized();
    await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(3))
      .untilAsserted(() -> assertEquals("Should not have been notified", 0, notifiedPaths.size()));
  }

  @Test
  public void testCallbackWorksOnFileChanges() throws IOException, InterruptedException {
    final List<Path> notifiedPaths = Collections.synchronizedList(new ArrayList<>());
    watcher = new FileChangeWatcher(tempFile.toPath(), "test", POLL_INTERVAL, path -> {
      LOG.info("Got an update on path {}", path);
      notifiedPaths.add(path);
    });
    watcher.start();
    awaitWatcherThreadInitialized();
    for (int i = 0; i < 3; i++) {
      final int index = i;
      LOG.info("Modifying file, attempt {}", (index + 1));
      FileUtils.writeStringToFile(tempFile, "Hello world " + index + "\n", StandardCharsets.UTF_8,
        true);
      await().atMost(Duration.ofSeconds(2)).untilAsserted(
        () -> assertEquals("Wrong number of notifications", index + 1, notifiedPaths.size()));
      Path path = notifiedPaths.get(index);
      assertEquals(tempFile.getPath(), path.toString());
    }
  }

  @Test
  public void testCallbackWorksOnFileTouched() throws IOException, InterruptedException {
    final List<Path> notifiedPaths = Collections.synchronizedList(new ArrayList<>());
    watcher = new FileChangeWatcher(tempFile.toPath(), "test", POLL_INTERVAL, path -> {
      LOG.info("Got an update on path {}", path);
      notifiedPaths.add(path);
    });
    watcher.start();
    awaitWatcherThreadInitialized();
    LOG.info("Touching file");
    FileUtils.touch(tempFile);
    await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> assertFalse(notifiedPaths.isEmpty()));
    Path path = notifiedPaths.get(0);
    assertEquals(tempFile.getPath(), path.toString());
  }

  @Test
  public void testCallbackErrorDoesNotCrashWatcherThread() throws Exception {
    final AtomicInteger callCount = new AtomicInteger(0);
    watcher = new FileChangeWatcher(tempFile.toPath(), "test", POLL_INTERVAL, path -> {
      LOG.info("Got an update for path {}", path);
      callCount.incrementAndGet();
      throw new RuntimeException("This error should not crash the watcher thread");
    });
    watcher.start();
    awaitWatcherThreadInitialized();

    LOG.info("Modifying file");
    FileUtils.writeStringToFile(tempFile, "Hello world\n", StandardCharsets.UTF_8, true);
    await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> assertEquals(1, callCount.get()));

    // make sure we can still receive the update event, which means the watcher thread is still
    // alive
    LOG.info("Modifying file again");
    FileUtils.writeStringToFile(tempFile, "Hello world again\n", StandardCharsets.UTF_8, true);
    await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> assertEquals(2, callCount.get()));

    // also make sure that the thread is not terminated
    assertNotEquals(Thread.State.TERMINATED, watcher.getWatcherThread());
  }
}
