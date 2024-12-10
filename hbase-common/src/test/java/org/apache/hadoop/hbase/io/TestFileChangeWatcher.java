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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.io.crypto.tls.X509Util;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This file was originally copied from the Apache ZooKeeper project, but has been modified
 * @see <a href=
 *      "https://github.com/apache/zookeeper/blob/391cb4aa6b54e19a028215e1340232a114c23ed3/zookeeper-server/src/test/java/org/apache/zookeeper/common/FileChangeWatcherTest.java">Base
 *      revision</a>
 */
@Category({ IOTests.class, SmallTests.class })
public class TestFileChangeWatcher {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestFileChangeWatcher.class);

  private static File tempFile;

  private static final Logger LOG = LoggerFactory.getLogger(TestFileChangeWatcher.class);
  private static final HBaseCommonTestingUtility UTIL = new HBaseCommonTestingUtility();

  private static final long FS_TIMEOUT = 30000L;
  private static final Duration POLL_INTERVAL = Duration.ofMillis(100);

  @BeforeClass
  public static void createTempFile() throws IOException {
    tempFile = File.createTempFile("zk_test_", "");
  }

  @AfterClass
  public static void cleanupTempDir() {
    UTIL.cleanupTestDir();
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
    assertThat(keystoreWatcher.get().getWatcherThreadName(), Matchers.endsWith("foo.jks"));
    assertNull(truststoreWatcher.get());

    keystoreWatcher.getAndSet(null).stop();
    truststoreWatcher.set(null);

    String truststorePath = File.createTempFile("bar", "bar.jks").getAbsolutePath();
    myConf.set(X509Util.TLS_CONFIG_TRUSTSTORE_LOCATION, truststorePath);
    X509Util.enableCertFileReloading(myConf, keystoreWatcher, truststoreWatcher, () -> {
    });

    assertNotNull(keystoreWatcher.get());
    assertThat(keystoreWatcher.get().getWatcherThreadName(), Matchers.endsWith("foo.jks"));
    assertNotNull(truststoreWatcher.get());
    assertThat(truststoreWatcher.get().getWatcherThreadName(), Matchers.endsWith("bar.jks"));

    keystoreWatcher.getAndSet(null).stop();
    truststoreWatcher.getAndSet(null).stop();
  }

  @Test
  public void testNoFalseNotifications() throws IOException, InterruptedException {
    FileChangeWatcher watcher = null;
    try {
      final List<Path> notifiedPaths = new ArrayList<>();
      watcher = new FileChangeWatcher(tempFile.toPath(), "test", POLL_INTERVAL, path -> {
        LOG.info("Got an update on path {}", path);
        synchronized (notifiedPaths) {
          notifiedPaths.add(path);
          notifiedPaths.notifyAll();
        }
      });
      watcher.start();
      watcher.waitForState(FileChangeWatcher.State.RUNNING);
      Thread.sleep(1000L); // TODO hack
      assertEquals("Should not have been notified", 0, notifiedPaths.size());
    } finally {
      if (watcher != null) {
        watcher.stop();
        watcher.waitForState(FileChangeWatcher.State.STOPPED);
      }
    }
  }

  @Test
  public void testCallbackWorksOnFileChanges() throws IOException, InterruptedException {
    FileChangeWatcher watcher = null;
    try {
      final List<Path> notifiedPaths = new ArrayList<>();
      watcher = new FileChangeWatcher(tempFile.toPath(), "test", POLL_INTERVAL, path -> {
        LOG.info("Got an update on path {}", path);
        synchronized (notifiedPaths) {
          notifiedPaths.add(path);
          notifiedPaths.notifyAll();
        }
      });
      watcher.start();
      watcher.waitForState(FileChangeWatcher.State.RUNNING);
      Thread.sleep(1000L); // TODO hack
      for (int i = 0; i < 3; i++) {
        LOG.info("Modifying file, attempt {}", (i + 1));
        FileUtils.writeStringToFile(tempFile, "Hello world " + i + "\n", StandardCharsets.UTF_8,
          true);
        synchronized (notifiedPaths) {
          if (notifiedPaths.size() < i + 1) {
            notifiedPaths.wait(FS_TIMEOUT);
          }
          assertEquals("Wrong number of notifications", i + 1, notifiedPaths.size());
          Path path = notifiedPaths.get(i);
          assertEquals(tempFile.getPath(), path.toString());
        }
      }
    } finally {
      if (watcher != null) {
        watcher.stop();
        watcher.waitForState(FileChangeWatcher.State.STOPPED);
      }
    }
  }

  @Test
  public void testCallbackWorksOnFileTouched() throws IOException, InterruptedException {
    FileChangeWatcher watcher = null;
    try {
      final List<Path> notifiedPaths = new ArrayList<>();
      watcher = new FileChangeWatcher(tempFile.toPath(), "test", POLL_INTERVAL, path -> {
        LOG.info("Got an update on path {}", path);
        synchronized (notifiedPaths) {
          notifiedPaths.add(path);
          notifiedPaths.notifyAll();
        }
      });
      watcher.start();
      watcher.waitForState(FileChangeWatcher.State.RUNNING);
      Thread.sleep(1000L); // TODO hack
      LOG.info("Touching file");
      FileUtils.touch(tempFile);
      synchronized (notifiedPaths) {
        if (notifiedPaths.isEmpty()) {
          notifiedPaths.wait(FS_TIMEOUT);
        }
        assertFalse(notifiedPaths.isEmpty());
        Path path = notifiedPaths.get(0);
        assertEquals(tempFile.getPath(), path.toString());
      }
    } finally {
      if (watcher != null) {
        watcher.stop();
        watcher.waitForState(FileChangeWatcher.State.STOPPED);
      }
    }
  }

  @Test
  public void testCallbackErrorDoesNotCrashWatcherThread()
    throws IOException, InterruptedException {
    FileChangeWatcher watcher = null;
    try {
      final AtomicInteger callCount = new AtomicInteger(0);
      watcher = new FileChangeWatcher(tempFile.toPath(), "test", POLL_INTERVAL, path -> {
        LOG.info("Got an update for path {}", path);
        int oldValue;
        synchronized (callCount) {
          oldValue = callCount.getAndIncrement();
          callCount.notifyAll();
        }
        if (oldValue == 0) {
          throw new RuntimeException("This error should not crash the watcher thread");
        }
      });
      watcher.start();
      watcher.waitForState(FileChangeWatcher.State.RUNNING);
      Thread.sleep(1000L); // TODO hack
      LOG.info("Modifying file");
      FileUtils.writeStringToFile(tempFile, "Hello world\n", StandardCharsets.UTF_8, true);
      synchronized (callCount) {
        while (callCount.get() == 0) {
          callCount.wait(FS_TIMEOUT);
        }
      }
      LOG.info("Modifying file again");
      FileUtils.writeStringToFile(tempFile, "Hello world again\n", StandardCharsets.UTF_8, true);
      synchronized (callCount) {
        if (callCount.get() == 1) {
          callCount.wait(FS_TIMEOUT);
        }
      }
      // The value of callCount can exceed 1 only if the callback thread
      // survives the exception thrown by the first callback.
      assertTrue(callCount.get() > 1);
    } finally {
      if (watcher != null) {
        watcher.stop();
        watcher.waitForState(FileChangeWatcher.State.STOPPED);
      }
    }
  }
}
