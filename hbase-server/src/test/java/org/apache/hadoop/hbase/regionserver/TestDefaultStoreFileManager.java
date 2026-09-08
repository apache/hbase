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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.OptionalLong;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionConfiguration;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestDefaultStoreFileManager {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestDefaultStoreFileManager.class);

  @Test
  public void testGetUnneededFilesSkipsNullReader() throws IOException {
    DefaultStoreFileManager manager = createManager(createStoreFile("null-reader.hfile", 1, null),
      createStoreFile("expired.hfile", 2, 5L), createStoreFile("latest.hfile", 3, 50L));
    BlockingDeque<LevelAndMessage> logs = new LinkedBlockingDeque<>();
    org.apache.logging.log4j.core.Appender appender =
      mock(org.apache.logging.log4j.core.Appender.class);
    when(appender.getName()).thenReturn("mockAppender");
    when(appender.isStarted()).thenReturn(true);
    doAnswer(new Answer<Void>() {

      @Override
      public Void answer(InvocationOnMock invocation) {
        org.apache.logging.log4j.core.LogEvent logEvent =
          invocation.getArgument(0, org.apache.logging.log4j.core.LogEvent.class);
        logs.add(new LevelAndMessage(logEvent.getLevel(),
          logEvent.getMessage().getFormattedMessage()));
        return null;
      }
    }).when(appender).append(any(org.apache.logging.log4j.core.LogEvent.class));

    org.apache.logging.log4j.core.Logger logger =
      (org.apache.logging.log4j.core.Logger) org.apache.logging.log4j.LogManager
        .getLogger(DefaultStoreFileManager.class);
    logger.addAppender(appender);
    try {
      Collection<HStoreFile> unneededFiles = manager.getUnneededFiles(10L, Collections.emptyList());
      assertEquals(1, unneededFiles.size());
      assertTrue(
        unneededFiles.stream().anyMatch(sf -> "expired.hfile".equals(sf.getPath().getName())));
      assertFalse(
        unneededFiles.stream().anyMatch(sf -> "null-reader.hfile".equals(sf.getPath().getName())));
      assertTrue(logs.stream().anyMatch(log -> log.level == org.apache.logging.log4j.Level.DEBUG
        && log.message.contains("Skipping store file")
        && log.message.contains("null-reader.hfile")
        && log.message.contains("reader is null")));
    } finally {
      logger.removeAppender(appender);
    }
  }

  private static DefaultStoreFileManager createManager(HStoreFile... files) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    CompactionConfiguration comConf = mock(CompactionConfiguration.class);
    DefaultStoreFileManager manager = new DefaultStoreFileManager(CellComparatorImpl.COMPARATOR,
      StoreFileComparators.SEQ_ID, conf, comConf);
    manager.loadFiles(Arrays.asList(files));
    return manager;
  }

  private static HStoreFile createStoreFile(String name, long sequenceId, Long maxTimestamp) {
    HStoreFile storeFile = mock(HStoreFile.class);
    when(storeFile.getPath()).thenReturn(new Path("/hbase/" + name));
    when(storeFile.getMaxSequenceId()).thenReturn(sequenceId);
    when(storeFile.getBulkLoadTimestamp()).thenReturn(OptionalLong.empty());
    if (maxTimestamp == null) {
      when(storeFile.getReader()).thenReturn(null);
    } else {
      StoreFileReader reader = mock(StoreFileReader.class);
      when(reader.length()).thenReturn(1L);
      when(reader.getMaxTimestamp()).thenReturn(maxTimestamp);
      when(storeFile.getReader()).thenReturn(reader);
    }
    return storeFile;
  }

  private static final class LevelAndMessage {
    private final org.apache.logging.log4j.Level level;
    private final String message;

    private LevelAndMessage(org.apache.logging.log4j.Level level, String message) {
      this.level = level;
      this.message = message;
    }
  }
}
