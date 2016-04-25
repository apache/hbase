/**
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
package org.apache.hadoop.hbase.regionserver.wal;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

/**
 * Provides AsyncFSWAL test cases.
 */
@Category({ RegionServerTests.class, MediumTests.class })
public class TestAsyncFSWAL extends AbstractTestFSWAL {

  private static EventLoopGroup GROUP;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    GROUP = new NioEventLoopGroup();
    AbstractTestFSWAL.setUpBeforeClass();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    AbstractTestFSWAL.tearDownAfterClass();
    GROUP.shutdownGracefully();
  }

  @Override
  protected AbstractFSWAL<?> newWAL(FileSystem fs, Path rootDir, String logDir, String archiveDir,
      Configuration conf, List<WALActionsListener> listeners, boolean failIfWALExists,
      String prefix, String suffix) throws IOException {
    return new AsyncFSWAL(fs, rootDir, logDir, archiveDir, conf, listeners, failIfWALExists, prefix,
        suffix, GROUP.next());
  }

  @Override
  protected AbstractFSWAL<?> newSlowWAL(FileSystem fs, Path rootDir, String logDir,
      String archiveDir, Configuration conf, List<WALActionsListener> listeners,
      boolean failIfWALExists, String prefix, String suffix, final Runnable action)
      throws IOException {
    return new AsyncFSWAL(fs, rootDir, logDir, archiveDir, conf, listeners, failIfWALExists, prefix,
        suffix, GROUP.next()) {

      @Override
      void atHeadOfRingBufferEventHandlerAppend() {
        action.run();
        super.atHeadOfRingBufferEventHandlerAppend();
      }

    };
  }
}
