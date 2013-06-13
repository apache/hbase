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

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test that we can create, load, setup our own custom codec
 */
@Category(SmallTests.class)
public class TestCustomWALEditCodec {

  public static class CustomWALEditCodec extends WALEditCodec {
    public boolean initialized = false;
    public boolean compressionSet = false;

    @Override
    public void init(Configuration conf) {
      this.initialized = true;
    }

    @Override
    public void setCompression(CompressionContext compression) {
      this.compressionSet = true;
    }
  }

  /**
   * Test that a custom WALEditCodec will be completely setup when it is instantiated via
   * {@link WALEditCodec}
   * @throws Exception on failure
   */
  @Test
  public void testCreatePreparesCodec() throws Exception {
    Configuration conf = new Configuration(false);
    conf.setClass(WALEditCodec.WAL_EDIT_CODEC_CLASS_KEY, CustomWALEditCodec.class, WALEditCodec.class);
    CustomWALEditCodec codec = (CustomWALEditCodec) WALEditCodec.create(conf, null);
    assertTrue("Custom codec didn't get initialized", codec.initialized);
    assertTrue("Custom codec didn't have compression set", codec.compressionSet);
  }
}