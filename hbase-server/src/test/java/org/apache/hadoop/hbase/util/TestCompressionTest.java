/*
 *
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

package org.apache.hadoop.hbase.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.junit.Assert.*;

@Category(SmallTests.class)
public class TestCompressionTest {
  static final Log LOG = LogFactory.getLog(TestCompressionTest.class);

  @Test
  public void testExceptionCaching() {
    // This test will fail if you run the tests with LZO compression available.
    try {
      CompressionTest.testCompression(Compression.Algorithm.LZO);
      fail(); // always throws
    } catch (IOException e) {
      // there should be a 'cause'.
      assertNotNull(e.getCause());
    }

    // this is testing the caching of the test results.
    try {
      CompressionTest.testCompression(Compression.Algorithm.LZO);
      fail(); // always throws
    } catch (IOException e) {
      // there should be NO cause because it's a direct exception not wrapped
      assertNull(e.getCause());
    }

    assertFalse(CompressionTest.testCompression("LZO"));
  }

  @Test
  public void testTestCompression() {
    assertTrue(CompressionTest.testCompression("NONE"));
    assertTrue(CompressionTest.testCompression("GZ"));

    if (NativeCodeLoader.isNativeCodeLoaded()) {
      nativeCodecTest("LZO", "lzo2", "com.hadoop.compression.lzo.LzoCodec");
      nativeCodecTest("LZ4", null, "org.apache.hadoop.io.compress.Lz4Codec");
      nativeCodecTest("SNAPPY", "snappy", "org.apache.hadoop.io.compress.SnappyCodec");
    } else {
      // Hadoop nativelib is not available
      LOG.debug("Native code not loaded");
      assertFalse(CompressionTest.testCompression("LZO"));
      assertFalse(CompressionTest.testCompression("LZ4"));
      assertFalse(CompressionTest.testCompression("SNAPPY"));
    }
  }

  private boolean isCompressionAvailable(String codecClassName) {
    try {
      Thread.currentThread().getContextClassLoader().loadClass(codecClassName);
      return true;
    } catch (Exception ex) {
      return false;
    }
  }

  /**
   * Verify CompressionTest.testCompression() on a native codec.
   */
  private void nativeCodecTest(String codecName, String libName, String codecClassName) {
    if (isCompressionAvailable(codecClassName)) {
      try {
        if (libName != null) {
          System.loadLibrary(libName);
        }

        try {
            Configuration conf = new Configuration();
            CompressionCodec codec = (CompressionCodec)
              ReflectionUtils.newInstance(conf.getClassByName(codecClassName), conf);

            DataOutputBuffer compressedDataBuffer = new DataOutputBuffer();
            CompressionOutputStream deflateFilter = codec.createOutputStream(compressedDataBuffer);

            byte[] data = new byte[1024];
            DataOutputStream deflateOut = new DataOutputStream(new BufferedOutputStream(deflateFilter));
            deflateOut.write(data, 0, data.length);
            deflateOut.flush();
            deflateFilter.finish();

            // Codec class, codec nativelib and Hadoop nativelib with codec JNIs are present
            assertTrue(CompressionTest.testCompression(codecName));
        } catch (UnsatisfiedLinkError e) {
          // Hadoop nativelib does not have codec JNIs.
          // cannot assert the codec here because the current logic of
          // CompressionTest checks only classloading, not the codec
          // usage.
          LOG.debug("No JNI for codec '" + codecName + "' " + e.getMessage());
        } catch (Exception e) {
          LOG.error(codecName, e);
        }
      } catch (UnsatisfiedLinkError e) {
        // nativelib is not available
        LOG.debug("Native lib not available: " + codecName);
        assertFalse(CompressionTest.testCompression(codecName));
      }
    } else {
      // Compression Codec class is not available
      LOG.debug("Codec class not available: " + codecName);
      assertFalse(CompressionTest.testCompression(codecName));
    }
  }
}

