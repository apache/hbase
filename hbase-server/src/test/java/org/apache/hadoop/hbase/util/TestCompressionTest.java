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
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.*;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MiscTests.class, SmallTests.class})
public class TestCompressionTest {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCompressionTest.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestCompressionTest.class);

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
      // LZO is GPL so not included in hadoop install. You need to do an extra install to pick
      // up the needed support. This article is good on the steps needed to add LZO support:
      // https://stackoverflow.com/questions/23441142/class-com-hadoop-compression-lzo-lzocodec-not-found-for-spark-on-cdh-5
      // Its unlikely at test time that the extras are installed so this test is useless.
      // nativeCodecTest("LZO", "lzo2", "com.hadoop.compression.lzo.LzoCodec");
      nativeCodecTest("LZ4", null, "org.apache.hadoop.io.compress.Lz4Codec");
      nativeCodecTest("SNAPPY", "snappy", "org.apache.hadoop.io.compress.SnappyCodec");
      nativeCodecTest("BZIP2", "bzip2", "org.apache.hadoop.io.compress.BZip2Codec");
      nativeCodecTest("ZSTD", "zstd", "org.apache.hadoop.io.compress.ZStandardCodec");
    } else {
      // Hadoop nativelib is not available
      LOG.debug("Native code not loaded");
      // This check is useless as it fails with
      //  ...DoNotRetryIOException: Compression algorithm 'lzo' previously failed test.
      // assertFalse("LZO", CompressionTest.testCompression("LZO"));
      // LZ4 requires that the native lib be present before 3.3.1. After 3.3.1, hadoop uses
      // lz4-java which will do java version of lz4 as last resort -- so the below fails before
      // 3.3.1 but passes at 3.3.1+... so commenting it out. See HADOOP-17292.
      // assertFalse("LZ4", CompressionTest.testCompression("LZ4"));
      // Same thing happens for snappy. See HADOOP-17125
      // assertFalse(CompressionTest.testCompression("SNAPPY"));
      assertFalse(CompressionTest.testCompression("BZIP2"));
      assertFalse(CompressionTest.testCompression("ZSTD"));
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

