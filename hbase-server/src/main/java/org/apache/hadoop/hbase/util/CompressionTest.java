/**
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

import java.io.IOException;
import java.util.Locale;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.ExtendedCellBuilderFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.HFileWriterImpl;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.io.compress.Compressor;

/**
 * Compression validation test.  Checks compression is working.  Be sure to run
 * on every node in your cluster.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
@InterfaceStability.Evolving
public class CompressionTest {
  private static final Logger LOG = LoggerFactory.getLogger(CompressionTest.class);

  public static boolean testCompression(String codec) {
    codec = codec.toLowerCase(Locale.ROOT);

    Compression.Algorithm a;

    try {
      a = Compression.getCompressionAlgorithmByName(codec);
    } catch (IllegalArgumentException e) {
      LOG.warn("Codec type: " + codec + " is not known");
      return false;
    }

    try {
      testCompression(a);
      return true;
    } catch (IOException ignored) {
      LOG.warn("Can't instantiate codec: " + codec, ignored);
      return false;
    }
  }

  private final static Boolean[] compressionTestResults
      = new Boolean[Compression.Algorithm.values().length];
  static {
    for (int i = 0 ; i < compressionTestResults.length ; ++i) {
      compressionTestResults[i] = null;
    }
  }

  public static void testCompression(Compression.Algorithm algo)
      throws IOException {
    if (compressionTestResults[algo.ordinal()] != null) {
      if (compressionTestResults[algo.ordinal()]) {
        return ; // already passed test, dont do it again.
      } else {
        // failed.
        throw new DoNotRetryIOException("Compression algorithm '" + algo.getName() + "'" +
        " previously failed test.");
      }
    }

    try {
      Compressor c = algo.getCompressor();
      algo.returnCompressor(c);
      compressionTestResults[algo.ordinal()] = true; // passes
    } catch (Throwable t) {
      compressionTestResults[algo.ordinal()] = false; // failure
      throw new DoNotRetryIOException(t);
    }
  }

  protected static Path path = new Path(".hfile-comp-test");

  public static void usage() {

    System.err.println(
      "Usage: CompressionTest <path> " +
      StringUtils.join( Compression.Algorithm.values(), "|").toLowerCase(Locale.ROOT) +
      "\n" +
      "For example:\n" +
      "  hbase " + CompressionTest.class + " file:///tmp/testfile gz\n");
    System.exit(1);
  }

  public static void doSmokeTest(FileSystem fs, Path path, String codec)
  throws Exception {
    Configuration conf = HBaseConfiguration.create();
    HFileContext context = new HFileContextBuilder()
                           .withCompression(HFileWriterImpl.compressionByName(codec)).build();
    HFile.Writer writer = HFile.getWriterFactoryNoCache(conf)
        .withPath(fs, path)
        .withFileContext(context)
        .create();
    // Write any-old Cell...
    final byte [] rowKey = Bytes.toBytes("compressiontestkey");
    Cell c = ExtendedCellBuilderFactory.create(CellBuilderType.DEEP_COPY)
      .setRow(rowKey)
      .setFamily(HConstants.EMPTY_BYTE_ARRAY)
      .setQualifier(HConstants.EMPTY_BYTE_ARRAY)
      .setTimestamp(HConstants.LATEST_TIMESTAMP)
      .setType(KeyValue.Type.Maximum.getCode())
      .setValue(Bytes.toBytes("compressiontestval"))
      .build();
    writer.append(c);
    writer.appendFileInfo(Bytes.toBytes("compressioninfokey"), Bytes.toBytes("compressioninfoval"));
    writer.close();
    Cell cc = null;
    HFile.Reader reader = HFile.createReader(fs, path, CacheConfig.DISABLED, true, conf);
    try {
      reader.loadFileInfo();
      HFileScanner scanner = reader.getScanner(false, true);
      scanner.seekTo(); // position to the start of file
      // Scanner does not do Cells yet. Do below for now till fixed.
      cc = scanner.getCell();
      if (CellComparator.getInstance().compareRows(c, cc) != 0) {
        throw new Exception("Read back incorrect result: " + c.toString() + " vs " + cc.toString());
      }
    } finally {
      reader.close();
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      usage();
      System.exit(1);
    }

    Configuration conf = new Configuration();
    Path path = new Path(args[0]);
    FileSystem fs = path.getFileSystem(conf);
    if (fs.exists(path)) {
      System.err.println("The specified path exists, aborting!");
      System.exit(1);
    }

    try {
      doSmokeTest(fs, path, args[1]);
    } finally {
      fs.delete(path, false);
    }
    System.out.println("SUCCESS");
  }
}
