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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.AbstractHFileWriter;
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
  static final Log LOG = LogFactory.getLog(CompressionTest.class);

  public static boolean testCompression(String codec) {
    codec = codec.toLowerCase();

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
      StringUtils.join( Compression.Algorithm.values(), "|").toLowerCase() +
      "\n" +
      "For example:\n" +
      "  hbase " + CompressionTest.class + " file:///tmp/testfile gz\n");
    System.exit(1);
  }

  public static void doSmokeTest(FileSystem fs, Path path, String codec)
  throws Exception {
    Configuration conf = HBaseConfiguration.create();
    HFileContext context = new HFileContextBuilder()
                           .withCompression(AbstractHFileWriter.compressionByName(codec)).build();
    HFile.Writer writer = HFile.getWriterFactoryNoCache(conf)
        .withPath(fs, path)
        .withFileContext(context)
        .create();
    // Write any-old Cell...
    final byte [] rowKey = Bytes.toBytes("compressiontestkey");
    Cell c = CellUtil.createCell(rowKey, Bytes.toBytes("compressiontestval"));
    writer.append(c);
    writer.appendFileInfo(Bytes.toBytes("compressioninfokey"), Bytes.toBytes("compressioninfoval"));
    writer.close();
    Cell cc = null;
    HFile.Reader reader = HFile.createReader(fs, path, new CacheConfig(conf), conf);
    try {
      reader.loadFileInfo();
      HFileScanner scanner = reader.getScanner(false, true);
      scanner.seekTo(); // position to the start of file
      // Scanner does not do Cells yet. Do below for now till fixed.
      cc = scanner.getKeyValue();
      if (CellComparator.compareRows(c, cc) != 0) {
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
