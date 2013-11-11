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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileBlockIndex;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.util.BloomFilterFactory;
import org.apache.hadoop.io.BytesWritable;

/**
 * Creates an HFile with random key/value pairs.
 */
public class CreateRandomStoreFile {

  /**
   * As much as this number of bytes can be added or subtracted from key/value
   * lengths.
   */
  private static final int LEN_VARIATION = 5;

  private static final Log LOG =
      LogFactory.getLog(CreateRandomStoreFile.class);
  private static final String OUTPUT_DIR_OPTION = "o";
  private static final String NUM_KV_OPTION = "n";
  private static final String HFILE_VERSION_OPTION = "h";
  private static final String KEY_SIZE_OPTION = "k";
  private static final String VALUE_SIZE_OPTION = "v";
  private static final String COMPRESSION_OPTION = "c";
  private static final String BLOOM_FILTER_OPTION = "bf";
  private static final String BLOCK_SIZE_OPTION = "bs";
  private static final String BLOOM_BLOCK_SIZE_OPTION = "bfbs";
  private static final String INDEX_BLOCK_SIZE_OPTION = "ibs";

  /** The exit code this command-line tool returns on failure */
  private static final int EXIT_FAILURE = 1;

  /** The number of valid key types in a store file */
  private static final int NUM_VALID_KEY_TYPES =
      KeyValue.Type.values().length - 2;

  private Options options = new Options();

  private int keyPrefixLen, keyLen, rowLen, cfLen, valueLen;
  private Random rand;

  /**
   * Runs the tools.
   *
   * @param args command-line arguments
   * @return true in case of success
   * @throws IOException
   */
  public boolean run(String[] args) throws IOException {
    options.addOption(OUTPUT_DIR_OPTION, "output_dir", true,
        "Output directory");
    options.addOption(NUM_KV_OPTION, "num_kv", true,
        "Number of key/value pairs");
    options.addOption(KEY_SIZE_OPTION, "key_size", true, "Average key size");
    options.addOption(VALUE_SIZE_OPTION, "value_size", true,
        "Average value size");
    options.addOption(HFILE_VERSION_OPTION, "hfile_version", true,
        "HFile version to create");
    options.addOption(COMPRESSION_OPTION, "compression", true,
        " Compression type, one of "
            + Arrays.toString(Compression.Algorithm.values()));
    options.addOption(BLOOM_FILTER_OPTION, "bloom_filter", true,
        "Bloom filter type, one of "
            + Arrays.toString(BloomType.values()));
    options.addOption(BLOCK_SIZE_OPTION, "block_size", true,
        "HFile block size");
    options.addOption(BLOOM_BLOCK_SIZE_OPTION, "bloom_block_size", true,
        "Compound Bloom filters block size");
    options.addOption(INDEX_BLOCK_SIZE_OPTION, "index_block_size", true,
        "Index block size");

    if (args.length == 0) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(CreateRandomStoreFile.class.getSimpleName(), options,
          true);
      return false;
    }

    CommandLineParser parser = new PosixParser();
    CommandLine cmdLine;
    try {
      cmdLine = parser.parse(options, args);
    } catch (ParseException ex) {
      LOG.error(ex);
      return false;
    }

    if (!cmdLine.hasOption(OUTPUT_DIR_OPTION)) {
      LOG.error("Output directory is not specified");
      return false;
    }

    if (!cmdLine.hasOption(NUM_KV_OPTION)) {
      LOG.error("The number of keys/values not specified");
      return false;
    }

    if (!cmdLine.hasOption(KEY_SIZE_OPTION)) {
      LOG.error("Key size is not specified");
      return false;
    }

    if (!cmdLine.hasOption(VALUE_SIZE_OPTION)) {
      LOG.error("Value size not specified");
      return false;
    }

    Configuration conf = HBaseConfiguration.create();

    Path outputDir = new Path(cmdLine.getOptionValue(OUTPUT_DIR_OPTION));

    long numKV = Long.parseLong(cmdLine.getOptionValue(NUM_KV_OPTION));
    configureKeyValue(numKV,
        Integer.parseInt(cmdLine.getOptionValue(KEY_SIZE_OPTION)),
        Integer.parseInt(cmdLine.getOptionValue(VALUE_SIZE_OPTION)));

    FileSystem fs = FileSystem.get(conf);

    Compression.Algorithm compr = Compression.Algorithm.NONE;
    if (cmdLine.hasOption(COMPRESSION_OPTION)) {
      compr = Compression.Algorithm.valueOf(
          cmdLine.getOptionValue(COMPRESSION_OPTION));
    }

    BloomType bloomType = BloomType.NONE;
    if (cmdLine.hasOption(BLOOM_FILTER_OPTION)) {
      bloomType = BloomType.valueOf(cmdLine.getOptionValue(
          BLOOM_FILTER_OPTION));
    }

    int blockSize = HConstants.DEFAULT_BLOCKSIZE;
    if (cmdLine.hasOption(BLOCK_SIZE_OPTION))
      blockSize = Integer.valueOf(cmdLine.getOptionValue(BLOCK_SIZE_OPTION));
    
    if (cmdLine.hasOption(BLOOM_BLOCK_SIZE_OPTION)) {
      conf.setInt(BloomFilterFactory.IO_STOREFILE_BLOOM_BLOCK_SIZE,
          Integer.valueOf(cmdLine.getOptionValue(BLOOM_BLOCK_SIZE_OPTION)));
    }
    
    if (cmdLine.hasOption(INDEX_BLOCK_SIZE_OPTION)) {
      conf.setInt(HFileBlockIndex.MAX_CHUNK_SIZE_KEY,
          Integer.valueOf(cmdLine.getOptionValue(INDEX_BLOCK_SIZE_OPTION)));
    }

    HFileContext meta = new HFileContextBuilder().withCompression(compr)
                        .withBlockSize(blockSize).build();
    StoreFile.Writer sfw = new StoreFile.WriterBuilder(conf,
        new CacheConfig(conf), fs)
            .withOutputDir(outputDir)
            .withBloomType(bloomType)
            .withMaxKeyCount(numKV)
            .withFileContext(meta)
            .build();

    rand = new Random();
    LOG.info("Writing " + numKV + " key/value pairs");
    for (long i = 0; i < numKV; ++i) {
      sfw.append(generateKeyValue(i));
    }

    int numMetaBlocks = rand.nextInt(10) + 1;
    LOG.info("Writing " + numMetaBlocks + " meta blocks");
    for (int metaI = 0; metaI < numMetaBlocks; ++metaI) {
      sfw.getHFileWriter().appendMetaBlock(generateString(),
          new BytesWritable(generateValue()));
    }
    sfw.close();

    Path storeFilePath = sfw.getPath();
    long fileSize = fs.getFileStatus(storeFilePath).getLen();
    LOG.info("Created " + storeFilePath + ", " + fileSize + " bytes");

    return true;
  }

  private void configureKeyValue(long numKV, int keyLen, int valueLen) {
    numKV = Math.abs(numKV);
    keyLen = Math.abs(keyLen);
    keyPrefixLen = 0;
    while (numKV != 0) {
      numKV >>>= 8;
      ++keyPrefixLen;
    }

    this.keyLen = Math.max(keyPrefixLen, keyLen);
    this.valueLen = valueLen;

    // Arbitrarily split the key into row, column family, and qualifier.
    rowLen = keyPrefixLen / 3;
    cfLen = keyPrefixLen / 4;
  }

  private int nextInRange(int range) {
    return rand.nextInt(2 * range + 1) - range;
  }

  public KeyValue generateKeyValue(long i) {
    byte[] k = generateKey(i);
    byte[] v = generateValue();

    return new KeyValue(
        k, 0, rowLen,
        k, rowLen, cfLen,
        k, rowLen + cfLen, k.length - rowLen - cfLen,
        rand.nextLong(),
        generateKeyType(rand),
        v, 0, v.length);
  }

  public static KeyValue.Type generateKeyType(Random rand) {
    if (rand.nextBoolean()) {
      // Let's make half of KVs puts.
      return KeyValue.Type.Put;
    } else {
      KeyValue.Type keyType =
          KeyValue.Type.values()[1 + rand.nextInt(NUM_VALID_KEY_TYPES)];
      if (keyType == KeyValue.Type.Minimum || keyType == KeyValue.Type.Maximum)
      {
        throw new RuntimeException("Generated an invalid key type: " + keyType
            + ". " + "Probably the layout of KeyValue.Type has changed.");
      }
      return keyType;
    }
  }

  private String generateString() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < rand.nextInt(10); ++i) {
      sb.append((char) ('A' + rand.nextInt(26)));
    }
    return sb.toString();
  }

  private byte[] generateKey(long i) {
    byte[] k = new byte[Math.max(keyPrefixLen, keyLen
        + nextInRange(LEN_VARIATION))];
    for (int pos = keyPrefixLen - 1; pos >= 0; --pos) {
      k[pos] = (byte) (i & 0xFF);
      i >>>= 8;
    }
    for (int pos = keyPrefixLen; pos < k.length; ++pos) {
      k[pos] = (byte) rand.nextInt(256);
    }
    return k;
  }

  private byte[] generateValue() {
    byte[] v = new byte[Math.max(1, valueLen + nextInRange(LEN_VARIATION))];
    for (int i = 0; i < v.length; ++i) {
      v[i] = (byte) rand.nextInt(256);
    }
    return v;
  }

  public static void main(String[] args) {
    CreateRandomStoreFile app = new CreateRandomStoreFile();
    try {
      if (!app.run(args))
        System.exit(EXIT_FAILURE);
    } catch (IOException ex) {
      LOG.error(ex);
      System.exit(EXIT_FAILURE);
    }

  }

}
