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

package org.apache.hadoop.hbase.test.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.hfile.FixedFileTrailer;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileBlock;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.hbase.thirdparty.org.apache.commons.cli.DefaultParser;
import org.apache.hbase.thirdparty.org.apache.commons.cli.Options;

/**
 * A utility for extracting individual blocks from HFiles into a collection of
 * individual files, one per block.
 * <p>
 * Usage:
 * <blockquote>
 * <tt>HFileBlockExtracter options outputDir hFile1 ... hFileN </tt><br>
 * <p>
 * where options are one or more of:
 * <p>
 * &nbsp;&nbsp; -d width: Width of generated file name for zero padding, default: 5 <br>
 * &nbsp;&nbsp; -n count: Total number of blocks to extract, default: unlimited <br>
 * &nbsp;&nbsp; -r | --random: Shuffle blocks and write them in randomized order
 * </blockquote>
 */
public class HFileBlockExtracter extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory.getLogger(HFileBlockExtracter.class);

  private Path outputDir;
  private int padWidth = 5;
  private int count, limit = -1;
  private boolean randomize;

  @Override
  public int run(String[] args) throws Exception {
    final Options opts = new Options()
      .addOption("r", "random", false, "Shuffle blocks and write them in randomized order")
      .addOption("d", true, "Width of generated file name for zero padding")
      .addOption("n", true, "Total number of blocks to extract");
    final CommandLine cmd = new DefaultParser().parse(opts, args);
    randomize = cmd.hasOption("r");
    if (cmd.hasOption("d")) {
      padWidth = Integer.valueOf(cmd.getOptionValue("d"));
    }
    if (cmd.hasOption("n")) {
      limit = Integer.valueOf(cmd.getOptionValue("n"));
    }
    args = cmd.getArgs();
    if (args.length < 2) {
      System.out.println(
        "Usage: HFileBlockExtracter <options> <outputDir> <hfile_1> ... <hfile_n>");
      System.out.println("where <options> are:");
      System.out.println(opts.toString());
    }
    outputDir = new Path(args[0]);
    final FileSystem fs = FileSystem.get(outputDir.toUri(), getConf());
    if (fs.exists(outputDir)) {
      throw new IllegalArgumentException(outputDir + " already exists");
    }
    if (!fs.mkdirs(outputDir)) {
      throw new IOException("Could not create " + outputDir);
    }
    for (int i = 1; i < args.length; i++) {
      extractBlocks(outputDir, new Path(args[i]));
    }
    return 0;
  }

  private void extractBlocks(final Path outputDir, final Path inPath)
      throws IOException {
    final String format = "%0" + padWidth + "d";
    final FileSystem inFs = FileSystem.get(inPath.toUri(), getConf());
    long fileSize, firstOffset, maxOffset;
    try (FSDataInputStreamWrapper fsdis = new FSDataInputStreamWrapper(inFs, inPath)) {
      fileSize = inFs.getFileStatus(inPath).getLen();
      final FixedFileTrailer trailer =
        FixedFileTrailer.readFromStream(fsdis.getStream(false), fileSize);
      firstOffset = trailer.getFirstDataBlockOffset();
      maxOffset = trailer.getLastDataBlockOffset();
    }
    LOG.info("Extracting blocks from {} (size={})", inPath, fileSize);
    // Build the list of block offsets and sizes
    final ArrayList<Long> offsets = new ArrayList<>();
    try (final HFile.Reader reader = HFile.createReader(inFs, inPath, getConf())) {
      long offset = firstOffset;
      while (offset <= maxOffset) {
        final HFileBlock block = reader.readBlock(offset, -1,
          /* cacheBlock */ false,
          /* pread */ false,
          /* isCompaction */ false,
          /* updateCacheMetrics */ false,
          null, null);
        switch (block.getBlockType().getCategory()) {
          // Only DATA and INDEX category blocks are compressed
          case DATA:
          case INDEX:
            offsets.add(offset);
            break;
          default:
            break;
        }
        offset += block.getOnDiskSizeWithHeader();
      }
      if (randomize) {
        LOG.info("Randomizing offsets");
        Collections.shuffle(offsets);
      }
      final FileSystem outFs = FileSystem.get(outputDir.toUri(), getConf());
      for (long o: offsets) {
        HFileBlock block = reader.readBlock(o, -1,
          false, // cacheBlock
          false, // pread
          false, // isCompation
          false, // updateCacheMetrics
          null, null);
        final Path outPath = new Path(outputDir, String.format(format, count++));
        try (final FSDataOutputStream out = outFs.create(outPath)) {
          final ByteBuff buffer = block.getBufferWithoutHeader();
          if (!buffer.hasArray()) {
            throw new RuntimeException("Not an on heap byte buffer");
          }
          LOG.info("{} at {} len {} -> {}", block.getBlockType(), o, buffer.remaining(),
            outPath);
          out.write(buffer.array(), buffer.arrayOffset() + buffer.position(),
            buffer.remaining());
        }
        if (limit > 0 && count >= limit) {
          break;
        }
      }
    }
  }

  public static void main(String[] args) {
    try {
      int ret = ToolRunner.run(HBaseConfiguration.create(), new HFileBlockExtracter(), args);
      System.exit(ret);
    } catch (Exception e) {
      LOG.error("Tool failed", e);
      System.exit(-1);
    }
  }

}
