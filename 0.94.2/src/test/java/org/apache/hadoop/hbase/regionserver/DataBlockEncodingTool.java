/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.encoding.EncodedDataBlock;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoder;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.NoOpDataBlockEncoder;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;

/**
 * Tests various algorithms for key compression on an existing HFile. Useful
 * for testing, debugging and benchmarking.
 */
public class DataBlockEncodingTool {
  private static final Log LOG = LogFactory.getLog(
      DataBlockEncodingTool.class);

  private static final boolean includesMemstoreTS = true;

  /**
   * How many times should benchmark run.
   * More times means better data in terms of statistics.
   * It has to be larger than BENCHMARK_N_OMIT.
   */
  public static int BENCHMARK_N_TIMES = 12;

  /**
   * How many first runs should omit benchmark.
   * Usually it is one in order to exclude setup cost.
   * Has to be 0 or larger.
   */
  public static int BENCHMARK_N_OMIT = 2;

  /** Compression algorithm to use if not specified on the command line */
  private static final Algorithm DEFAULT_COMPRESSION =
      Compression.Algorithm.GZ;

  private List<EncodedDataBlock> codecs = new ArrayList<EncodedDataBlock>();
  private int totalPrefixLength = 0;
  private int totalKeyLength = 0;
  private int totalValueLength = 0;
  private int totalKeyRedundancyLength = 0;

  final private String compressionAlgorithmName;
  final private Algorithm compressionAlgorithm;
  final private Compressor compressor;
  final private Decompressor decompressor;

  /**
   * @param compressionAlgorithmName What kind of algorithm should be used
   *                                 as baseline for comparison (e.g. lzo, gz).
   */
  public DataBlockEncodingTool(String compressionAlgorithmName) {
    this.compressionAlgorithmName = compressionAlgorithmName;
    this.compressionAlgorithm = Compression.getCompressionAlgorithmByName(
        compressionAlgorithmName);
    this.compressor = this.compressionAlgorithm.getCompressor();
    this.decompressor = this.compressionAlgorithm.getDecompressor();
  }

  /**
   * Check statistics for given HFile for different data block encoders.
   * @param scanner Of file which will be compressed.
   * @param kvLimit Maximal count of KeyValue which will be processed.
   * @throws IOException thrown if scanner is invalid
   */
  public void checkStatistics(final KeyValueScanner scanner, final int kvLimit)
      throws IOException {
    scanner.seek(KeyValue.LOWESTKEY);

    KeyValue currentKv;

    byte[] previousKey = null;
    byte[] currentKey;

    List<DataBlockEncoder> dataBlockEncoders =
        DataBlockEncoding.getAllEncoders();

    for (DataBlockEncoder d : dataBlockEncoders) {
      codecs.add(new EncodedDataBlock(d, includesMemstoreTS));
    }

    int j = 0;
    while ((currentKv = scanner.next()) != null && j < kvLimit) {
      // Iterates through key/value pairs
      j++;
      currentKey = currentKv.getKey();
      if (previousKey != null) {
        for (int i = 0; i < previousKey.length && i < currentKey.length &&
            previousKey[i] == currentKey[i]; ++i) {
          totalKeyRedundancyLength++;
        }
      }

      for (EncodedDataBlock codec : codecs) {
        codec.addKv(currentKv);
      }

      previousKey = currentKey;

      totalPrefixLength += currentKv.getLength() - currentKv.getKeyLength() -
          currentKv.getValueLength();
      totalKeyLength += currentKv.getKeyLength();
      totalValueLength += currentKv.getValueLength();
    }
  }

  /**
   * Verify if all data block encoders are working properly.
   * 
   * @param scanner Of file which was compressed.
   * @param kvLimit Maximal count of KeyValue which will be processed.
   * @return true if all data block encoders compressed/decompressed correctly.
   * @throws IOException thrown if scanner is invalid
   */
  public boolean verifyCodecs(final KeyValueScanner scanner, final int kvLimit)
      throws IOException {
    KeyValue currentKv;

    scanner.seek(KeyValue.LOWESTKEY);
    List<Iterator<KeyValue>> codecIterators =
        new ArrayList<Iterator<KeyValue>>();
    for(EncodedDataBlock codec : codecs) {
      codecIterators.add(codec.getIterator());
    }

    int j = 0;
    while ((currentKv = scanner.next()) != null && j < kvLimit) {
      // Iterates through key/value pairs
      ++j;
      for (Iterator<KeyValue> it : codecIterators) {
        KeyValue codecKv = it.next();
        if (codecKv == null || 0 != Bytes.compareTo(
            codecKv.getBuffer(), codecKv.getOffset(), codecKv.getLength(),
            currentKv.getBuffer(), currentKv.getOffset(),
            currentKv.getLength())) {
          if (codecKv == null) {
            LOG.error("There is a bug in codec " + it +
                " it returned null KeyValue,");
          } else {
            int prefix = 0;
            int limitLength = 2 * Bytes.SIZEOF_INT +
                Math.min(codecKv.getLength(), currentKv.getLength());
            while (prefix < limitLength &&
                codecKv.getBuffer()[prefix + codecKv.getOffset()] ==
                currentKv.getBuffer()[prefix + currentKv.getOffset()]) {
              prefix++;
            }

            LOG.error("There is bug in codec " + it.toString() +
                "\n on element " + j +
                "\n codecKv.getKeyLength() " + codecKv.getKeyLength() +
                "\n codecKv.getValueLength() " + codecKv.getValueLength() +
                "\n codecKv.getLength() " + codecKv.getLength() +
                "\n currentKv.getKeyLength() " + currentKv.getKeyLength() +
                "\n currentKv.getValueLength() " + currentKv.getValueLength() +
                "\n codecKv.getLength() " + currentKv.getLength() +
                "\n currentKV rowLength " + currentKv.getRowLength() +
                " familyName " + currentKv.getFamilyLength() +
                " qualifier " + currentKv.getQualifierLength() +
                "\n prefix " + prefix +
                "\n codecKv   '" + Bytes.toStringBinary(codecKv.getBuffer(),
                    codecKv.getOffset(), prefix) + "' diff '" +
                    Bytes.toStringBinary(codecKv.getBuffer(),
                        codecKv.getOffset() + prefix, codecKv.getLength() -
                        prefix) + "'" +
                "\n currentKv '" + Bytes.toStringBinary(
                   currentKv.getBuffer(),
                   currentKv.getOffset(), prefix) + "' diff '" +
                   Bytes.toStringBinary(currentKv.getBuffer(),
                       currentKv.getOffset() + prefix, currentKv.getLength() -
                       prefix) + "'"
                );
          }
          return false;
        }
      }
    }

    LOG.info("Verification was successful!");

    return true;
  }

  /**
   * Benchmark codec's speed.
   */
  public void benchmarkCodecs() {
    int prevTotalSize = -1;
    for (EncodedDataBlock codec : codecs) {
      prevTotalSize = benchmarkEncoder(prevTotalSize, codec);
    }

    byte[] buffer = codecs.get(0).getRawKeyValues();

    benchmarkDefaultCompression(prevTotalSize, buffer);
  }

  /**
   * Benchmark compression/decompression throughput.
   * @param previousTotalSize Total size used for verification. Use -1 if
   *          unknown.
   * @param codec Tested encoder.
   * @return Size of uncompressed data.
   */
  private int benchmarkEncoder(int previousTotalSize, EncodedDataBlock codec) {
    int prevTotalSize = previousTotalSize;
    int totalSize = 0;

    // decompression time
    List<Long> durations = new ArrayList<Long>();
    for (int itTime = 0; itTime < BENCHMARK_N_TIMES; ++itTime) {
      totalSize = 0;

      Iterator<KeyValue> it;

      it = codec.getIterator();

      // count only the algorithm time, without memory allocations
      // (expect first time)
      final long startTime = System.nanoTime();
      while (it.hasNext()) {
        totalSize += it.next().getLength();
      }
      final long finishTime = System.nanoTime();
      if (itTime >= BENCHMARK_N_OMIT) {
        durations.add(finishTime - startTime);
      }

      if (prevTotalSize != -1 && prevTotalSize != totalSize) {
        throw new IllegalStateException(String.format(
            "Algorithm '%s' decoded data to different size", codec.toString()));
      }
      prevTotalSize = totalSize;
    }

    // compression time
    List<Long> compressDurations = new ArrayList<Long>();
    for (int itTime = 0; itTime < BENCHMARK_N_TIMES; ++itTime) {
      final long startTime = System.nanoTime();
      codec.doCompressData();
      final long finishTime = System.nanoTime();
      if (itTime >= BENCHMARK_N_OMIT) {
        compressDurations.add(finishTime - startTime);
      }
    }

    System.out.println(codec.toString() + ":");
    printBenchmarkResult(totalSize, compressDurations, false);
    printBenchmarkResult(totalSize, durations, true);

    return prevTotalSize;
  }

  private void benchmarkDefaultCompression(int totalSize, byte[] rawBuffer) {
    benchmarkAlgorithm(compressionAlgorithm, compressor, decompressor,
        compressionAlgorithmName.toUpperCase(), rawBuffer, 0, totalSize);
  }

  /**
   * Check decompress performance of a given algorithm and print it.
   * @param algorithm Compression algorithm.
   * @param compressorCodec Compressor to be tested.
   * @param decompressorCodec Decompressor of the same algorithm.
   * @param name Name of algorithm.
   * @param buffer Buffer to be compressed.
   * @param offset Position of the beginning of the data.
   * @param length Length of data in buffer.
   */
  public static void benchmarkAlgorithm(
      Compression.Algorithm algorithm,
      Compressor compressorCodec,
      Decompressor decompressorCodec,
      String name,
      byte[] buffer, int offset, int length) {
    System.out.println(name + ":");

    // compress it
    List<Long> compressDurations = new ArrayList<Long>();
    ByteArrayOutputStream compressedStream = new ByteArrayOutputStream();
    OutputStream compressingStream;
    try {
      for (int itTime = 0; itTime < BENCHMARK_N_TIMES; ++itTime) {
        final long startTime = System.nanoTime();
        compressingStream = algorithm.createCompressionStream(
            compressedStream, compressorCodec, 0);
        compressingStream.write(buffer, offset, length);
        compressingStream.flush();
        compressedStream.toByteArray();

        final long finishTime = System.nanoTime();

        // add time record
        if (itTime >= BENCHMARK_N_OMIT) {
          compressDurations.add(finishTime - startTime);
        }

        if (itTime + 1 < BENCHMARK_N_TIMES) { // not the last one
          compressedStream.reset();
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(String.format(
          "Benchmark, or encoding algorithm '%s' cause some stream problems",
          name), e);
    }
    printBenchmarkResult(length, compressDurations, false);


    byte[] compBuffer = compressedStream.toByteArray();

    // uncompress it several times and measure performance
    List<Long> durations = new ArrayList<Long>();
    for (int itTime = 0; itTime < BENCHMARK_N_TIMES; ++itTime) {
      final long startTime = System.nanoTime();
      byte[] newBuf = new byte[length + 1];

      try {

        ByteArrayInputStream downStream = new ByteArrayInputStream(compBuffer,
            0, compBuffer.length);
        InputStream decompressedStream = algorithm.createDecompressionStream(
            downStream, decompressorCodec, 0);

        int destOffset = 0;
        int nextChunk;
        while ((nextChunk = decompressedStream.available()) > 0) {
          destOffset += decompressedStream.read(newBuf, destOffset, nextChunk);
        }
        decompressedStream.close();

        // iterate over KeyValue
        KeyValue kv;
        for (int pos = 0; pos < length; pos += kv.getLength()) {
          kv = new KeyValue(newBuf, pos);
        }

      } catch (IOException e) {
        throw new RuntimeException(String.format(
            "Decoding path in '%s' algorithm cause exception ", name), e);
      }

      final long finishTime = System.nanoTime();

      // check correctness
      if (0 != Bytes.compareTo(buffer, 0, length, newBuf, 0, length)) {
        int prefix = 0;
        for(; prefix < buffer.length && prefix < newBuf.length; ++prefix) {
          if (buffer[prefix] != newBuf[prefix]) {
            break;
          }
        }
        throw new RuntimeException(String.format(
            "Algorithm '%s' is corrupting the data", name));
      }

      // add time record
      if (itTime >= BENCHMARK_N_OMIT) {
        durations.add(finishTime - startTime);
      }
    }
    printBenchmarkResult(length, durations, true);
  }

  private static void printBenchmarkResult(int totalSize,
      List<Long> durationsInNanoSed, boolean isDecompression) {
    long meanTime = 0;
    for (long time : durationsInNanoSed) {
      meanTime += time;
    }
    meanTime /= durationsInNanoSed.size();

    long standardDev = 0;
    for (long time : durationsInNanoSed) {
      standardDev += (time - meanTime) * (time - meanTime);
    }
    standardDev = (long) Math.sqrt(standardDev / durationsInNanoSed.size());

    final double million = 1000.0 * 1000.0 * 1000.0;
    double mbPerSec = (totalSize * million) / (1024.0 * 1024.0 * meanTime);
    double mbPerSecDev = (totalSize * million) /
        (1024.0 * 1024.0 * (meanTime - standardDev));

    System.out.println(String.format(
        "  %s performance:%s %6.2f MB/s (+/- %.2f MB/s)",
        isDecompression ? "Decompression" : "Compression",
        isDecompression ? "" : "  ",
        mbPerSec, mbPerSecDev - mbPerSec));
  }

  /**
   * Display statistics of different compression algorithms.
   */
  public void displayStatistics() {
    int totalLength = totalPrefixLength + totalKeyLength + totalValueLength;
    if (compressor != null) {  // might be null e.g. for pure-Java GZIP 
      compressor.reset();
    }

    for(EncodedDataBlock codec : codecs) {
      System.out.println(codec.toString());
      int saved = totalKeyLength + totalPrefixLength + totalValueLength
          - codec.getSize();
      System.out.println(
          String.format("  Saved bytes:                 %8d", saved));
      double keyRatio = (saved * 100.0) / (totalPrefixLength + totalKeyLength);
      double allRatio = (saved * 100.0) / totalLength;
      System.out.println(
          String.format("  Key compression ratio:        %.2f %%", keyRatio));
      System.out.println(
          String.format("  All compression ratio:        %.2f %%", allRatio));

      String compressedSizeCaption =
          String.format("  %s compressed size:         ",
              compressionAlgorithmName.toUpperCase());
      String compressOnlyRatioCaption =
          String.format("  %s compression ratio:        ",
              compressionAlgorithmName.toUpperCase());

      if (compressor != null) {
        int compressedSize = codec.checkCompressedSize(compressor);
        System.out.println(compressedSizeCaption +
            String.format("%8d", compressedSize));
        double compressOnlyRatio =
            100.0 * (1.0 - compressedSize / (0.0 + totalLength));
        System.out.println(compressOnlyRatioCaption
            + String.format("%.2f %%", compressOnlyRatio));
      } else {
        System.out.println(compressedSizeCaption + "N/A");
        System.out.println(compressOnlyRatioCaption + "N/A");
      }
    }

    System.out.println(
        String.format("Total KV prefix length:   %8d", totalPrefixLength));
    System.out.println(
        String.format("Total key length:         %8d", totalKeyLength));
    System.out.println(
        String.format("Total key redundancy:     %8d",
            totalKeyRedundancyLength));
    System.out.println(
        String.format("Total value length:       %8d", totalValueLength));
  }

  /**
   * Test a data block encoder on the given HFile. Output results to console.
   * @param kvLimit The limit of KeyValue which will be analyzed.
   * @param hfilePath an HFile path on the file system.
   * @param compressionName Compression algorithm used for comparison.
   * @param doBenchmark Run performance benchmarks.
   * @param doVerify Verify correctness.
   * @throws IOException When pathName is incorrect.
   */
  public static void testCodecs(Configuration conf, int kvLimit,
      String hfilePath, String compressionName, boolean doBenchmark,
      boolean doVerify) throws IOException {
    // create environment
    Path path = new Path(hfilePath);
    CacheConfig cacheConf = new CacheConfig(conf);
    FileSystem fs = FileSystem.get(conf);
    StoreFile hsf = new StoreFile(fs, path, conf, cacheConf,
        StoreFile.BloomType.NONE, NoOpDataBlockEncoder.INSTANCE);

    StoreFile.Reader reader = hsf.createReader();
    reader.loadFileInfo();
    KeyValueScanner scanner = reader.getStoreFileScanner(true, true);

    // run the utilities
    DataBlockEncodingTool comp = new DataBlockEncodingTool(compressionName);
    comp.checkStatistics(scanner, kvLimit);
    if (doVerify) {
      comp.verifyCodecs(scanner, kvLimit);
    }
    if (doBenchmark) {
      comp.benchmarkCodecs();
    }
    comp.displayStatistics();

    // cleanup
    scanner.close();
    reader.close(cacheConf.shouldEvictOnClose());
  }

  private static void printUsage(Options options) {
    System.err.println("Usage:");
    System.err.println(String.format("./hbase %s <options>",
        DataBlockEncodingTool.class.getName()));
    System.err.println("Options:");
    for (Object it : options.getOptions()) {
      Option opt = (Option) it;
      if (opt.hasArg()) {
        System.err.println(String.format("-%s %s: %s", opt.getOpt(),
            opt.getArgName(), opt.getDescription()));
      } else {
        System.err.println(String.format("-%s: %s", opt.getOpt(),
            opt.getDescription()));
      }
    }
  }

  /**
   * A command line interface to benchmarks.
   * @param args Should have length at least 1 and holds the file path to HFile.
   * @throws IOException If you specified the wrong file.
   */
  public static void main(final String[] args) throws IOException {
    // set up user arguments
    Options options = new Options();
    options.addOption("f", true, "HFile to analyse (REQUIRED)");
    options.getOption("f").setArgName("FILENAME");
    options.addOption("n", true,
        "Limit number of KeyValue which will be analysed");
    options.getOption("n").setArgName("NUMBER");
    options.addOption("b", false, "Measure read throughput");
    options.addOption("c", false, "Omit corectness tests.");
    options.addOption("a", true,
        "What kind of compression algorithm use for comparison.");

    // parse arguments
    CommandLineParser parser = new PosixParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      System.err.println("Could not parse arguments!");
      System.exit(-1);
      return; // avoid warning
    }

    int kvLimit = Integer.MAX_VALUE;
    if (cmd.hasOption("n")) {
      kvLimit = Integer.parseInt(cmd.getOptionValue("n"));
    }

    // basic argument sanity checks
    if (!cmd.hasOption("f")) {
      System.err.println("ERROR: Filename is required!");
      printUsage(options);
      System.exit(-1);
    }

    String pathName = cmd.getOptionValue("f");
    String compressionName = DEFAULT_COMPRESSION.getName();
    if (cmd.hasOption("a")) {
      compressionName = cmd.getOptionValue("a").toLowerCase();
    }
    boolean doBenchmark = cmd.hasOption("b");
    boolean doVerify = !cmd.hasOption("c");

    final Configuration conf = HBaseConfiguration.create();
    try {
      testCodecs(conf, kvLimit, pathName, compressionName, doBenchmark,
          doVerify);
    } finally {
      (new CacheConfig(conf)).getBlockCache().shutdown();
    }
  }

}
