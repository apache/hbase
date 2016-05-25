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
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

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
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoder;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.encoding.EncodedDataBlock;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileBlock;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.io.hfile.HFileReaderV2;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.compress.CompressionOutputStream;
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
   * How many times to run the benchmark. More times means better data in terms
   * of statistics but slower execution. Has to be strictly larger than
   * {@link DEFAULT_BENCHMARK_N_OMIT}.
   */
  private static final int DEFAULT_BENCHMARK_N_TIMES = 12;

  /**
   * How many first runs should not be included in the benchmark. Done in order
   * to exclude setup cost.
   */
  private static final int DEFAULT_BENCHMARK_N_OMIT = 2;

  /** HFile name to be used in benchmark */
  private static final String OPT_HFILE_NAME = "f";

  /** Maximum number of key/value pairs to process in a single benchmark run */
  private static final String OPT_KV_LIMIT = "n";

  /** Whether to run a benchmark to measure read throughput */
  private static final String OPT_MEASURE_THROUGHPUT = "b";

  /** If this is specified, no correctness testing will be done */
  private static final String OPT_OMIT_CORRECTNESS_TEST = "c";

  /** What encoding algorithm to test */
  private static final String OPT_ENCODING_ALGORITHM = "a";

  /** Number of times to run each benchmark */
  private static final String OPT_BENCHMARK_N_TIMES = "t";

  /** Number of first runs of every benchmark to omit from statistics */
  private static final String OPT_BENCHMARK_N_OMIT = "omit";

  /** Compression algorithm to use if not specified on the command line */
  private static final Algorithm DEFAULT_COMPRESSION =
      Compression.Algorithm.GZ;

  private static final DecimalFormat DELIMITED_DECIMAL_FORMAT =
      new DecimalFormat();

  static {
    DELIMITED_DECIMAL_FORMAT.setGroupingSize(3);
  }

  private static final String PCT_FORMAT = "%.2f %%";
  private static final String INT_FORMAT = "%d";

  private static int benchmarkNTimes = DEFAULT_BENCHMARK_N_TIMES;
  private static int benchmarkNOmit = DEFAULT_BENCHMARK_N_OMIT;

  private List<EncodedDataBlock> codecs = new ArrayList<EncodedDataBlock>();
  private long totalPrefixLength = 0;
  private long totalKeyLength = 0;
  private long totalValueLength = 0;
  private long totalKeyRedundancyLength = 0;
  private long totalCFLength = 0;

  private byte[] rawKVs;
  private boolean useHBaseChecksum = false;

  private final String compressionAlgorithmName;
  private final Algorithm compressionAlgorithm;
  private final Compressor compressor;
  private final Decompressor decompressor;

  private static enum Manipulation {
    ENCODING,
    DECODING,
    COMPRESSION,
    DECOMPRESSION;

    @Override
    public String toString() {
      String s = super.toString();
      StringBuilder sb = new StringBuilder();
      sb.append(s.charAt(0));
      sb.append(s.substring(1).toLowerCase(Locale.ROOT));
      return sb.toString();
    }
  }

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

    KeyValue currentKV;

    byte[] previousKey = null;
    byte[] currentKey;

    DataBlockEncoding[] encodings = DataBlockEncoding.values();

    ByteArrayOutputStream uncompressedOutputStream =
        new ByteArrayOutputStream();

    int j = 0;
    while ((currentKV = scanner.next()) != null && j < kvLimit) {
      // Iterates through key/value pairs
      j++;
      currentKey = currentKV.getKey();
      if (previousKey != null) {
        for (int i = 0; i < previousKey.length && i < currentKey.length &&
            previousKey[i] == currentKey[i]; ++i) {
          totalKeyRedundancyLength++;
        }
      }

      uncompressedOutputStream.write(currentKV.getBuffer(),
          currentKV.getOffset(), currentKV.getLength());

      previousKey = currentKey;

      int kLen = currentKV.getKeyLength();
      int vLen = currentKV.getValueLength();
      int cfLen = currentKV.getFamilyLength(currentKV.getFamilyOffset());
      int restLen = currentKV.getLength() - kLen - vLen;

      totalKeyLength += kLen;
      totalValueLength += vLen;
      totalPrefixLength += restLen;
      totalCFLength += cfLen;
    }

    rawKVs = uncompressedOutputStream.toByteArray();
    boolean useTag = (currentKV.getTagsLengthUnsigned() > 0);
    for (DataBlockEncoding encoding : encodings) {
      if (encoding == DataBlockEncoding.NONE) {
        continue;
      }
      DataBlockEncoder d = encoding.getEncoder();
      HFileContext meta = new HFileContextBuilder()
                          .withCompression(Compression.Algorithm.NONE)
                          .withIncludesMvcc(includesMemstoreTS)
                          .withIncludesTags(useTag).build();
      codecs.add(new EncodedDataBlock(d, encoding, rawKVs, meta ));
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
    List<Iterator<Cell>> codecIterators =
        new ArrayList<Iterator<Cell>>();
    for(EncodedDataBlock codec : codecs) {
      codecIterators.add(codec.getIterator(HFileBlock.headerSize(useHBaseChecksum)));
    }

    int j = 0;
    while ((currentKv = scanner.next()) != null && j < kvLimit) {
      // Iterates through key/value pairs
      ++j;
      for (Iterator<Cell> it : codecIterators) {
        Cell c = it.next();
        KeyValue codecKv = KeyValueUtil.ensureKeyValue(c);
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
  public void benchmarkCodecs() throws IOException {
    LOG.info("Starting a throughput benchmark for data block encoding codecs");
    int prevTotalSize = -1;
    for (EncodedDataBlock codec : codecs) {
      prevTotalSize = benchmarkEncoder(prevTotalSize, codec);
    }

    benchmarkDefaultCompression(prevTotalSize, rawKVs);
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
    for (int itTime = 0; itTime < benchmarkNTimes; ++itTime) {
      totalSize = 0;

      Iterator<Cell> it;

      it = codec.getIterator(HFileBlock.headerSize(useHBaseChecksum));

      // count only the algorithm time, without memory allocations
      // (expect first time)
      final long startTime = System.nanoTime();
      while (it.hasNext()) {
        totalSize += KeyValueUtil.ensureKeyValue(it.next()).getLength();
      }
      final long finishTime = System.nanoTime();
      if (itTime >= benchmarkNOmit) {
        durations.add(finishTime - startTime);
      }

      if (prevTotalSize != -1 && prevTotalSize != totalSize) {
        throw new IllegalStateException(String.format(
            "Algorithm '%s' decoded data to different size", codec.toString()));
      }
      prevTotalSize = totalSize;
    }

    List<Long> encodingDurations = new ArrayList<Long>();
    for (int itTime = 0; itTime < benchmarkNTimes; ++itTime) {
      final long startTime = System.nanoTime();
      codec.encodeData();
      final long finishTime = System.nanoTime();
      if (itTime >= benchmarkNOmit) {
        encodingDurations.add(finishTime - startTime);
      }
    }

    System.out.println(codec.toString() + ":");
    printBenchmarkResult(totalSize, encodingDurations, Manipulation.ENCODING);
    printBenchmarkResult(totalSize, durations, Manipulation.DECODING);
    System.out.println();

    return prevTotalSize;
  }

  private void benchmarkDefaultCompression(int totalSize, byte[] rawBuffer)
      throws IOException {
    benchmarkAlgorithm(compressionAlgorithm,
        compressionAlgorithmName.toUpperCase(Locale.ROOT), rawBuffer, 0, totalSize);
  }

  /**
   * Check decompress performance of a given algorithm and print it.
   * @param algorithm Compression algorithm.
   * @param name Name of algorithm.
   * @param buffer Buffer to be compressed.
   * @param offset Position of the beginning of the data.
   * @param length Length of data in buffer.
   * @throws IOException
   */
  public void benchmarkAlgorithm(Compression.Algorithm algorithm, String name,
      byte[] buffer, int offset, int length) throws IOException {
    System.out.println(name + ":");

    // compress it
    List<Long> compressDurations = new ArrayList<Long>();
    ByteArrayOutputStream compressedStream = new ByteArrayOutputStream();
    CompressionOutputStream compressingStream =
        algorithm.createPlainCompressionStream(compressedStream, compressor);
    try {
      for (int itTime = 0; itTime < benchmarkNTimes; ++itTime) {
        final long startTime = System.nanoTime();
        compressingStream.resetState();
        compressedStream.reset();
        compressingStream.write(buffer, offset, length);
        compressingStream.flush();
        compressedStream.toByteArray();

        final long finishTime = System.nanoTime();

        // add time record
        if (itTime >= benchmarkNOmit) {
          compressDurations.add(finishTime - startTime);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(String.format(
          "Benchmark, or encoding algorithm '%s' cause some stream problems",
          name), e);
    }
    compressingStream.close();
    printBenchmarkResult(length, compressDurations, Manipulation.COMPRESSION);

    byte[] compBuffer = compressedStream.toByteArray();

    // uncompress it several times and measure performance
    List<Long> durations = new ArrayList<Long>();
    for (int itTime = 0; itTime < benchmarkNTimes; ++itTime) {
      final long startTime = System.nanoTime();
      byte[] newBuf = new byte[length + 1];

      try {
        ByteArrayInputStream downStream = new ByteArrayInputStream(compBuffer,
            0, compBuffer.length);
        InputStream decompressedStream = algorithm.createDecompressionStream(
            downStream, decompressor, 0);

        int destOffset = 0;
        int nextChunk;
        while ((nextChunk = decompressedStream.available()) > 0) {
          destOffset += decompressedStream.read(newBuf, destOffset, nextChunk);
        }
        decompressedStream.close();

        // iterate over KeyValues
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
      if (itTime >= benchmarkNOmit) {
        durations.add(finishTime - startTime);
      }
    }
    printBenchmarkResult(length, durations, Manipulation.DECOMPRESSION);
    System.out.println();
  }

  private static final double BYTES_IN_MB = 1024 * 1024.0;
  private static final double NS_IN_SEC = 1000.0 * 1000.0 * 1000.0;
  private static final double MB_SEC_COEF = NS_IN_SEC / BYTES_IN_MB;

  private static void printBenchmarkResult(int totalSize,
      List<Long> durationsInNanoSec, Manipulation manipulation) {
    final int n = durationsInNanoSec.size();
    long meanTime = 0;
    for (long time : durationsInNanoSec) {
      meanTime += time;
    }
    meanTime /= n;

    double meanMBPerSec = totalSize * MB_SEC_COEF / meanTime;
    double mbPerSecSTD = 0;
    if (n > 0) {
      for (long time : durationsInNanoSec) {
        double mbPerSec = totalSize * MB_SEC_COEF / time;
        double dev = mbPerSec - meanMBPerSec;
        mbPerSecSTD += dev * dev;
      }
      mbPerSecSTD = Math.sqrt(mbPerSecSTD / n);
    }

    outputTuple(manipulation + " performance", "%6.2f MB/s (+/- %.2f MB/s)",
         meanMBPerSec, mbPerSecSTD);
  }

  private static void outputTuple(String caption, String format,
      Object... values) {
    if (format.startsWith(INT_FORMAT)) {
      format = "%s" + format.substring(INT_FORMAT.length());
      values[0] = DELIMITED_DECIMAL_FORMAT.format(values[0]);
    }

    StringBuilder sb = new StringBuilder();
    sb.append("  ");
    sb.append(caption);
    sb.append(":");

    String v = String.format(format, values);
    int padding = 60 - sb.length() - v.length();
    for (int i = 0; i < padding; ++i) {
      sb.append(' ');
    }
    sb.append(v);
    System.out.println(sb);
  }

  /**
   * Display statistics of different compression algorithms.
   * @throws IOException
   */
  public void displayStatistics() throws IOException {
    final String comprAlgo = compressionAlgorithmName.toUpperCase(Locale.ROOT);
    long rawBytes = totalKeyLength + totalPrefixLength + totalValueLength;

    System.out.println("Raw data size:");
    outputTuple("Raw bytes", INT_FORMAT, rawBytes);
    outputTuplePct("Key bytes", totalKeyLength);
    outputTuplePct("Value bytes", totalValueLength);
    outputTuplePct("KV infrastructure", totalPrefixLength);
    outputTuplePct("CF overhead", totalCFLength);
    outputTuplePct("Total key redundancy", totalKeyRedundancyLength);

    int compressedSize = EncodedDataBlock.getCompressedSize(
        compressionAlgorithm, compressor, rawKVs, 0, rawKVs.length);
    outputTuple(comprAlgo + " only size", INT_FORMAT,
        compressedSize);
    outputSavings(comprAlgo + " only", compressedSize, rawBytes);
    System.out.println();

    for (EncodedDataBlock codec : codecs) {
      System.out.println(codec.toString());
      long encodedBytes = codec.getSize();
      outputTuple("Encoded bytes", INT_FORMAT, encodedBytes);
      outputSavings("Key encoding", encodedBytes - totalValueLength,
          rawBytes - totalValueLength);
      outputSavings("Total encoding", encodedBytes, rawBytes);

      int encodedCompressedSize = codec.getEncodedCompressedSize(
          compressionAlgorithm, compressor);
      outputTuple("Encoding + " + comprAlgo + " size", INT_FORMAT,
          encodedCompressedSize);
      outputSavings("Encoding + " + comprAlgo, encodedCompressedSize, rawBytes);
      outputSavings("Encoding with " + comprAlgo, encodedCompressedSize,
          compressedSize);

      System.out.println();
    }
  }

  private void outputTuplePct(String caption, long size) {
    outputTuple(caption, INT_FORMAT + " (" + PCT_FORMAT + ")",
        size, size * 100.0 / rawKVs.length);
  }

  private void outputSavings(String caption, long part, long whole) {
    double pct = 100.0 * (1 - 1.0 * part / whole);
    double times = whole * 1.0 / part;
    outputTuple(caption + " savings", PCT_FORMAT + " (%.2f x)",
        pct, times);
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
      BloomType.NONE);

    StoreFile.Reader reader = hsf.createReader();
    reader.loadFileInfo();
    KeyValueScanner scanner = reader.getStoreFileScanner(true, true);

    // run the utilities
    DataBlockEncodingTool comp = new DataBlockEncodingTool(compressionName);
    int majorVersion = reader.getHFileVersion();
    comp.useHBaseChecksum = majorVersion > 2
        || (majorVersion == 2 && reader.getHFileMinorVersion() >= HFileReaderV2.MINOR_VERSION_WITH_CHECKSUM);
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
   * A command line interface to benchmarks. Parses command-line arguments and
   * runs the appropriate benchmarks.
   * @param args Should have length at least 1 and holds the file path to HFile.
   * @throws IOException If you specified the wrong file.
   */
  public static void main(final String[] args) throws IOException {
    // set up user arguments
    Options options = new Options();
    options.addOption(OPT_HFILE_NAME, true, "HFile to analyse (REQUIRED)");
    options.getOption(OPT_HFILE_NAME).setArgName("FILENAME");
    options.addOption(OPT_KV_LIMIT, true,
        "Maximum number of KeyValues to process. A benchmark stops running " +
        "after iterating over this many KV pairs.");
    options.getOption(OPT_KV_LIMIT).setArgName("NUMBER");
    options.addOption(OPT_MEASURE_THROUGHPUT, false,
        "Measure read throughput");
    options.addOption(OPT_OMIT_CORRECTNESS_TEST, false,
        "Omit corectness tests.");
    options.addOption(OPT_ENCODING_ALGORITHM, true,
        "What kind of compression algorithm use for comparison.");
    options.addOption(OPT_BENCHMARK_N_TIMES,
        true, "Number of times to run each benchmark. Default value: " +
            DEFAULT_BENCHMARK_N_TIMES);
    options.addOption(OPT_BENCHMARK_N_OMIT, true,
        "Number of first runs of every benchmark to exclude from "
            + "statistics (" + DEFAULT_BENCHMARK_N_OMIT
            + " by default, so that " + "only the last "
            + (DEFAULT_BENCHMARK_N_TIMES - DEFAULT_BENCHMARK_N_OMIT)
            + " times are included in statistics.)");

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
    if (cmd.hasOption(OPT_KV_LIMIT)) {
      kvLimit = Integer.parseInt(cmd.getOptionValue(OPT_KV_LIMIT));
    }

    // basic argument sanity checks
    if (!cmd.hasOption(OPT_HFILE_NAME)) {
      LOG.error("Please specify HFile name using the " + OPT_HFILE_NAME
          + " option");
      printUsage(options);
      System.exit(-1);
    }

    String pathName = cmd.getOptionValue(OPT_HFILE_NAME);
    String compressionName = DEFAULT_COMPRESSION.getName();
    if (cmd.hasOption(OPT_ENCODING_ALGORITHM)) {
      compressionName =
          cmd.getOptionValue(OPT_ENCODING_ALGORITHM).toLowerCase(Locale.ROOT);
    }
    boolean doBenchmark = cmd.hasOption(OPT_MEASURE_THROUGHPUT);
    boolean doVerify = !cmd.hasOption(OPT_OMIT_CORRECTNESS_TEST);

    if (cmd.hasOption(OPT_BENCHMARK_N_TIMES)) {
      benchmarkNTimes = Integer.valueOf(cmd.getOptionValue(
          OPT_BENCHMARK_N_TIMES));
    }
    if (cmd.hasOption(OPT_BENCHMARK_N_OMIT)) {
      benchmarkNOmit =
          Integer.valueOf(cmd.getOptionValue(OPT_BENCHMARK_N_OMIT));
    }
    if (benchmarkNTimes < benchmarkNOmit) {
      LOG.error("The number of times to run each benchmark ("
          + benchmarkNTimes
          + ") must be greater than the number of benchmark runs to exclude "
          + "from statistics (" + benchmarkNOmit + ")");
      System.exit(1);
    }
    LOG.info("Running benchmark " + benchmarkNTimes + " times. " +
        "Excluding the first " + benchmarkNOmit + " times from statistics.");

    final Configuration conf = HBaseConfiguration.create();
    try {
      testCodecs(conf, kvLimit, pathName, compressionName, doBenchmark,
          doVerify);
    } finally {
      (new CacheConfig(conf)).getBlockCache().shutdown();
    }
  }

}
