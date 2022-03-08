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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.math3.random.RandomData;
import org.apache.commons.math3.random.RandomDataImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.crypto.CryptoCipherProvider;
import org.apache.hadoop.hbase.io.crypto.DefaultCipherProvider;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.KeyProviderForTesting;
import org.apache.hadoop.hbase.io.crypto.aes.AES;
import org.apache.hadoop.hbase.io.hfile.HFileWriterImpl;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * This class runs performance benchmarks for {@link HFile}.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
public class HFilePerformanceEvaluation {
  private static final int ROW_LENGTH = 10;
  private static final int ROW_COUNT = 1000000;
  private static final int RFILE_BLOCKSIZE = 8 * 1024;
  private static StringBuilder testSummary = new StringBuilder();
  
  // Disable verbose INFO logging from org.apache.hadoop.io.compress.CodecPool
  static {
    System.setProperty("org.apache.commons.logging.Log", 
      "org.apache.commons.logging.impl.SimpleLog");
    System.setProperty("org.apache.commons.logging.simplelog.log.org.apache.hadoop.io.compress.CodecPool",
      "WARN");
  }
  
  private static final Logger LOG =
    LoggerFactory.getLogger(HFilePerformanceEvaluation.class.getName());

  static byte [] format(final int i) {
    String v = Integer.toString(i);
    return Bytes.toBytes("0000000000".substring(v.length()) + v);
  }

  static ImmutableBytesWritable format(final int i, ImmutableBytesWritable w) {
    w.set(format(i));
    return w;
  }

  static Cell createCell(final int i) {
    return createCell(i, HConstants.EMPTY_BYTE_ARRAY);
  }

  /**
   * HFile is Cell-based. It used to be byte arrays.  Doing this test, pass Cells. All Cells
   * intentionally have same coordinates in all fields but row.
   * @param i Integer to format as a row Key.
   * @param value Value to use
   * @return Created Cell.
   */
  static Cell createCell(final int i, final byte [] value) {
    return createCell(format(i), value);
  }

  static Cell createCell(final byte [] keyRow) {
    return CellUtil.createCell(keyRow);
  }

  static Cell createCell(final byte [] keyRow, final byte [] value) {
    return CellUtil.createCell(keyRow, value);
  }

  /**
   * Add any supported codec or cipher to test the HFile read/write performance. 
   * Specify "none" to disable codec or cipher or both.  
   * @throws Exception
   */
  private void runBenchmarks() throws Exception {
    final Configuration conf = new Configuration();
    final FileSystem fs = FileSystem.get(conf);
    final Path mf = fs.makeQualified(new Path("performanceevaluation.mapfile"));
    
    // codec=none cipher=none
    runWriteBenchmark(conf, fs, mf, "none", "none");
    runReadBenchmark(conf, fs, mf, "none", "none");
    
    // codec=gz cipher=none
    runWriteBenchmark(conf, fs, mf, "gz", "none");
    runReadBenchmark(conf, fs, mf, "gz", "none");

    // Add configuration for AES cipher
    final Configuration aesconf = new Configuration();
    aesconf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, KeyProviderForTesting.class.getName());
    aesconf.set(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY, "hbase");
    aesconf.setInt("hfile.format.version", 3);
    final FileSystem aesfs = FileSystem.get(aesconf);
    final Path aesmf = aesfs.makeQualified(new Path("performanceevaluation.aes.mapfile"));

    // codec=none cipher=aes
    runWriteBenchmark(aesconf, aesfs, aesmf, "none", "aes");
    runReadBenchmark(aesconf, aesfs, aesmf, "none", "aes");

    // codec=gz cipher=aes
    runWriteBenchmark(aesconf, aesfs, aesmf, "gz", "aes");
    runReadBenchmark(aesconf, aesfs, aesmf, "gz", "aes");

    // Add configuration for Commons cipher
    final Configuration cryptoconf = new Configuration();
    cryptoconf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, KeyProviderForTesting.class.getName());
    cryptoconf.set(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY, "hbase");
    cryptoconf.setInt("hfile.format.version", 3);
    cryptoconf.set(HConstants.CRYPTO_CIPHERPROVIDER_CONF_KEY, CryptoCipherProvider.class.getName());
    final FileSystem cryptofs = FileSystem.get(cryptoconf);
    final Path cryptof = cryptofs.makeQualified(new Path("performanceevaluation.aes.mapfile"));

    // codec=none cipher=aes
    runWriteBenchmark(cryptoconf, cryptofs, aesmf, "none", "aes");
    runReadBenchmark(cryptoconf, cryptofs, aesmf, "none", "aes");

    // codec=gz cipher=aes
    runWriteBenchmark(cryptoconf, aesfs, aesmf, "gz", "aes");
    runReadBenchmark(cryptoconf, aesfs, aesmf, "gz", "aes");

    // cleanup test files
    if (fs.exists(mf)) {
      fs.delete(mf, true);
    }
    if (aesfs.exists(aesmf)) {
      aesfs.delete(aesmf, true);
    }
    if (cryptofs.exists(aesmf)) {
      cryptofs.delete(cryptof, true);
    }

    // Print Result Summary
    LOG.info("\n***************\n" + "Result Summary" + "\n***************\n");
    LOG.info(testSummary.toString());

  }

  /**
   * Write a test HFile with the given codec & cipher
   * @param conf
   * @param fs
   * @param mf
   * @param codec "none", "lzo", "gz", "snappy"
   * @param cipher "none", "aes"
   * @throws Exception
   */
  private void runWriteBenchmark(Configuration conf, FileSystem fs, Path mf, String codec,
      String cipher) throws Exception {
    if (fs.exists(mf)) {
      fs.delete(mf, true);
    }

    runBenchmark(new SequentialWriteBenchmark(conf, fs, mf, ROW_COUNT, codec, cipher),
        ROW_COUNT, codec, getCipherName(conf, cipher));

  }

  /**
   * Run all the read benchmarks for the test HFile 
   * @param conf
   * @param fs
   * @param mf
   * @param codec "none", "lzo", "gz", "snappy"
   * @param cipher "none", "aes"
   */
  private void runReadBenchmark(final Configuration conf, final FileSystem fs, final Path mf,
      final String codec, final String cipher) {
    PerformanceEvaluationCommons.concurrentReads(new Runnable() {
      @Override
      public void run() {
        try {
          runBenchmark(new UniformRandomSmallScan(conf, fs, mf, ROW_COUNT),
            ROW_COUNT, codec, getCipherName(conf, cipher));
        } catch (Exception e) {
          testSummary.append("UniformRandomSmallScan failed " + e.getMessage());
          e.printStackTrace();
        }
      }
    });
    
    PerformanceEvaluationCommons.concurrentReads(new Runnable() {
      @Override
      public void run() {
        try {
          runBenchmark(new UniformRandomReadBenchmark(conf, fs, mf, ROW_COUNT),
              ROW_COUNT, codec, getCipherName(conf, cipher));
        } catch (Exception e) {
          testSummary.append("UniformRandomReadBenchmark failed " + e.getMessage());
          e.printStackTrace();
        }
      }
    });
    
    PerformanceEvaluationCommons.concurrentReads(new Runnable() {
      @Override
      public void run() {
        try {
          runBenchmark(new GaussianRandomReadBenchmark(conf, fs, mf, ROW_COUNT),
              ROW_COUNT, codec, getCipherName(conf, cipher));
        } catch (Exception e) {
          testSummary.append("GaussianRandomReadBenchmark failed " + e.getMessage());
          e.printStackTrace();
        }
      }
    });
    
    PerformanceEvaluationCommons.concurrentReads(new Runnable() {
      @Override
      public void run() {
        try {
          runBenchmark(new SequentialReadBenchmark(conf, fs, mf, ROW_COUNT),
              ROW_COUNT, codec, getCipherName(conf, cipher));
        } catch (Exception e) {
          testSummary.append("SequentialReadBenchmark failed " + e.getMessage());
          e.printStackTrace();
        }
      }
    });    

  }
  
  protected void runBenchmark(RowOrientedBenchmark benchmark, int rowCount,
      String codec, String cipher) throws Exception {
    LOG.info("Running " + benchmark.getClass().getSimpleName() + " with codec[" + 
        codec + "] " + "cipher[" + cipher + "] for " + rowCount + " rows.");
    
    long elapsedTime = benchmark.run();
    
    LOG.info("Running " + benchmark.getClass().getSimpleName() + " with codec[" + 
        codec + "] " + "cipher[" + cipher + "] for " + rowCount + " rows took " + 
        elapsedTime + "ms.");
    
    // Store results to print summary at the end
    testSummary.append("Running ").append(benchmark.getClass().getSimpleName())
        .append(" with codec[").append(codec).append("] cipher[").append(cipher)
        .append("] for ").append(rowCount).append(" rows took ").append(elapsedTime)
        .append("ms.").append("\n");
  }

  static abstract class RowOrientedBenchmark {

    protected final Configuration conf;
    protected final FileSystem fs;
    protected final Path mf;
    protected final int totalRows;
    protected String codec = "none";
    protected String cipher = "none";

    public RowOrientedBenchmark(Configuration conf, FileSystem fs, Path mf,
        int totalRows, String codec, String cipher) {
      this.conf = conf;
      this.fs = fs;
      this.mf = mf;
      this.totalRows = totalRows;
      this.codec = codec;
      this.cipher = cipher;
    }

    public RowOrientedBenchmark(Configuration conf, FileSystem fs, Path mf,
        int totalRows) {
      this.conf = conf;
      this.fs = fs;
      this.mf = mf;
      this.totalRows = totalRows;
    }

    void setUp() throws Exception {
      // do nothing
    }

    abstract void doRow(int i) throws Exception;

    protected int getReportingPeriod() {
      return this.totalRows / 10;
    }

    void tearDown() throws Exception {
      // do nothing
    }

    /**
     * Run benchmark
     * @return elapsed time.
     * @throws Exception
     */
    long run() throws Exception {
      long elapsedTime;
      setUp();
      long startTime = System.currentTimeMillis();
      try {
        for (int i = 0; i < totalRows; i++) {
          if (i > 0 && i % getReportingPeriod() == 0) {
            LOG.info("Processed " + i + " rows.");
          }
          doRow(i);
        }
        elapsedTime = System.currentTimeMillis() - startTime;
      } finally {
        tearDown();
      }
      return elapsedTime;
    }

  }

  static class SequentialWriteBenchmark extends RowOrientedBenchmark {
    protected HFile.Writer writer;
    private byte[] bytes = new byte[ROW_LENGTH];

    public SequentialWriteBenchmark(Configuration conf, FileSystem fs, Path mf,
        int totalRows, String codec, String cipher) {
      super(conf, fs, mf, totalRows, codec, cipher);
    }

    @Override
    void setUp() throws Exception {

      HFileContextBuilder builder = new HFileContextBuilder()
          .withCompression(HFileWriterImpl.compressionByName(codec))
          .withBlockSize(RFILE_BLOCKSIZE);
      
      if (cipher == "aes") {
        byte[] cipherKey = new byte[AES.KEY_LENGTH];
        Bytes.secureRandom(cipherKey);
        builder.withEncryptionContext(Encryption.newContext(conf)
            .setCipher(Encryption.getCipher(conf, cipher))
            .setKey(cipherKey));
      } else if (!"none".equals(cipher)) {
        throw new IOException("Cipher " + cipher + " not supported.");
      }
      
      HFileContext hFileContext = builder.build();

      writer = HFile.getWriterFactoryNoCache(conf)
          .withPath(fs, mf)
          .withFileContext(hFileContext)
          .create();
    }
    
    @Override
    void doRow(int i) throws Exception {
      writer.append(createCell(i, generateValue()));
    }

    private byte[] generateValue() {
      Bytes.random(bytes);
      return bytes;
    }

    @Override
    protected int getReportingPeriod() {
      return this.totalRows; // don't report progress
    }

    @Override
    void tearDown() throws Exception {
      writer.close();
    }

  }

  static abstract class ReadBenchmark extends RowOrientedBenchmark {

    protected HFile.Reader reader;

    public ReadBenchmark(Configuration conf, FileSystem fs, Path mf,
        int totalRows) {
      super(conf, fs, mf, totalRows);
    }

    @Override
    void setUp() throws Exception {
      reader = HFile.createReader(this.fs, this.mf, new CacheConfig(this.conf), true, this.conf);
    }

    @Override
    void tearDown() throws Exception {
      reader.close();
    }

  }

  static class SequentialReadBenchmark extends ReadBenchmark {
    private HFileScanner scanner;

    public SequentialReadBenchmark(Configuration conf, FileSystem fs,
      Path mf, int totalRows) {
      super(conf, fs, mf, totalRows);
    }

    @Override
    void setUp() throws Exception {
      super.setUp();
      this.scanner = this.reader.getScanner(false, false);
      this.scanner.seekTo();
    }

    @Override
    void doRow(int i) throws Exception {
      if (this.scanner.next()) {
        // TODO: Fix. Make Scanner do Cells.
        Cell c = this.scanner.getCell();
        PerformanceEvaluationCommons.assertKey(format(i + 1), c);
        PerformanceEvaluationCommons.assertValueSize(ROW_LENGTH, c.getValueLength());
      }
    }

    @Override
    protected int getReportingPeriod() {
      return this.totalRows; // don't report progress
    }

  }

  static class UniformRandomReadBenchmark extends ReadBenchmark {

    public UniformRandomReadBenchmark(Configuration conf, FileSystem fs,
        Path mf, int totalRows) {
      super(conf, fs, mf, totalRows);
    }

    @Override
    void doRow(int i) throws Exception {
      HFileScanner scanner = this.reader.getScanner(false, true);
      byte [] b = getRandomRow();
      if (scanner.seekTo(createCell(b)) < 0) {
        LOG.info("Not able to seekTo " + new String(b));
        return;
      }
      // TODO: Fix scanner so it does Cells
      Cell c = scanner.getCell();
      PerformanceEvaluationCommons.assertKey(b, c);
      PerformanceEvaluationCommons.assertValueSize(ROW_LENGTH, c.getValueLength());
    }

    private byte [] getRandomRow() {
      return format(ThreadLocalRandom.current().nextInt(totalRows));
    }
  }

  static class UniformRandomSmallScan extends ReadBenchmark {

    public UniformRandomSmallScan(Configuration conf, FileSystem fs,
        Path mf, int totalRows) {
      super(conf, fs, mf, totalRows/10);
    }

    @Override
    void doRow(int i) throws Exception {
      HFileScanner scanner = this.reader.getScanner(false, false);
      byte [] b = getRandomRow();
      // System.out.println("Random row: " + new String(b));
      Cell c = createCell(b);
      if (scanner.seekTo(c) != 0) {
        LOG.info("Nonexistent row: " + new String(b));
        return;
      }
      // TODO: HFileScanner doesn't do Cells yet. Temporary fix.
      c = scanner.getCell();
      // System.out.println("Found row: " +
      //  new String(c.getRowArray(), c.getRowOffset(), c.getRowLength()));
      PerformanceEvaluationCommons.assertKey(b, c);
      for (int ii = 0; ii < 30; ii++) {
        if (!scanner.next()) {
          LOG.info("NOTHING FOLLOWS");
          return;
        }
        c = scanner.getCell();
        PerformanceEvaluationCommons.assertValueSize(ROW_LENGTH, c.getValueLength());
      }
    }

    private byte [] getRandomRow() {
      return format(ThreadLocalRandom.current().nextInt(totalRows));
    }
  }

  static class GaussianRandomReadBenchmark extends ReadBenchmark {

    private RandomData randomData = new RandomDataImpl();

    public GaussianRandomReadBenchmark(Configuration conf, FileSystem fs,
        Path mf, int totalRows) {
      super(conf, fs, mf, totalRows);
    }

    @Override
    void doRow(int i) throws Exception {
      HFileScanner scanner = this.reader.getScanner(false, true);
      byte[] gaussianRandomRowBytes = getGaussianRandomRowBytes();
      scanner.seekTo(createCell(gaussianRandomRowBytes));
      for (int ii = 0; ii < 30; ii++) {
        if (!scanner.next()) {
          LOG.info("NOTHING FOLLOWS");
          return;
        }
        // TODO: Fix. Make scanner do Cells.
        scanner.getCell();
      }
    }

    private byte [] getGaussianRandomRowBytes() {
      int r = (int) randomData.nextGaussian((double)totalRows / 2.0,
          (double)totalRows / 10.0);
      // make sure r falls into [0,totalRows)
      return format(Math.min(totalRows, Math.max(r,0)));
    }
  }

  /**
   * @param args
   * @throws Exception
   * @throws IOException
   */
  public static void main(String[] args) throws Exception {
    new HFilePerformanceEvaluation().runBenchmarks();
  }

  private String getCipherName(Configuration conf, String cipherName) {
    if (cipherName.equals("aes")) {
      String provider = conf.get(HConstants.CRYPTO_CIPHERPROVIDER_CONF_KEY);
      if (provider == null || provider.equals("")
              || provider.equals(DefaultCipherProvider.class.getName())) {
        return "aes-default";
      } else if (provider.equals(CryptoCipherProvider.class.getName())) {
        return "aes-commons";
      }
    }
    return cipherName;
  }
}
