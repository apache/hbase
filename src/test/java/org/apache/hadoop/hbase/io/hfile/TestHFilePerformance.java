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
package org.apache.hadoop.hbase.io.hfile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.junit.experimental.categories.Category;

class RawKeyValue implements Comparable<RawKeyValue> {
  private final byte[] key;
  private final byte[] value;

  public RawKeyValue(byte[] key, byte[] value) {
    super();
    this.key = key.clone();
    this.value = value.clone();
  }

  public byte[] getKey() {
    return key;
  }

  public byte[] getValue() {
    return value;
  }

  @Override
  public int compareTo(RawKeyValue o) {
    return Bytes.compareTo(key, o.key);
  }
}

/**
 *  Set of long-running tests to measure performance of HFile.
 * <p>
 * Copied from
 * <a href="https://issues.apache.org/jira/browse/HADOOP-3315">hadoop-3315 tfile</a>.
 * Remove after tfile is committed and use the tfile version of this class
 * instead.</p>
 */
@Category(MediumTests.class)
public class TestHFilePerformance extends TestCase {
  private static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static String ROOT_DIR =
    TEST_UTIL.getTestDir("TestHFilePerformance").toString();
  private FileSystem fs;
  private Configuration conf;
  private long startTimeEpoch;
  private long finishTimeEpoch;
  private DateFormat formatter;

  @Override
  public void setUp() throws IOException {
    conf = new Configuration();
    fs = FileSystem.get(conf);
    formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  }

  public void startTime() {
    startTimeEpoch = System.currentTimeMillis();
    System.out.println(formatTime() + " Started timing.");
  }

  public void stopTime() {
    finishTimeEpoch = System.currentTimeMillis();
    System.out.println(formatTime() + " Stopped timing.");
  }

  public long getIntervalMillis() {
    return finishTimeEpoch - startTimeEpoch;
  }

  public void printlnWithTimestamp(String message) {
    System.out.println(formatTime() + "  " +  message);
  }

  /*
   * Format millis into minutes and seconds.
   */
  public String formatTime(long milis){
    return formatter.format(milis);
  }

  public String formatTime(){
    return formatTime(System.currentTimeMillis());
  }

  private FSDataOutputStream createFSOutput(Path name) throws IOException {
    if (fs.exists(name))
      fs.delete(name, true);
    FSDataOutputStream fout = fs.create(name);
    return fout;
  }

  //TODO have multiple ways of generating key/value e.g. dictionary words
  //TODO to have a sample compressable data, for now, made 1 out of 3 values random
  //     keys are all random.

  private static class KeyValueGenerator {
    Random keyRandomizer;
    Random valueRandomizer;
    int randomPartsRatio = 10; // 1 out of randomValueRatio generated values will be random.
    long valueSequence = 0;

    byte[][] randomParts;
    final int randomPartsCount = 64;
    final int randomPartsMinLength = 8;
    final int randomPartsMaxLength = 64;
    final int randomPartsDiffLength = 1 + randomPartsMaxLength -
        randomPartsMinLength;


    KeyValueGenerator() {
      keyRandomizer = new Random(0L); //TODO with seed zero
      valueRandomizer = new Random(1L); //TODO with seed one
      randomParts = new byte[randomPartsCount][];

      for (int i = 0 ; i < randomParts.length ; ++i) {
        int length = valueRandomizer.nextInt(randomPartsDiffLength) +
            randomPartsMinLength;
        randomParts[i] = new byte[length];
        valueRandomizer.nextBytes(randomParts[i]);
      }
    }

    // Key is always random now.
    void getKey(byte[] key) {
      keyRandomizer.nextBytes(key);
    }

    void getValue(byte[] value) {
      for (int pos = 0 ; pos < value.length ; ) {
        byte[] part;
        if (valueRandomizer.nextInt(randomPartsRatio) + 1 ==
            randomPartsRatio) {
          int length = valueRandomizer.nextInt(randomPartsDiffLength) +
              randomPartsMinLength;
          part = new byte[length];
          valueRandomizer.nextBytes(part);
        } else {
          int partId = valueRandomizer.nextInt(randomParts.length);
          part = randomParts[partId];
        }
        int copyLength = Math.min(part.length, value.length - pos);
        System.arraycopy(part, 0, value, pos, copyLength);
        pos += copyLength;
      }
      valueSequence++;
    }
  }

  /**
   *
   * @param fileType "HFile" or "SequenceFile"
   * @param keyLength
   * @param valueLength
   * @param codecName "none", "lzo", "gz"
   * @param rows number of rows to be written.
   * @param writeMethod used for HFile only.
   * @param minBlockSize used for HFile only.
   * @throws IOException
   */
   //TODO writeMethod: implement multiple ways of writing e.g. A) known length (no chunk) B) using a buffer and streaming (for many chunks).
  public void timeWrite(String fileType, List<RawKeyValue> testKeyValues,
    String codecName, String writeMethod, int minBlockSize)
  throws IOException {
    System.out.println("File Type: " + fileType);
    System.out.println("Writing " + fileType + " with codecName: " + codecName);
    long totalBytesWritten = 0;

    startTime();

    Path path = new Path(ROOT_DIR, fileType + ".Performance");
    System.out.println(ROOT_DIR + path.getName());
    FSDataOutputStream fout =  createFSOutput(path);

    if ("HFile".equals(fileType)){
        System.out.println("HFile write method: ");
        HFile.Writer writer = HFile.getWriterFactoryNoCache(conf)
            .withOutputStream(fout)
            .withBlockSize(minBlockSize)
            .withCompression(codecName)
            .create();

        // Writing value in one shot.
        for (RawKeyValue rawKV : testKeyValues) {
          writer.append(rawKV.getKey(), rawKV.getValue());
          totalBytesWritten += rawKV.getKey().length;
          totalBytesWritten += rawKV.getValue().length;
         }
        writer.close();
    } else if ("SequenceFile".equals(fileType)){
        CompressionCodec codec = null;
        if ("gz".equals(codecName))
          codec = new GzipCodec();
        else if (!"none".equals(codecName))
          throw new IOException("Codec not supported.");

        SequenceFile.Writer writer;

        //TODO
        //JobConf conf = new JobConf();

        if (!"none".equals(codecName))
          writer = SequenceFile.createWriter(conf, fout, BytesWritable.class,
            BytesWritable.class, SequenceFile.CompressionType.BLOCK, codec);
        else
          writer = SequenceFile.createWriter(conf, fout, BytesWritable.class,
            BytesWritable.class, SequenceFile.CompressionType.NONE, null);

        BytesWritable keyBsw;
        BytesWritable valBsw;
        for (RawKeyValue rawKV : testKeyValues) {
           keyBsw = new BytesWritable(rawKV.getKey());
           totalBytesWritten += keyBsw.getSize();

           valBsw = new BytesWritable(rawKV.getValue());
           writer.append(keyBsw, valBsw);
           totalBytesWritten += valBsw.getSize();
        }

        writer.close();
    } else
       throw new IOException("File Type is not supported");

    fout.close();
    stopTime();

    printlnWithTimestamp("Data written: ");
    printlnWithTimestamp("  rate  = " +
      totalBytesWritten / getIntervalMillis() * 1000 / 1024 / 1024 + "MB/s");
    printlnWithTimestamp("  total = " + totalBytesWritten + "B");

    printlnWithTimestamp("File written: ");
    printlnWithTimestamp("  rate  = " +
      fs.getFileStatus(path).getLen() / getIntervalMillis() * 1000 / 1024 / 1024 + "MB/s");
    printlnWithTimestamp("  total = " + fs.getFileStatus(path).getLen() + "B");
  }

  private List<RawKeyValue> generateDataset(int keyLength, int valueLength,
      long rows, KeyValueGenerator generator) {
    byte[] key = new byte[keyLength];
    byte[] value = new byte[valueLength];
    List<RawKeyValue> testKeyValues = new ArrayList<RawKeyValue>((int) rows);
    for (long l=0 ; l<rows ; l++ ) {
      generator.getKey(key);
      generator.getValue(value);
      testKeyValues.add(new RawKeyValue(
          key, value));
    }
    Collections.sort(testKeyValues);
    return testKeyValues;
  }

  public void timeReading(String fileType,
      long rows, int method) throws IOException {
    System.out.println("Reading file of type: " + fileType);
    Path path = new Path(ROOT_DIR, fileType + ".Performance");
    System.out.println("Input file size: " + fs.getFileStatus(path).getLen());
    long totalBytesRead = 0;


    ByteBuffer val;

    ByteBuffer key;

    startTime();
    FSDataInputStream fin = fs.open(path);

    if ("HFile".equals(fileType)){
        HFile.Reader reader = HFile.createReaderFromStream(path, fs.open(path),
          fs.getFileStatus(path).getLen(), new CacheConfig(conf));
        reader.loadFileInfo();
        switch (method) {

          case 0:
          case 1:
          default:
            {
              HFileScanner scanner = reader.getScanner(false, false, false);
              scanner.seekTo();
              for (long l=0; l<rows; l++ ) {
                key = scanner.getKey();
                val = scanner.getValue();
                totalBytesRead += key.limit() + val.limit();
                scanner.next();
              }
            }
            break;
        }
    } else if("SequenceFile".equals(fileType)){

        SequenceFile.Reader reader;
        reader = new SequenceFile.Reader(fs, path, new Configuration());

        if (reader.getCompressionCodec() != null) {
            printlnWithTimestamp("Compression codec class: " + reader.getCompressionCodec().getClass());
        } else
            printlnWithTimestamp("Compression codec class: " + "none");

        BytesWritable keyBsw = new BytesWritable();
        BytesWritable valBsw = new BytesWritable();

        for (long l=0; l<rows; l++ ) {
          reader.next(keyBsw, valBsw);
          totalBytesRead += keyBsw.getSize() + valBsw.getSize();
        }
        reader.close();

        //TODO make a tests for other types of SequenceFile reading scenarios

    } else {
        throw new IOException("File Type not supported.");
    }


    //printlnWithTimestamp("Closing reader");
    fin.close();
    stopTime();
    //printlnWithTimestamp("Finished close");

    printlnWithTimestamp("Finished in " + getIntervalMillis() + "ms");
    printlnWithTimestamp("Data read: ");
    printlnWithTimestamp("  rate  = " +
      totalBytesRead / getIntervalMillis() * 1000 / 1024 / 1024 + "MB/s");
    printlnWithTimestamp("  total = " + totalBytesRead + "B");

    printlnWithTimestamp("File read: ");
    printlnWithTimestamp("  rate  = " +
      fs.getFileStatus(path).getLen() / getIntervalMillis() * 1000 / 1024 / 1024 + "MB/s");
    printlnWithTimestamp("  total = " + fs.getFileStatus(path).getLen() + "B");

    //TODO uncomment this for final committing so test files is removed.
    //fs.delete(path, true);
  }

  public void testRunComparisons() throws IOException {

    int keyLength = 100; // 100B
    int valueLength = 5*1024; // 5KB
    int minBlockSize = 10*1024*1024; // 10MB
    int rows = 10000;

    //Using separate randomizer for key/value with seeds matching Sequence File.
    KeyValueGenerator generator = new KeyValueGenerator();
    List<RawKeyValue> testKeyValues = generateDataset(keyLength, valueLength,
        rows, generator);

    System.out.println("****************************** Sequence File *****************************");

    timeWrite("SequenceFile", testKeyValues, "none", null, minBlockSize);
    System.out.println("\n+++++++\n");
    timeReading("SequenceFile", rows, -1);

    System.out.println("");
    System.out.println("----------------------");
    System.out.println("");

    /* DISABLED LZO
    timeWrite("SequenceFile", testKeyValues, "lzo", null, minBlockSize);
    System.out.println("\n+++++++\n");
    timeReading("SequenceFile", rows, -1);

    System.out.println("");
    System.out.println("----------------------");
    System.out.println("");

    /* Sequence file can only use native hadoop libs gzipping so commenting out.
     */
    try {
      timeWrite("SequenceFile", testKeyValues, "gz", null, minBlockSize);
      System.out.println("\n+++++++\n");
      timeReading("SequenceFile", rows, -1);
    } catch (IllegalArgumentException e) {
      System.out.println("Skipping sequencefile gz: " + e.getMessage());
    }


    System.out.println("\n\n\n");
    System.out.println("****************************** HFile *****************************");

    timeWrite("HFile", testKeyValues, "none", null, minBlockSize);
    System.out.println("\n+++++++\n");
    timeReading("HFile", rows, 0 );

    System.out.println("");
    System.out.println("----------------------");
    System.out.println("");
/* DISABLED LZO
    timeWrite("HFile", testKeyValues, "lzo", null, minBlockSize);
    System.out.println("\n+++++++\n");
    timeReading("HFile", keyLength, valueLength, rows, 0 );
    System.out.println("\n+++++++\n");
    timeReading("HFile", keyLength, valueLength, rows, 1 );
    System.out.println("\n+++++++\n");
    timeReading("HFile", keyLength, valueLength, rows, 2 );

    System.out.println("");
    System.out.println("----------------------");
    System.out.println("");
*/
    timeWrite("HFile", testKeyValues, "gz", null, minBlockSize);
    System.out.println("\n+++++++\n");
    timeReading("HFile", rows, 0 );

    System.out.println("\n\n\n\nNotes: ");
    System.out.println(" * Timing includes open/closing of files.");
    System.out.println(" * Timing includes reading both Key and Value");
    System.out.println(" * Data is generated as random bytes. Other methods e.g. using " +
            "dictionary with care for distributation of words is under development.");
    System.out.println(" * Timing of write excludes random value/key generations. ");
    System.out.println(" * We need to mitigate cache effect on benchmark. We can apply several " +
            "ideas, for next step we do a large dummy read between benchmark read to dismantle " +
            "caching of data. Renaming of file may be helpful. We can have a loop that reads with" +
            " the same method several times and flood cache every time and average it to get a" +
            " better number.");
  }
}
