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
package org.apache.hadoop.hbase.io.hfile;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilder;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ChecksumType;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExternalResource;
import org.junit.rules.RuleChain;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This test provides coverage for HFileHeader block fields that are read and interpreted before
 * HBase checksum validation can be applied. As of now, this is just
 * {@code onDiskSizeWithoutHeader}.
 */
@Category({ IOTests.class, SmallTests.class })
public class TestHFileBlockHeaderCorruption {

  private static final Logger LOG = LoggerFactory.getLogger(TestHFileBlockHeaderCorruption.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestHFileBlockHeaderCorruption.class);

  private final HFileTestRule hFileTestRule;

  @Rule
  public final RuleChain ruleChain;

  public TestHFileBlockHeaderCorruption() throws IOException {
    TestName testName = new TestName();
    hFileTestRule = new HFileTestRule(new HBaseTestingUtility(), testName);
    ruleChain = RuleChain.outerRule(testName).around(hFileTestRule);
  }

  @Test
  public void testChecksumTypeCorruptionFirstBlock() throws Exception {
    HFileBlockChannelPosition firstBlock = null;
    try {
      try (HFileBlockChannelPositionIterator it =
        new HFileBlockChannelPositionIterator(hFileTestRule)) {
        assertTrue(it.hasNext());
        firstBlock = it.next();
      }

      Corrupter c = new Corrupter(firstBlock);

      logHeader(firstBlock);

      // test corrupted HFileBlock with unknown checksumType code -1
      c.write(HFileBlock.Header.CHECKSUM_TYPE_INDEX, ByteBuffer.wrap(new byte[] { -1 }));
      logHeader(firstBlock);
      try (HFileBlockChannelPositionIterator it =
        new HFileBlockChannelPositionIterator(hFileTestRule)) {
        CountingConsumer consumer = new CountingConsumer(it);
        try {
          consumer.readFully();
          fail();
        } catch (Exception e) {
          assertThat(e, new IsThrowableMatching().withInstanceOf(IOException.class)
            .withMessage(startsWith("Unknown checksum type code")));
        }
        assertEquals(0, consumer.getItemsRead());
      }

      // valid checksumType code test
      for (ChecksumType t : ChecksumType.values()) {
        testValidChecksumTypeReadBlock(t.getCode(), c, firstBlock);
      }

      c.restore();
      // test corrupted HFileBlock with unknown checksumType code 3
      c.write(HFileBlock.Header.CHECKSUM_TYPE_INDEX, ByteBuffer.wrap(new byte[] { 3 }));
      logHeader(firstBlock);
      try (HFileBlockChannelPositionIterator it =
        new HFileBlockChannelPositionIterator(hFileTestRule)) {
        CountingConsumer consumer = new CountingConsumer(it);
        try {
          consumer.readFully();
          fail();
        } catch (Exception e) {
          assertThat(e, new IsThrowableMatching().withInstanceOf(IOException.class)
            .withMessage(startsWith("Unknown checksum type code")));
        }
        assertEquals(0, consumer.getItemsRead());
      }
    } finally {
      if (firstBlock != null) {
        firstBlock.close();
      }
    }
  }

  @Test
  public void testChecksumTypeCorruptionSecondBlock() throws Exception {
    HFileBlockChannelPosition secondBlock = null;
    try {
      try (HFileBlockChannelPositionIterator it =
        new HFileBlockChannelPositionIterator(hFileTestRule)) {
        assertTrue(it.hasNext());
        it.next();
        assertTrue(it.hasNext());
        secondBlock = it.next();
      }

      Corrupter c = new Corrupter(secondBlock);

      logHeader(secondBlock);
      // test corrupted HFileBlock with unknown checksumType code -1
      c.write(HFileBlock.Header.CHECKSUM_TYPE_INDEX, ByteBuffer.wrap(new byte[] { -1 }));
      logHeader(secondBlock);
      try (HFileBlockChannelPositionIterator it =
        new HFileBlockChannelPositionIterator(hFileTestRule)) {
        CountingConsumer consumer = new CountingConsumer(it);
        try {
          consumer.readFully();
          fail();
        } catch (Exception e) {
          assertThat(e, new IsThrowableMatching().withInstanceOf(RuntimeException.class)
            .withMessage(startsWith("Unknown checksum type code")));
        }
        assertEquals(1, consumer.getItemsRead());
      }

      // valid checksumType code test
      for (ChecksumType t : ChecksumType.values()) {
        testValidChecksumTypeReadBlock(t.getCode(), c, secondBlock);
      }

      c.restore();
      // test corrupted HFileBlock with unknown checksumType code 3
      c.write(HFileBlock.Header.CHECKSUM_TYPE_INDEX, ByteBuffer.wrap(new byte[] { 3 }));
      logHeader(secondBlock);
      try (HFileBlockChannelPositionIterator it =
        new HFileBlockChannelPositionIterator(hFileTestRule)) {
        CountingConsumer consumer = new CountingConsumer(it);
        try {
          consumer.readFully();
          fail();
        } catch (Exception e) {
          assertThat(e, new IsThrowableMatching().withInstanceOf(RuntimeException.class)
            .withMessage(startsWith("Unknown checksum type code")));
        }
        assertEquals(1, consumer.getItemsRead());
      }
    } finally {
      if (secondBlock != null) {
        secondBlock.close();
      }
    }
  }

  public void testValidChecksumTypeReadBlock(byte checksumTypeCode, Corrupter c,
    HFileBlockChannelPosition testBlock) throws IOException {
    c.restore();
    c.write(HFileBlock.Header.CHECKSUM_TYPE_INDEX,
      ByteBuffer.wrap(new byte[] { checksumTypeCode }));
    logHeader(testBlock);
    try (
      HFileBlockChannelPositionIterator it = new HFileBlockChannelPositionIterator(hFileTestRule)) {
      CountingConsumer consumer = new CountingConsumer(it);
      try {
        consumer.readFully();
      } catch (Exception e) {
        fail("test fail: valid checksumType are not executing properly");
      }
      assertNotEquals(0, consumer.getItemsRead());
    }
  }

  @Test
  public void testOnDiskSizeWithoutHeaderCorruptionFirstBlock() throws Exception {
    HFileBlockChannelPosition firstBlock = null;
    try {
      try (HFileBlockChannelPositionIterator it =
        new HFileBlockChannelPositionIterator(hFileTestRule)) {
        assertTrue(it.hasNext());
        firstBlock = it.next();
      }

      Corrupter c = new Corrupter(firstBlock);

      logHeader(firstBlock);
      c.write(HFileBlock.Header.ON_DISK_SIZE_WITHOUT_HEADER_INDEX,
        ByteBuffer.wrap(Bytes.toBytes(Integer.MIN_VALUE)));
      logHeader(firstBlock);
      try (HFileBlockChannelPositionIterator it =
        new HFileBlockChannelPositionIterator(hFileTestRule)) {
        CountingConsumer consumer = new CountingConsumer(it);
        try {
          consumer.readFully();
          fail();
        } catch (Exception e) {
          assertThat(e, new IsThrowableMatching().withInstanceOf(IOException.class)
            .withMessage(startsWith("Invalid onDiskSizeWithHeader=")));
        }
        assertEquals(0, consumer.getItemsRead());
      }

      c.restore();
      c.write(HFileBlock.Header.ON_DISK_SIZE_WITHOUT_HEADER_INDEX,
        ByteBuffer.wrap(Bytes.toBytes(0)));
      logHeader(firstBlock);
      try (HFileBlockChannelPositionIterator it =
        new HFileBlockChannelPositionIterator(hFileTestRule)) {
        CountingConsumer consumer = new CountingConsumer(it);
        try {
          consumer.readFully();
          fail();
        } catch (Exception e) {
          assertThat(e, new IsThrowableMatching().withInstanceOf(IllegalArgumentException.class));
        }
        assertEquals(0, consumer.getItemsRead());
      }

      c.restore();
      c.write(HFileBlock.Header.ON_DISK_SIZE_WITHOUT_HEADER_INDEX,
        ByteBuffer.wrap(Bytes.toBytes(Integer.MAX_VALUE)));
      logHeader(firstBlock);
      try (HFileBlockChannelPositionIterator it =
        new HFileBlockChannelPositionIterator(hFileTestRule)) {
        CountingConsumer consumer = new CountingConsumer(it);
        try {
          consumer.readFully();
          fail();
        } catch (Exception e) {
          assertThat(e, new IsThrowableMatching().withInstanceOf(IOException.class)
            .withMessage(startsWith("Invalid onDiskSizeWithHeader=")));
        }
        assertEquals(0, consumer.getItemsRead());
      }
    } finally {
      if (firstBlock != null) {
        firstBlock.close();
      }
    }
  }

  @Test
  public void testOnDiskSizeWithoutHeaderCorruptionSecondBlock() throws Exception {
    HFileBlockChannelPosition secondBlock = null;
    try {
      try (HFileBlockChannelPositionIterator it =
        new HFileBlockChannelPositionIterator(hFileTestRule)) {
        assertTrue(it.hasNext());
        it.next();
        assertTrue(it.hasNext());
        secondBlock = it.next();
      }

      Corrupter c = new Corrupter(secondBlock);

      logHeader(secondBlock);
      c.write(HFileBlock.Header.ON_DISK_SIZE_WITHOUT_HEADER_INDEX,
        ByteBuffer.wrap(Bytes.toBytes(Integer.MIN_VALUE)));
      logHeader(secondBlock);
      try (HFileBlockChannelPositionIterator it =
        new HFileBlockChannelPositionIterator(hFileTestRule)) {
        CountingConsumer consumer = new CountingConsumer(it);
        try {
          consumer.readFully();
          fail();
        } catch (Exception e) {
          assertThat(e, new IsThrowableMatching().withInstanceOf(IOException.class)
            .withMessage(startsWith("Invalid onDiskSizeWithHeader=")));
        }
        assertEquals(1, consumer.getItemsRead());
      }

      c.restore();
      c.write(HFileBlock.Header.ON_DISK_SIZE_WITHOUT_HEADER_INDEX,
        ByteBuffer.wrap(Bytes.toBytes(0)));
      logHeader(secondBlock);
      try (HFileBlockChannelPositionIterator it =
        new HFileBlockChannelPositionIterator(hFileTestRule)) {
        CountingConsumer consumer = new CountingConsumer(it);
        try {
          consumer.readFully();
          fail();
        } catch (Exception e) {
          assertThat(e, new IsThrowableMatching().withInstanceOf(IllegalArgumentException.class));
        }
        assertEquals(1, consumer.getItemsRead());
      }

      c.restore();
      c.write(HFileBlock.Header.ON_DISK_SIZE_WITHOUT_HEADER_INDEX,
        ByteBuffer.wrap(Bytes.toBytes(Integer.MAX_VALUE)));
      logHeader(secondBlock);
      try (HFileBlockChannelPositionIterator it =
        new HFileBlockChannelPositionIterator(hFileTestRule)) {
        CountingConsumer consumer = new CountingConsumer(it);
        try {
          consumer.readFully();
          fail();
        } catch (Exception e) {
          assertThat(e, new IsThrowableMatching().withInstanceOf(IOException.class)
            .withMessage(startsWith("Invalid onDiskSizeWithHeader=")));
        }
        assertEquals(1, consumer.getItemsRead());
      }
    } finally {
      if (secondBlock != null) {
        secondBlock.close();
      }
    }
  }

  private static void logHeader(HFileBlockChannelPosition hbcp) throws IOException {
    ByteBuff buf = ByteBuff.wrap(ByteBuffer.allocate(HFileBlock.headerSize(true)));
    hbcp.rewind();
    assertEquals(buf.capacity(), buf.read(hbcp.getChannel()));
    buf.rewind();
    hbcp.rewind();
    logHeader(buf);
  }

  private static void logHeader(ByteBuff buf) {
    byte[] blockMagic = new byte[8];
    buf.get(blockMagic);
    int onDiskSizeWithoutHeader = buf.getInt();
    int uncompressedSizeWithoutHeader = buf.getInt();
    long prevBlockOffset = buf.getLong();
    byte checksumType = buf.get();
    int bytesPerChecksum = buf.getInt();
    int onDiskDataSizeWithHeader = buf.getInt();
    LOG.debug(
      "blockMagic={}, onDiskSizeWithoutHeader={}, uncompressedSizeWithoutHeader={}, "
        + "prevBlockOffset={}, checksumType={}, bytesPerChecksum={}, onDiskDataSizeWithHeader={}",
      Bytes.toStringBinary(blockMagic), onDiskSizeWithoutHeader, uncompressedSizeWithoutHeader,
      prevBlockOffset, checksumType, bytesPerChecksum, onDiskDataSizeWithHeader);
  }

  /**
   * Data class to enabled messing with the bytes behind an {@link HFileBlock}.
   */
  public static class HFileBlockChannelPosition implements Closeable {
    private final SeekableByteChannel channel;
    private final long position;

    public HFileBlockChannelPosition(SeekableByteChannel channel, long position) {
      this.channel = channel;
      this.position = position;
    }

    public SeekableByteChannel getChannel() {
      return channel;
    }

    public long getPosition() {
      return position;
    }

    public void rewind() throws IOException {
      channel.position(position);
    }

    @Override
    public void close() throws IOException {
      channel.close();
    }
  }

  /**
   * Reads blocks off of an {@link HFileBlockChannelPositionIterator}, counting them as it does.
   */
  public static class CountingConsumer {
    private final HFileBlockChannelPositionIterator iterator;
    private int itemsRead = 0;

    public CountingConsumer(HFileBlockChannelPositionIterator iterator) {
      this.iterator = iterator;
    }

    public int getItemsRead() {
      return itemsRead;
    }

    public Object readFully() throws IOException {
      Object val = null;
      for (itemsRead = 0; iterator.hasNext(); itemsRead++) {
        val = iterator.next();
      }
      return val;
    }
  }

  /**
   * A simplified wrapper over an {@link HFileBlock.BlockIterator} that looks a lot like an
   * {@link java.util.Iterator}.
   */
  public static class HFileBlockChannelPositionIterator implements Closeable {

    private final HFileTestRule hFileTestRule;
    private final HFile.Reader reader;
    private final HFileBlock.BlockIterator iter;
    private HFileBlockChannelPosition current = null;

    public HFileBlockChannelPositionIterator(HFileTestRule hFileTestRule) throws IOException {
      Configuration conf = hFileTestRule.getConfiguration();
      HFileSystem hfs = hFileTestRule.getHFileSystem();
      Path hfsPath = hFileTestRule.getPath();

      HFile.Reader reader = null;
      HFileBlock.BlockIterator iter = null;
      try {
        reader = HFile.createReader(hfs, hfsPath, CacheConfig.DISABLED, true, conf);
        HFileBlock.FSReader fsreader = reader.getUncachedBlockReader();
        // The read block offset cannot out of the range:0,loadOnOpenDataOffset
        iter = fsreader.blockRange(0, reader.getTrailer().getLoadOnOpenDataOffset());
      } catch (IOException e) {
        if (reader != null) {
          closeQuietly(reader::close);
        }
        throw e;
      }

      this.hFileTestRule = hFileTestRule;
      this.reader = reader;
      this.iter = iter;
    }

    public boolean hasNext() throws IOException {
      HFileBlock next = iter.nextBlock();
      SeekableByteChannel channel = hFileTestRule.getRWChannel();
      if (next != null) {
        current = new HFileBlockChannelPosition(channel, next.getOffset());
      }
      return next != null;
    }

    public HFileBlockChannelPosition next() {
      if (current == null) {
        throw new NoSuchElementException();
      }
      HFileBlockChannelPosition ret = current;
      current = null;
      return ret;
    }

    @Override
    public void close() throws IOException {
      if (current != null) {
        closeQuietly(current::close);
      }
      closeQuietly(reader::close);
    }

    @FunctionalInterface
    private interface CloseMethod {
      void run() throws IOException;
    }

    private static void closeQuietly(CloseMethod closeMethod) {
      try {
        closeMethod.run();
      } catch (Throwable e) {
        LOG.debug("Ignoring thrown exception.", e);
      }
    }
  }

  /**
   * Enables writing and rewriting portions of the file backing an {@link HFileBlock}.
   */
  public static class Corrupter {

    private final HFileBlockChannelPosition channelAndPosition;
    private final ByteBuffer originalHeader;

    public Corrupter(HFileBlockChannelPosition channelAndPosition) throws IOException {
      this.channelAndPosition = channelAndPosition;
      this.originalHeader = readHeaderData(channelAndPosition);
    }

    private static ByteBuffer readHeaderData(HFileBlockChannelPosition channelAndPosition)
      throws IOException {
      SeekableByteChannel channel = channelAndPosition.getChannel();
      ByteBuffer originalHeader = ByteBuffer.allocate(HFileBlock.headerSize(true));
      channelAndPosition.rewind();
      channel.read(originalHeader);
      return originalHeader;
    }

    public void write(int offset, ByteBuffer src) throws IOException {
      SeekableByteChannel channel = channelAndPosition.getChannel();
      long position = channelAndPosition.getPosition();
      channel.position(position + offset);
      channel.write(src);
    }

    public void restore() throws IOException {
      SeekableByteChannel channel = channelAndPosition.getChannel();
      originalHeader.rewind();
      channelAndPosition.rewind();
      assertEquals(originalHeader.capacity(), channel.write(originalHeader));
    }
  }

  public static class HFileTestRule extends ExternalResource {

    private final HBaseTestingUtility testingUtility;
    private final HFileSystem hfs;
    private final HFileContext context;
    private final TestName testName;
    private Path path;

    public HFileTestRule(HBaseTestingUtility testingUtility, TestName testName) throws IOException {
      this.testingUtility = testingUtility;
      this.testName = testName;
      this.hfs = (HFileSystem) HFileSystem.get(testingUtility.getConfiguration());
      this.context =
        new HFileContextBuilder().withBlockSize(4 * 1024).withHBaseCheckSum(true).build();
    }

    public Configuration getConfiguration() {
      return testingUtility.getConfiguration();
    }

    public HFileSystem getHFileSystem() {
      return hfs;
    }

    public HFileContext getHFileContext() {
      return context;
    }

    public Path getPath() {
      return path;
    }

    public SeekableByteChannel getRWChannel() throws IOException {
      java.nio.file.Path p = FileSystems.getDefault().getPath(path.toString());
      return Files.newByteChannel(p, StandardOpenOption.READ, StandardOpenOption.WRITE,
        StandardOpenOption.DSYNC);
    }

    @Override
    protected void before() throws Throwable {
      this.path = new Path(testingUtility.getDataTestDirOnTestFS(), testName.getMethodName());
      HFile.WriterFactory factory =
        HFile.getWriterFactory(testingUtility.getConfiguration(), CacheConfig.DISABLED)
          .withPath(hfs, path).withFileContext(context);

      CellBuilder cellBuilder = CellBuilderFactory.create(CellBuilderType.DEEP_COPY);
      Random rand = new Random(Instant.now().toEpochMilli());
      byte[] family = Bytes.toBytes("f");
      try (HFile.Writer writer = factory.create()) {
        for (int i = 0; i < 40; i++) {
          byte[] row = RandomKeyValueUtil.randomOrderedFixedLengthKey(rand, i, 100);
          byte[] qualifier = RandomKeyValueUtil.randomRowOrQualifier(rand);
          byte[] value = RandomKeyValueUtil.randomValue(rand);
          Cell cell = cellBuilder.setType(Cell.Type.Put).setRow(row).setFamily(family)
            .setQualifier(qualifier).setValue(value).build();
          writer.append(cell);
          cellBuilder.clear();
        }
      }
    }
  }

  /**
   * A Matcher implementation that can make basic assertions over a provided {@link Throwable}.
   * Assertion failures include the full stacktrace in their description.
   */
  private static final class IsThrowableMatching extends TypeSafeMatcher<Throwable> {

    private final List<Matcher<? super Throwable>> requirements = new LinkedList<>();

    public IsThrowableMatching withInstanceOf(Class<?> type) {
      requirements.add(instanceOf(type));
      return this;
    }

    public IsThrowableMatching withMessage(Matcher<String> matcher) {
      requirements.add(hasProperty("message", matcher));
      return this;
    }

    @Override
    protected boolean matchesSafely(Throwable throwable) {
      return allOf(requirements).matches(throwable);
    }

    @Override
    protected void describeMismatchSafely(Throwable item, Description mismatchDescription) {
      allOf(requirements).describeMismatch(item, mismatchDescription);
      // would be nice if `item` could be provided as the cause of the AssertionError instead.
      mismatchDescription.appendText(String.format("%nProvided: "));
      try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
        try (PrintStream ps = new PrintStream(baos, false, StandardCharsets.UTF_8.name())) {
          item.printStackTrace(ps);
          ps.flush();
        }
        mismatchDescription.appendText(baos.toString(StandardCharsets.UTF_8.name()));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void describeTo(Description description) {
      description.appendDescriptionOf(allOf(requirements));
    }
  }
}
