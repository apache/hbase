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
package org.apache.hadoop.hbase.mapreduce;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.mapreduce.ImportTsv.TsvParser;
import org.apache.hadoop.hbase.mapreduce.ImportTsv.TsvParser.BadTsvLineException;
import org.apache.hadoop.hbase.mapreduce.ImportTsv.TsvParser.ParsedLine;
import org.apache.hadoop.hbase.testclassification.MapReduceTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.hbase.thirdparty.com.google.common.base.Joiner;
import org.apache.hbase.thirdparty.com.google.common.base.Splitter;
import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;

/**
 * Tests for {@link TsvParser}.
 */
@Tag(MapReduceTests.TAG)
@Tag(SmallTests.TAG)
public class TestImportTsvParser {

  private void assertBytesEquals(byte[] a, byte[] b) {
    assertEquals(Bytes.toStringBinary(a), Bytes.toStringBinary(b));
  }

  private void checkParsing(ParsedLine parsed, Iterable<String> expected) {
    ArrayList<String> parsedCols = new ArrayList<>();
    for (int i = 0; i < parsed.getColumnCount(); i++) {
      parsedCols.add(Bytes.toString(parsed.getLineBytes(), parsed.getColumnOffset(i),
        parsed.getColumnLength(i)));
    }
    if (!Iterables.elementsEqual(parsedCols, expected)) {
      fail("Expected: " + Joiner.on(",").join(expected) + "\n" + "Got:"
        + Joiner.on(",").join(parsedCols));
    }
  }

  @Test
  public void testTsvParserSpecParsing() {
    TsvParser parser;

    parser = new TsvParser("HBASE_ROW_KEY", "\t");
    assertNull(parser.getFamily(0));
    assertNull(parser.getQualifier(0));
    assertEquals(0, parser.getRowKeyColumnIndex());
    assertFalse(parser.hasTimestamp());

    parser = new TsvParser("HBASE_ROW_KEY,col1:scol1", "\t");
    assertNull(parser.getFamily(0));
    assertNull(parser.getQualifier(0));
    assertBytesEquals(Bytes.toBytes("col1"), parser.getFamily(1));
    assertBytesEquals(Bytes.toBytes("scol1"), parser.getQualifier(1));
    assertEquals(0, parser.getRowKeyColumnIndex());
    assertFalse(parser.hasTimestamp());

    parser = new TsvParser("HBASE_ROW_KEY,col1:scol1,col1:scol2", "\t");
    assertNull(parser.getFamily(0));
    assertNull(parser.getQualifier(0));
    assertBytesEquals(Bytes.toBytes("col1"), parser.getFamily(1));
    assertBytesEquals(Bytes.toBytes("scol1"), parser.getQualifier(1));
    assertBytesEquals(Bytes.toBytes("col1"), parser.getFamily(2));
    assertBytesEquals(Bytes.toBytes("scol2"), parser.getQualifier(2));
    assertEquals(0, parser.getRowKeyColumnIndex());
    assertFalse(parser.hasTimestamp());

    parser = new TsvParser("HBASE_ROW_KEY,col1:scol1,HBASE_TS_KEY,col1:scol2", "\t");
    assertNull(parser.getFamily(0));
    assertNull(parser.getQualifier(0));
    assertBytesEquals(Bytes.toBytes("col1"), parser.getFamily(1));
    assertBytesEquals(Bytes.toBytes("scol1"), parser.getQualifier(1));
    assertBytesEquals(Bytes.toBytes("col1"), parser.getFamily(3));
    assertBytesEquals(Bytes.toBytes("scol2"), parser.getQualifier(3));
    assertEquals(0, parser.getRowKeyColumnIndex());
    assertTrue(parser.hasTimestamp());
    assertEquals(2, parser.getTimestampKeyColumnIndex());

    parser =
      new TsvParser("HBASE_ROW_KEY,col1:scol1,HBASE_TS_KEY,col1:scol2,HBASE_ATTRIBUTES_KEY", "\t");
    assertNull(parser.getFamily(0));
    assertNull(parser.getQualifier(0));
    assertBytesEquals(Bytes.toBytes("col1"), parser.getFamily(1));
    assertBytesEquals(Bytes.toBytes("scol1"), parser.getQualifier(1));
    assertBytesEquals(Bytes.toBytes("col1"), parser.getFamily(3));
    assertBytesEquals(Bytes.toBytes("scol2"), parser.getQualifier(3));
    assertEquals(0, parser.getRowKeyColumnIndex());
    assertTrue(parser.hasTimestamp());
    assertEquals(2, parser.getTimestampKeyColumnIndex());
    assertEquals(4, parser.getAttributesKeyColumnIndex());

    parser =
      new TsvParser("HBASE_ATTRIBUTES_KEY,col1:scol1,HBASE_TS_KEY,col1:scol2,HBASE_ROW_KEY", "\t");
    assertNull(parser.getFamily(0));
    assertNull(parser.getQualifier(0));
    assertBytesEquals(Bytes.toBytes("col1"), parser.getFamily(1));
    assertBytesEquals(Bytes.toBytes("scol1"), parser.getQualifier(1));
    assertBytesEquals(Bytes.toBytes("col1"), parser.getFamily(3));
    assertBytesEquals(Bytes.toBytes("scol2"), parser.getQualifier(3));
    assertEquals(4, parser.getRowKeyColumnIndex());
    assertTrue(parser.hasTimestamp());
    assertEquals(2, parser.getTimestampKeyColumnIndex());
    assertEquals(0, parser.getAttributesKeyColumnIndex());
  }

  @Test
  public void testTsvParser() throws BadTsvLineException {
    TsvParser parser = new TsvParser("col_a,col_b:qual,HBASE_ROW_KEY,col_d", "\t");
    assertBytesEquals(Bytes.toBytes("col_a"), parser.getFamily(0));
    assertBytesEquals(HConstants.EMPTY_BYTE_ARRAY, parser.getQualifier(0));
    assertBytesEquals(Bytes.toBytes("col_b"), parser.getFamily(1));
    assertBytesEquals(Bytes.toBytes("qual"), parser.getQualifier(1));
    assertNull(parser.getFamily(2));
    assertNull(parser.getQualifier(2));
    assertEquals(2, parser.getRowKeyColumnIndex());

    assertEquals(TsvParser.DEFAULT_TIMESTAMP_COLUMN_INDEX, parser.getTimestampKeyColumnIndex());

    byte[] line = Bytes.toBytes("val_a\tval_b\tval_c\tval_d");
    ParsedLine parsed = parser.parse(line, line.length);
    checkParsing(parsed, Splitter.on("\t").split(Bytes.toString(line)));
  }

  @Test
  public void testTsvParserWithTimestamp() throws BadTsvLineException {
    TsvParser parser = new TsvParser("HBASE_ROW_KEY,HBASE_TS_KEY,col_a,", "\t");
    assertNull(parser.getFamily(0));
    assertNull(parser.getQualifier(0));
    assertNull(parser.getFamily(1));
    assertNull(parser.getQualifier(1));
    assertBytesEquals(Bytes.toBytes("col_a"), parser.getFamily(2));
    assertBytesEquals(HConstants.EMPTY_BYTE_ARRAY, parser.getQualifier(2));
    assertEquals(0, parser.getRowKeyColumnIndex());
    assertEquals(1, parser.getTimestampKeyColumnIndex());

    byte[] line = Bytes.toBytes("rowkey\t1234\tval_a");
    ParsedLine parsed = parser.parse(line, line.length);
    assertEquals(1234L, parsed.getTimestamp(-1));
    checkParsing(parsed, Splitter.on("\t").split(Bytes.toString(line)));
  }

  /**
   * Test cases that throw BadTsvLineException
   */
  @Test
  public void testTsvParserBadTsvLineExcessiveColumns() throws BadTsvLineException {
    TsvParser parser = new TsvParser("HBASE_ROW_KEY,col_a", "\t");
    byte[] line = Bytes.toBytes("val_a\tval_b\tval_c");
    assertThrows(BadTsvLineException.class, () -> parser.parse(line, line.length));
  }

  @Test
  public void testTsvParserBadTsvLineZeroColumn() throws BadTsvLineException {
    TsvParser parser = new TsvParser("HBASE_ROW_KEY,col_a", "\t");
    byte[] line = Bytes.toBytes("");
    assertThrows(BadTsvLineException.class, () -> parser.parse(line, line.length));
  }

  @Test
  public void testTsvParserBadTsvLineOnlyKey() throws BadTsvLineException {
    TsvParser parser = new TsvParser("HBASE_ROW_KEY,col_a", "\t");
    byte[] line = Bytes.toBytes("key_only");
    assertThrows(BadTsvLineException.class, () -> parser.parse(line, line.length));
  }

  @Test
  public void testTsvParserBadTsvLineNoRowKey() throws BadTsvLineException {
    TsvParser parser = new TsvParser("col_a,HBASE_ROW_KEY", "\t");
    byte[] line = Bytes.toBytes("only_cola_data_and_no_row_key");
    assertThrows(BadTsvLineException.class, () -> parser.parse(line, line.length));
  }

  @Test
  public void testTsvParserInvalidTimestamp() throws BadTsvLineException {
    TsvParser parser = new TsvParser("HBASE_ROW_KEY,HBASE_TS_KEY,col_a,", "\t");
    assertEquals(1, parser.getTimestampKeyColumnIndex());
    byte[] line = Bytes.toBytes("rowkey\ttimestamp\tval_a");
    ParsedLine parsed = parser.parse(line, line.length);
    assertThrows(BadTsvLineException.class, () -> parsed.getTimestamp(-1));
    checkParsing(parsed, Splitter.on("\t").split(Bytes.toString(line)));
  }

  @Test
  public void testTsvParserNoTimestampValue() throws BadTsvLineException {
    TsvParser parser = new TsvParser("HBASE_ROW_KEY,col_a,HBASE_TS_KEY", "\t");
    assertEquals(2, parser.getTimestampKeyColumnIndex());
    byte[] line = Bytes.toBytes("rowkey\tval_a");
    assertThrows(BadTsvLineException.class, () -> parser.parse(line, line.length));
  }

  @Test
  public void testTsvParserParseRowKey() throws BadTsvLineException {
    TsvParser parser = new TsvParser("HBASE_ROW_KEY,col_a,HBASE_TS_KEY", "\t");
    assertEquals(0, parser.getRowKeyColumnIndex());
    byte[] line = Bytes.toBytes("rowkey\tval_a\t1234");
    Pair<Integer, Integer> rowKeyOffsets = parser.parseRowKey(line, line.length);
    assertEquals(0, rowKeyOffsets.getFirst().intValue());
    assertEquals(6, rowKeyOffsets.getSecond().intValue());

    byte[] line2 = Bytes.toBytes("\t\tval_a\t1234");
    assertThrows(BadTsvLineException.class, () -> parser.parseRowKey(line2, line2.length));

    TsvParser parser2 = new TsvParser("col_a,HBASE_ROW_KEY,HBASE_TS_KEY", "\t");
    assertEquals(1, parser2.getRowKeyColumnIndex());
    line = Bytes.toBytes("val_a\trowkey\t1234");
    rowKeyOffsets = parser2.parseRowKey(line, line.length);
    assertEquals(6, rowKeyOffsets.getFirst().intValue());
    assertEquals(6, rowKeyOffsets.getSecond().intValue());

    byte[] line3 = Bytes.toBytes("val_a");
    assertThrows(BadTsvLineException.class, () -> parser2.parseRowKey(line3, line3.length));

    TsvParser parser3 = new TsvParser("col_a,HBASE_TS_KEY,HBASE_ROW_KEY", "\t");
    assertEquals(2, parser3.getRowKeyColumnIndex());
    line = Bytes.toBytes("val_a\t1234\trowkey");
    rowKeyOffsets = parser3.parseRowKey(line, line.length);
    assertEquals(11, rowKeyOffsets.getFirst().intValue());
    assertEquals(6, rowKeyOffsets.getSecond().intValue());
  }

  @Test
  public void testTsvParseAttributesKey() throws BadTsvLineException {
    TsvParser parser = new TsvParser("HBASE_ROW_KEY,col_a,HBASE_TS_KEY,HBASE_ATTRIBUTES_KEY", "\t");
    assertEquals(0, parser.getRowKeyColumnIndex());
    byte[] line = Bytes.toBytes("rowkey\tval_a\t1234\tkey=>value");
    ParsedLine parse = parser.parse(line, line.length);
    assertEquals(18, parse.getAttributeKeyOffset());
    assertEquals(3, parser.getAttributesKeyColumnIndex());
    String[] attributes = parse.getIndividualAttributes();
    assertEquals("key=>value", attributes[0]);
    byte[] line2 = Bytes.toBytes("rowkey\tval_a\t1234");
    TsvParser finalParser = parser;
    assertThrows(BadTsvLineException.class, () -> finalParser.parse(line2, line2.length));

    TsvParser parser2 =
      new TsvParser("HBASE_ATTRIBUTES_KEY,col_a,HBASE_ROW_KEY,HBASE_TS_KEY", "\t");
    assertEquals(2, parser2.getRowKeyColumnIndex());
    line = Bytes.toBytes("key=>value\tval_a\trowkey\t1234");
    parse = parser2.parse(line, line.length);
    assertEquals(0, parse.getAttributeKeyOffset());
    assertEquals(0, parser2.getAttributesKeyColumnIndex());
    attributes = parse.getIndividualAttributes();
    assertEquals("key=>value", attributes[0]);
    byte[] line3 = Bytes.toBytes("val_a");
    assertThrows(BadTsvLineException.class, () -> parser2.parse(line3, line3.length));

    parser = new TsvParser("col_a,HBASE_ATTRIBUTES_KEY,HBASE_TS_KEY,HBASE_ROW_KEY", "\t");
    assertEquals(3, parser.getRowKeyColumnIndex());
    line = Bytes.toBytes("val_a\tkey0=>value0,key1=>value1,key2=>value2\t1234\trowkey");
    parse = parser.parse(line, line.length);
    assertEquals(1, parser.getAttributesKeyColumnIndex());
    assertEquals(6, parse.getAttributeKeyOffset());
    String[] attr = parse.getIndividualAttributes();
    int i = 0;
    for (String str : attr) {
      assertEquals(("key" + i + "=>" + "value" + i), str);
      i++;
    }
  }

  @Test
  public void testTsvParserWithCellVisibilityCol() throws BadTsvLineException {
    TsvParser parser = new TsvParser(
      "HBASE_ROW_KEY,col_a,HBASE_TS_KEY,HBASE_ATTRIBUTES_KEY,HBASE_CELL_VISIBILITY", "\t");
    assertEquals(0, parser.getRowKeyColumnIndex());
    assertEquals(4, parser.getCellVisibilityColumnIndex());
    byte[] line = Bytes.toBytes("rowkey\tval_a\t1234\tkey=>value\tPRIVATE&SECRET");
    ParsedLine parse = parser.parse(line, line.length);
    assertEquals(18, parse.getAttributeKeyOffset());
    assertEquals(3, parser.getAttributesKeyColumnIndex());
    String[] attributes = parse.getIndividualAttributes();
    assertEquals("key=>value", attributes[0]);
    assertEquals(29, parse.getCellVisibilityColumnOffset());
  }
}
