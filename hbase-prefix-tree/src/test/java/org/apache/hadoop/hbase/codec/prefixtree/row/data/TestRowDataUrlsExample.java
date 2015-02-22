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

package org.apache.hadoop.hbase.codec.prefixtree.row.data;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueTestUtil;
import org.apache.hadoop.hbase.codec.prefixtree.encode.PrefixTreeEncoder;
import org.apache.hadoop.hbase.codec.prefixtree.encode.column.ColumnNodeWriter;
import org.apache.hadoop.hbase.codec.prefixtree.encode.row.RowNodeWriter;
import org.apache.hadoop.hbase.codec.prefixtree.encode.tokenize.TokenizerNode;
import org.apache.hadoop.hbase.codec.prefixtree.row.BaseTestRowData;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Lists;

/*
 * test different timestamps
 * 
 * http://pastebin.com/7ks8kzJ2
 * http://pastebin.com/MPn03nsK
 */
public class TestRowDataUrlsExample extends BaseTestRowData{

  static String TENANT_ID = Integer.toString(95322);
  static String APP_ID = Integer.toString(12);
  static List<String> URLS = Lists.newArrayList(
      "com.dablog/2011/10/04/boating", 
      "com.dablog/2011/10/09/lasers", 
      "com.jamiesrecipes", //this nub helped find a bug
      "com.jamiesrecipes/eggs");
  static String FAMILY = "hits";
  static List<String> BROWSERS = Lists.newArrayList(
      "Chrome", "IE8", "IE9beta");//, "Opera", "Safari");
  static long TIMESTAMP = 1234567890;

  static int MAX_VALUE = 50;

  static List<KeyValue> kvs = Lists.newArrayList();
  static{
    for(String rowKey : URLS){
      for(String qualifier : BROWSERS){
        KeyValue kv = new KeyValue(
            Bytes.toBytes(rowKey), 
            Bytes.toBytes(FAMILY), 
            Bytes.toBytes(qualifier), 
            TIMESTAMP, 
            KeyValue.Type.Put, 
            Bytes.toBytes("VvvV"));
        kvs.add(kv);
      }
    }
  }

  /**
   * Used for generating docs.
   */
  public static void main(String... args) throws IOException{
    System.out.println("-- inputs --");
    System.out.println(KeyValueTestUtil.toStringWithPadding(kvs, true));
    ByteArrayOutputStream os = new ByteArrayOutputStream(1<<20);
    PrefixTreeEncoder encoder = new PrefixTreeEncoder(os, false);

    for(KeyValue kv : kvs){
      encoder.write(kv);
    }
    encoder.flush();

    System.out.println("-- qualifier SortedPtBuilderNodes --");
    for(TokenizerNode tokenizer : encoder.getQualifierWriter().getNonLeaves()){
      System.out.println(tokenizer);
    }
    for(TokenizerNode tokenizerNode : encoder.getQualifierWriter().getLeaves()){
      System.out.println(tokenizerNode);
    }

    System.out.println("-- qualifier PtColumnNodeWriters --");
    for(ColumnNodeWriter writer : encoder.getQualifierWriter().getColumnNodeWriters()){
      System.out.println(writer);
    }

    System.out.println("-- rowKey SortedPtBuilderNodes --");
    for(TokenizerNode tokenizerNode : encoder.getRowWriter().getNonLeaves()){
      System.out.println(tokenizerNode);
    }
    for(TokenizerNode tokenizerNode : encoder.getRowWriter().getLeaves()){
      System.out.println(tokenizerNode);
    }

    System.out.println("-- row PtRowNodeWriters --");
    for(RowNodeWriter writer : encoder.getRowWriter().getNonLeafWriters()){
      System.out.println(writer);
    }
    for(RowNodeWriter writer : encoder.getRowWriter().getLeafWriters()){
      System.out.println(writer);
    }

    System.out.println("-- concatenated values --");
    System.out.println(Bytes.toStringBinary(encoder.getValueByteRange().deepCopyToNewArray()));
  }

  @Override
  public List<KeyValue> getInputs() {
    return kvs;
  }

}
