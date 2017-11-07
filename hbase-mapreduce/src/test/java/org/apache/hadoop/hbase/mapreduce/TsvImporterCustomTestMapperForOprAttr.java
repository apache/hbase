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
package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.ImportTsv.TsvParser.BadTsvLineException;
import org.apache.hadoop.hbase.mapreduce.ImportTsv.TsvParser.ParsedLine;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Just shows a simple example of how the attributes can be extracted and added
 * to the puts
 */
public class TsvImporterCustomTestMapperForOprAttr extends TsvImporterMapper {
  @Override
  protected void populatePut(byte[] lineBytes, ParsedLine parsed, Put put, int i)
      throws BadTsvLineException, IOException {
    KeyValue kv;
    kv = new KeyValue(lineBytes, parsed.getRowKeyOffset(), parsed.getRowKeyLength(),
        parser.getFamily(i), 0, parser.getFamily(i).length, parser.getQualifier(i), 0,
        parser.getQualifier(i).length, ts, KeyValue.Type.Put, lineBytes, parsed.getColumnOffset(i),
        parsed.getColumnLength(i));
    if (parsed.getIndividualAttributes() != null) {
      String[] attributes = parsed.getIndividualAttributes();
      for (String attr : attributes) {
        String[] split = attr.split(ImportTsv.DEFAULT_ATTRIBUTES_SEPERATOR);
        if (split == null || split.length <= 1) {
          throw new BadTsvLineException(msg(attributes));
        } else {
          if (split[0].length() <= 0 || split[1].length() <= 0) {
            throw new BadTsvLineException(msg(attributes));
          }
          put.setAttribute(split[0], Bytes.toBytes(split[1]));
        }
      }
    }
    put.add(kv);
  }

  private String msg(Object[] attributes) {
    return "Invalid attributes separator specified: " + Arrays.toString(attributes);
  }
}
