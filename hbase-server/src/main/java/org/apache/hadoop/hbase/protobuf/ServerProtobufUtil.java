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
package org.apache.hadoop.hbase.protobuf;

import java.util.List;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.fs.StorageIdentifier;
import org.apache.hadoop.hbase.fs.legacy.LegacyPathIdentifier;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.CompactionDescriptor;
import org.apache.hadoop.hbase.util.ByteStringer;

/**
 * Protobufs utility for server side only.
 */
@InterfaceAudience.Private
public final class ServerProtobufUtil {

  private ServerProtobufUtil() {
  }

  public static CompactionDescriptor toCompactionDescriptor(HRegionInfo info, byte[] family,
      List<Path> inputPaths, List<Path> outputPaths, StorageIdentifier storeDir) {
    return toCompactionDescriptor(info, null, family, inputPaths, outputPaths, storeDir);
  }

  public static CompactionDescriptor toCompactionDescriptor(HRegionInfo info, byte[] regionName,
      byte[] family, List<Path> inputPaths, List<Path> outputPaths, StorageIdentifier storeDir) {
    // compaction descriptor contains relative paths.
    // input / output paths are relative to the store dir
    // store dir is relative to region dir
    CompactionDescriptor.Builder builder = CompactionDescriptor.newBuilder()
        .setTableName(ByteStringer.wrap(info.getTable().toBytes()))
        .setEncodedRegionName(ByteStringer.wrap(
          regionName == null ? info.getEncodedNameAsBytes() : regionName))
        .setFamilyName(ByteStringer.wrap(family))
        // TODO need an equivalent to getName as unique name for StorageIdentifier
        .setStoreHomeDir(((LegacyPathIdentifier)storeDir).path.getName()); //make relative
    for (Path inputPath : inputPaths) {
      builder.addCompactionInput(inputPath.getName()); //relative path
    }
    for (Path outputPath : outputPaths) {
      builder.addCompactionOutput(outputPath.getName());
    }
    builder.setRegionName(ByteStringer.wrap(info.getRegionName()));
    return builder.build();
  }

}
