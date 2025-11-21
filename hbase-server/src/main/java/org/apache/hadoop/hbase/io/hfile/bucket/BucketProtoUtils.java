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
package org.apache.hadoop.hbase.io.hfile.bucket;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Function;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.io.ByteBuffAllocator.Recycler;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.BlockPriority;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.CacheableDeserializerIdManager;
import org.apache.hadoop.hbase.io.hfile.HFileBlock;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.shaded.protobuf.generated.BucketCacheProtos;

@InterfaceAudience.Private
final class BucketProtoUtils {

  final static byte[] PB_MAGIC_V2 = new byte[] { 'V', '2', 'U', 'F' };

  private BucketProtoUtils() {

  }

  static BucketCacheProtos.BucketCacheEntry toPB(BucketCache cache,
    BucketCacheProtos.BackingMap.Builder backingMapBuilder) {
    return BucketCacheProtos.BucketCacheEntry.newBuilder().setCacheCapacity(cache.getMaxSize())
      .setIoClass(cache.ioEngine.getClass().getName())
      .setMapClass(cache.backingMap.getClass().getName())
      .putAllDeserializers(CacheableDeserializerIdManager.save())
      .putAllCachedFiles(toCachedPB(cache.fullyCachedFiles))
      .setBackingMap(backingMapBuilder.build())
      .setChecksum(ByteString
        .copyFrom(((PersistentIOEngine) cache.ioEngine).calculateChecksum(cache.getAlgorithm())))
      .build();
  }

  public static void serializeAsPB(BucketCache cache, FileOutputStream fos, long chunkSize)
    throws IOException {
    // Write the new version of magic number.
    fos.write(PB_MAGIC_V2);

    BucketCacheProtos.BackingMap.Builder builder = BucketCacheProtos.BackingMap.newBuilder();
    BucketCacheProtos.BackingMapEntry.Builder entryBuilder =
      BucketCacheProtos.BackingMapEntry.newBuilder();

    // Persist the metadata first.
    toPB(cache, builder).writeDelimitedTo(fos);

    int blockCount = 0;
    // Persist backing map entries in chunks of size 'chunkSize'.
    for (Map.Entry<BlockCacheKey, BucketEntry> entry : cache.backingMap.entrySet()) {
      blockCount++;
      addEntryToBuilder(entry, entryBuilder, builder);
      if (blockCount % chunkSize == 0) {
        builder.build().writeDelimitedTo(fos);
        builder.clear();
      }
    }
    // Persist the last chunk.
    if (builder.getEntryList().size() > 0) {
      builder.build().writeDelimitedTo(fos);
    }
  }

  private static void addEntryToBuilder(Map.Entry<BlockCacheKey, BucketEntry> entry,
    BucketCacheProtos.BackingMapEntry.Builder entryBuilder,
    BucketCacheProtos.BackingMap.Builder builder) {
    entryBuilder.clear();
    entryBuilder.setKey(BucketProtoUtils.toPB(entry.getKey()));
    entryBuilder.setValue(BucketProtoUtils.toPB(entry.getValue()));
    builder.addEntry(entryBuilder.build());
  }

  private static BucketCacheProtos.BlockCacheKey toPB(BlockCacheKey key) {
    return BucketCacheProtos.BlockCacheKey.newBuilder().setHfilename(key.getHfileName())
      .setOffset(key.getOffset()).setPrimaryReplicaBlock(key.isPrimary())
      .setBlockType(toPB(key.getBlockType())).build();
  }

  private static BucketCacheProtos.BlockType toPB(BlockType blockType) {
    switch (blockType) {
      case DATA:
        return BucketCacheProtos.BlockType.data;
      case META:
        return BucketCacheProtos.BlockType.meta;
      case TRAILER:
        return BucketCacheProtos.BlockType.trailer;
      case INDEX_V1:
        return BucketCacheProtos.BlockType.index_v1;
      case FILE_INFO:
        return BucketCacheProtos.BlockType.file_info;
      case LEAF_INDEX:
        return BucketCacheProtos.BlockType.leaf_index;
      case ROOT_INDEX:
        return BucketCacheProtos.BlockType.root_index;
      case BLOOM_CHUNK:
        return BucketCacheProtos.BlockType.bloom_chunk;
      case ENCODED_DATA:
        return BucketCacheProtos.BlockType.encoded_data;
      case GENERAL_BLOOM_META:
        return BucketCacheProtos.BlockType.general_bloom_meta;
      case INTERMEDIATE_INDEX:
        return BucketCacheProtos.BlockType.intermediate_index;
      case DELETE_FAMILY_BLOOM_META:
        return BucketCacheProtos.BlockType.delete_family_bloom_meta;
      default:
        throw new Error("Unrecognized BlockType.");
    }
  }

  private static BucketCacheProtos.BucketEntry toPB(BucketEntry entry) {
    return BucketCacheProtos.BucketEntry.newBuilder().setOffset(entry.offset())
      .setCachedTime(entry.getCachedTime()).setLength(entry.getLength())
      .setDiskSizeWithHeader(entry.getOnDiskSizeWithHeader())
      .setDeserialiserIndex(entry.deserializerIndex).setAccessCounter(entry.getAccessCounter())
      .setPriority(toPB(entry.getPriority())).build();
  }

  private static BucketCacheProtos.BlockPriority toPB(BlockPriority p) {
    switch (p) {
      case MULTI:
        return BucketCacheProtos.BlockPriority.multi;
      case MEMORY:
        return BucketCacheProtos.BlockPriority.memory;
      case SINGLE:
        return BucketCacheProtos.BlockPriority.single;
      default:
        throw new Error("Unrecognized BlockPriority.");
    }
  }

  static Pair<ConcurrentHashMap<BlockCacheKey, BucketEntry>, NavigableSet<BlockCacheKey>> fromPB(
    Map<Integer, String> deserializers, BucketCacheProtos.BackingMap backingMap,
    Function<BucketEntry, Recycler> createRecycler) throws IOException {
    ConcurrentHashMap<BlockCacheKey, BucketEntry> result = new ConcurrentHashMap<>();
    NavigableSet<BlockCacheKey> resultSet = new ConcurrentSkipListSet<>(Comparator
      .comparing(BlockCacheKey::getHfileName).thenComparingLong(BlockCacheKey::getOffset));
    for (BucketCacheProtos.BackingMapEntry entry : backingMap.getEntryList()) {
      BucketCacheProtos.BlockCacheKey protoKey = entry.getKey();
      BlockCacheKey key = new BlockCacheKey(protoKey.getHfilename(), protoKey.getFamilyName(),
        protoKey.getRegionName(), protoKey.getOffset(), protoKey.getPrimaryReplicaBlock(),
        fromPb(protoKey.getBlockType()), protoKey.getArchived());
      BucketCacheProtos.BucketEntry protoValue = entry.getValue();
      // TODO:We use ByteBuffAllocator.HEAP here, because we could not get the ByteBuffAllocator
      // which created by RpcServer elegantly.
      BucketEntry value = new BucketEntry(protoValue.getOffset(), protoValue.getLength(),
        protoValue.getDiskSizeWithHeader(), protoValue.getAccessCounter(),
        protoValue.getCachedTime(),
        protoValue.getPriority() == BucketCacheProtos.BlockPriority.memory, createRecycler,
        ByteBuffAllocator.HEAP);
      // This is the deserializer that we stored
      int oldIndex = protoValue.getDeserialiserIndex();
      String deserializerClass = deserializers.get(oldIndex);
      if (deserializerClass == null) {
        throw new IOException("Found deserializer index without matching entry.");
      }
      // Convert it to the identifier for the deserializer that we have in this runtime
      if (deserializerClass.equals(HFileBlock.BlockDeserializer.class.getName())) {
        int actualIndex = HFileBlock.BLOCK_DESERIALIZER.getDeserializerIdentifier();
        value.deserializerIndex = (byte) actualIndex;
      } else {
        // We could make this more plugable, but right now HFileBlock is the only implementation
        // of Cacheable outside of tests, so this might not ever matter.
        throw new IOException("Unknown deserializer class found: " + deserializerClass);
      }
      result.put(key, value);
      resultSet.add(key);
    }
    return new Pair<>(result, resultSet);
  }

  private static BlockType fromPb(BucketCacheProtos.BlockType blockType) {
    switch (blockType) {
      case data:
        return BlockType.DATA;
      case meta:
        return BlockType.META;
      case trailer:
        return BlockType.TRAILER;
      case index_v1:
        return BlockType.INDEX_V1;
      case file_info:
        return BlockType.FILE_INFO;
      case leaf_index:
        return BlockType.LEAF_INDEX;
      case root_index:
        return BlockType.ROOT_INDEX;
      case bloom_chunk:
        return BlockType.BLOOM_CHUNK;
      case encoded_data:
        return BlockType.ENCODED_DATA;
      case general_bloom_meta:
        return BlockType.GENERAL_BLOOM_META;
      case intermediate_index:
        return BlockType.INTERMEDIATE_INDEX;
      case delete_family_bloom_meta:
        return BlockType.DELETE_FAMILY_BLOOM_META;
      default:
        throw new Error("Unrecognized BlockType.");
    }
  }

  static Map<String, BucketCacheProtos.RegionFileSizeMap>
    toCachedPB(Map<String, Pair<String, Long>> prefetchedHfileNames) {
    Map<String, BucketCacheProtos.RegionFileSizeMap> tmpMap = new HashMap<>();
    prefetchedHfileNames.forEach((hfileName, regionPrefetchMap) -> {
      BucketCacheProtos.RegionFileSizeMap tmpRegionFileSize =
        BucketCacheProtos.RegionFileSizeMap.newBuilder().setRegionName(regionPrefetchMap.getFirst())
          .setRegionCachedSize(regionPrefetchMap.getSecond()).build();
      tmpMap.put(hfileName, tmpRegionFileSize);
    });
    return tmpMap;
  }

  static Map<String, Pair<String, Long>>
    fromPB(Map<String, BucketCacheProtos.RegionFileSizeMap> prefetchHFileNames) {
    Map<String, Pair<String, Long>> hfileMap = new HashMap<>();
    prefetchHFileNames.forEach((hfileName, regionPrefetchMap) -> {
      hfileMap.put(hfileName,
        new Pair<>(regionPrefetchMap.getRegionName(), regionPrefetchMap.getRegionCachedSize()));
    });
    return hfileMap;
  }
}
