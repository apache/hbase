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
package org.apache.hadoop.hbase.mob;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * The constants used in mob.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MobConstants {

  public static final String MOB_SCAN_RAW = "hbase.mob.scan.raw";
  public static final String MOB_CACHE_BLOCKS = "hbase.mob.cache.blocks";
  public static final String MOB_SCAN_REF_ONLY = "hbase.mob.scan.ref.only";

  public static final String MOB_FILE_CACHE_SIZE_KEY = "hbase.mob.file.cache.size";
  public static final int DEFAULT_MOB_FILE_CACHE_SIZE = 1000;

  public static final String MOB_DIR_NAME = "mobdir";
  public static final String MOB_REGION_NAME = ".mob";
  public static final byte[] MOB_REGION_NAME_BYTES = Bytes.toBytes(MOB_REGION_NAME);

  public static final String MOB_CLEANER_PERIOD = "hbase.master.mob.ttl.cleaner.period";
  public static final int DEFAULT_MOB_CLEANER_PERIOD = 24 * 60 * 60 * 1000; // one day

  public static final String MOB_SWEEP_TOOL_COMPACTION_START_DATE =
      "hbase.mob.sweep.tool.compaction.start.date";
  public static final String MOB_SWEEP_TOOL_COMPACTION_RATIO =
      "hbase.mob.sweep.tool.compaction.ratio";
  public static final String MOB_SWEEP_TOOL_COMPACTION_MERGEABLE_SIZE =
      "hbase.mob.sweep.tool.compaction.mergeable.size";

  public static final float DEFAULT_SWEEP_TOOL_MOB_COMPACTION_RATIO = 0.5f;
  public static final long DEFAULT_SWEEP_TOOL_MOB_COMPACTION_MERGEABLE_SIZE = 128 * 1024 * 1024;

  public static final String MOB_SWEEP_TOOL_COMPACTION_TEMP_DIR_NAME = "mobcompaction";

  public static final String MOB_SWEEP_TOOL_COMPACTION_MEMSTORE_FLUSH_SIZE =
      "hbase.mob.sweep.tool.compaction.memstore.flush.size";
  public static final long DEFAULT_MOB_SWEEP_TOOL_COMPACTION_MEMSTORE_FLUSH_SIZE =
      1024 * 1024 * 128; // 128M

  public static final String MOB_CACHE_EVICT_PERIOD = "hbase.mob.cache.evict.period";
  public static final String MOB_CACHE_EVICT_REMAIN_RATIO = "hbase.mob.cache.evict.remain.ratio";
  public static final Tag MOB_REF_TAG = new Tag(TagType.MOB_REFERENCE_TAG_TYPE,
      HConstants.EMPTY_BYTE_ARRAY);

  public static final float DEFAULT_EVICT_REMAIN_RATIO = 0.5f;
  public static final long DEFAULT_MOB_CACHE_EVICT_PERIOD = 3600l;

  public final static String TEMP_DIR_NAME = ".tmp";
  public final static byte[] MOB_TABLE_LOCK_SUFFIX = Bytes.toBytes(".mobLock");
  public final static String EMPTY_STRING = "";
  private MobConstants() {

  }
}
