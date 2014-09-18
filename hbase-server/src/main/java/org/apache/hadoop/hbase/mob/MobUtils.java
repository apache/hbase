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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * The mob utilities
 */
@InterfaceAudience.Private
public class MobUtils {

  private static final ThreadLocal<SimpleDateFormat> LOCAL_FORMAT =
      new ThreadLocal<SimpleDateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      return new SimpleDateFormat("yyyyMMdd");
    }
  };

  /**
   * Indicates whether the column family is a mob one.
   * @param hcd The descriptor of a column family.
   * @return True if this column family is a mob one, false if it's not.
   */
  public static boolean isMobFamily(HColumnDescriptor hcd) {
    byte[] isMob = hcd.getValue(MobConstants.IS_MOB);
    return isMob != null && isMob.length == 1 && Bytes.toBoolean(isMob);
  }

  /**
   * Gets the mob threshold.
   * If the size of a cell value is larger than this threshold, it's regarded as a mob.
   * The default threshold is 1024*100(100K)B.
   * @param hcd The descriptor of a column family.
   * @return The threshold.
   */
  public static long getMobThreshold(HColumnDescriptor hcd) {
    byte[] threshold = hcd.getValue(MobConstants.MOB_THRESHOLD);
    return threshold != null && threshold.length == Bytes.SIZEOF_LONG ? Bytes.toLong(threshold)
        : MobConstants.DEFAULT_MOB_THRESHOLD;
  }

  /**
   * Formats a date to a string.
   * @param date The date.
   * @return The string format of the date, it's yyyymmdd.
   */
  public static String formatDate(Date date) {
    return LOCAL_FORMAT.get().format(date);
  }

  /**
   * Parses the string to a date.
   * @param dateString The string format of a date, it's yyyymmdd.
   * @return A date.
   * @throws ParseException
   */
  public static Date parseDate(String dateString) throws ParseException {
    return LOCAL_FORMAT.get().parse(dateString);
  }

  /**
   * Whether the current cell is a mob reference cell.
   * @param cell The current cell.
   * @return True if the cell has a mob reference tag, false if it doesn't.
   */
  public static boolean isMobReferenceCell(Cell cell) {
    if (cell.getTagsLength() > 0) {
      Tag tag = Tag.getTag(cell.getTagsArray(), cell.getTagsOffset(), cell.getTagsLength(),
          TagType.MOB_REFERENCE_TAG_TYPE);
      return tag != null;
    }
    return false;
  }

  /**
   * Whether the tag list has a mob reference tag.
   * @param tags The tag list.
   * @return True if the list has a mob reference tag, false if it doesn't.
   */
  public static boolean hasMobReferenceTag(List<Tag> tags) {
    if (!tags.isEmpty()) {
      for (Tag tag : tags) {
        if (tag.getType() == TagType.MOB_REFERENCE_TAG_TYPE) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Indicates whether it's a raw scan.
   * The information is set in the attribute "hbase.mob.scan.raw" of scan.
   * For a mob cell, in a normal scan the scanners retrieves the mob cell from the mob file.
   * In a raw scan, the scanner directly returns cell in HBase without retrieve the one in
   * the mob file.
   * @param scan The current scan.
   * @return True if it's a raw scan.
   */
  public static boolean isRawMobScan(Scan scan) {
    byte[] raw = scan.getAttribute(MobConstants.MOB_SCAN_RAW);
    try {
      return raw != null && Bytes.toBoolean(raw);
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  /**
   * Indicates whether the scan contains the information of caching blocks.
   * The information is set in the attribute "hbase.mob.cache.blocks" of scan.
   * @param scan The current scan.
   * @return True when the Scan attribute specifies to cache the MOB blocks.
   */
  public static boolean isCacheMobBlocks(Scan scan) {
    byte[] cache = scan.getAttribute(MobConstants.MOB_CACHE_BLOCKS);
    try {
      return cache != null && Bytes.toBoolean(cache);
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  /**
   * Sets the attribute of caching blocks in the scan.
   *
   * @param scan
   *          The current scan.
   * @param cacheBlocks
   *          True, set the attribute of caching blocks into the scan, the scanner with this scan
   *          caches blocks.
   *          False, the scanner doesn't cache blocks for this scan.
   */
  public static void setCacheMobBlocks(Scan scan, boolean cacheBlocks) {
    scan.setAttribute(MobConstants.MOB_CACHE_BLOCKS, Bytes.toBytes(cacheBlocks));
  }

  /**
   * Gets the root dir of the mob files.
   * It's {HBASE_DIR}/mobdir.
   * @param conf The current configuration.
   * @return the root dir of the mob file.
   */
  public static Path getMobHome(Configuration conf) {
    Path hbaseDir = new Path(conf.get(HConstants.HBASE_DIR));
    return new Path(hbaseDir, MobConstants.MOB_DIR_NAME);
  }

  /**
   * Gets the region dir of the mob files.
   * It's {HBASE_DIR}/mobdir/{namespace}/{tableName}/{regionEncodedName}.
   * @param conf The current configuration.
   * @param tableName The current table name.
   * @return The region dir of the mob files.
   */
  public static Path getMobRegionPath(Configuration conf, TableName tableName) {
    Path tablePath = FSUtils.getTableDir(MobUtils.getMobHome(conf), tableName);
    HRegionInfo regionInfo = getMobRegionInfo(tableName);
    return new Path(tablePath, regionInfo.getEncodedName());
  }

  /**
   * Gets the family dir of the mob files.
   * It's {HBASE_DIR}/mobdir/{namespace}/{tableName}/{regionEncodedName}/{columnFamilyName}.
   * @param conf The current configuration.
   * @param tableName The current table name.
   * @param familyName The current family name.
   * @return The family dir of the mob files.
   */
  public static Path getMobFamilyPath(Configuration conf, TableName tableName, String familyName) {
    return new Path(getMobRegionPath(conf, tableName), familyName);
  }

  /**
   * Gets the family dir of the mob files.
   * It's {HBASE_DIR}/mobdir/{namespace}/{tableName}/{regionEncodedName}/{columnFamilyName}.
   * @param regionPath The path of mob region which is a dummy one.
   * @param familyName The current family name.
   * @return The family dir of the mob files.
   */
  public static Path getMobFamilyPath(Path regionPath, String familyName) {
    return new Path(regionPath, familyName);
  }

  /**
   * Gets the HRegionInfo of the mob files.
   * This is a dummy region. The mob files are not saved in a region in HBase.
   * This is only used in mob snapshot. It's internally used only.
   * @param tableName
   * @return A dummy mob region info.
   */
  public static HRegionInfo getMobRegionInfo(TableName tableName) {
    HRegionInfo info = new HRegionInfo(tableName, MobConstants.MOB_REGION_NAME_BYTES,
        HConstants.EMPTY_END_ROW, false, 0);
    return info;
  }

  /**
   * Creates a mob reference KeyValue.
   * The value of the mob reference KeyValue is mobCellValueSize + mobFileName.
   * @param kv The original KeyValue.
   * @param fileName The mob file name where the mob reference KeyValue is written.
   * @param tableNameTag The tag of the current table name. It's very important in
   *                        cloning the snapshot.
   * @return The mob reference KeyValue.
   */
  public static KeyValue createMobRefKeyValue(KeyValue kv, byte[] fileName, Tag tableNameTag) {
    // Append the tags to the KeyValue.
    // The key is same, the value is the filename of the mob file
    List<Tag> existingTags = Tag.asList(kv.getTagsArray(), kv.getTagsOffset(), kv.getTagsLength());
    existingTags.add(MobConstants.MOB_REF_TAG);
    // Add the tag of the source table name, this table is where this mob file is flushed
    // from.
    // It's very useful in cloning the snapshot. When reading from the cloning table, we need to
    // find the original mob files by this table name. For details please see cloning
    // snapshot for mob files.
    existingTags.add(tableNameTag);
    int valueLength = kv.getValueLength();
    byte[] refValue = Bytes.add(Bytes.toBytes(valueLength), fileName);
    KeyValue reference = new KeyValue(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(),
        kv.getFamilyArray(), kv.getFamilyOffset(), kv.getFamilyLength(), kv.getQualifierArray(),
        kv.getQualifierOffset(), kv.getQualifierLength(), kv.getTimestamp(), KeyValue.Type.Put,
        refValue, 0, refValue.length, existingTags);
    reference.setSequenceId(kv.getSequenceId());
    return reference;
  }

  /**
   * Indicates whether the current mob ref cell has a valid value.
   * A mob ref cell has a mob reference tag.
   * The value of a mob ref cell consists of two parts, real mob value length and mob file name.
   * The real mob value length takes 4 bytes.
   * The remaining part is the mob file name.
   * @param cell The mob ref cell.
   * @return True if the cell has a valid value.
   */
  public static boolean isValidMobRefCellValue(Cell cell) {
    return cell.getValueLength() > Bytes.SIZEOF_INT;
  }

  /**
   * Gets the mob value length from the mob ref cell.
   * A mob ref cell has a mob reference tag.
   * The value of a mob ref cell consists of two parts, real mob value length and mob file name.
   * The real mob value length takes 4 bytes.
   * The remaining part is the mob file name.
   * @param cell The mob ref cell.
   * @return The real mob value length.
   */
  public static int getMobValueLength(Cell cell) {
    return Bytes.toInt(cell.getValueArray(), cell.getValueOffset(), Bytes.SIZEOF_INT);
  }

  /**
   * Gets the mob file name from the mob ref cell.
   * A mob ref cell has a mob reference tag.
   * The value of a mob ref cell consists of two parts, real mob value length and mob file name.
   * The real mob value length takes 4 bytes.
   * The remaining part is the mob file name.
   * @param cell The mob ref cell.
   * @return The mob file name.
   */
  public static String getMobFileName(Cell cell) {
    return Bytes.toString(cell.getValueArray(), cell.getValueOffset() + Bytes.SIZEOF_INT,
        cell.getValueLength() - Bytes.SIZEOF_INT);
  }
}
