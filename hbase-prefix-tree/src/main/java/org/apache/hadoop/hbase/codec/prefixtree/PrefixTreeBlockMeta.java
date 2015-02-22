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

package org.apache.hadoop.hbase.codec.prefixtree;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.codec.prefixtree.encode.other.LongEncoder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.vint.UVIntTool;
import org.apache.hadoop.hbase.util.vint.UVLongTool;

/**
 * Information about the block.  Stored at the beginning of the byte[].  Contains things
 * like minimum timestamp and width of FInts in the row tree.
 *
 * Most fields stored in VInts that get decoded on the first access of each new block.
 */
@InterfaceAudience.Private
public class PrefixTreeBlockMeta {

  /******************* static fields ********************/

  public static final int VERSION = 0;

  public static final int MAX_FAMILY_LENGTH = Byte.MAX_VALUE;// hard-coded in KeyValue

  public static final int
    NUM_LONGS = 2,
    NUM_INTS = 28,
    NUM_SHORTS = 0,//keyValueTypeWidth not persisted
    NUM_SINGLE_BYTES = 2,
    MAX_BYTES = Bytes.SIZEOF_LONG * NUM_LONGS
        + Bytes.SIZEOF_SHORT * NUM_SHORTS
        + Bytes.SIZEOF_INT * NUM_INTS
        + NUM_SINGLE_BYTES;


  /**************** transient fields *********************/

  protected int arrayOffset;
  protected int bufferOffset;


  /**************** persisted fields **********************/

  // PrefixTree version to allow future format modifications
  protected int version;
  protected int numMetaBytes;
  protected int numKeyValueBytes;
  protected boolean includesMvccVersion;//probably don't need this explicitly, but only 1 byte

  // split the byte[] into 6 sections for the different data types
  protected int numRowBytes;
  protected int numFamilyBytes;
  protected int numQualifierBytes;
  protected int numTimestampBytes;
  protected int numMvccVersionBytes;
  protected int numValueBytes;
  protected int numTagsBytes;

  // number of bytes in each section of fixed width FInts
  protected int nextNodeOffsetWidth;
  protected int familyOffsetWidth;
  protected int qualifierOffsetWidth;
  protected int timestampIndexWidth;
  protected int mvccVersionIndexWidth;
  protected int valueOffsetWidth;
  protected int valueLengthWidth;
  protected int tagsOffsetWidth;

  // used to pre-allocate structures for reading
  protected int rowTreeDepth;
  protected int maxRowLength;
  protected int maxQualifierLength;
  protected int maxTagsLength;

  // the timestamp from which the deltas are calculated
  protected long minTimestamp;
  protected int timestampDeltaWidth;
  protected long minMvccVersion;
  protected int mvccVersionDeltaWidth;

  protected boolean allSameType;
  protected byte allTypes;

  protected int numUniqueRows;
  protected int numUniqueFamilies;
  protected int numUniqueQualifiers;
  protected int numUniqueTags;


  /***************** constructors ********************/

  public PrefixTreeBlockMeta() {
  }

  public PrefixTreeBlockMeta(InputStream is) throws IOException{
    this.version = VERSION;
    this.arrayOffset = 0;
    this.bufferOffset = 0;
    readVariableBytesFromInputStream(is);
  }

  /**
   * @param buffer positioned at start of PtBlockMeta
   */
  public PrefixTreeBlockMeta(ByteBuffer buffer) {
    initOnBlock(buffer);
  }

  public void initOnBlock(ByteBuffer buffer) {
    arrayOffset = buffer.arrayOffset();
    bufferOffset = buffer.position();
    readVariableBytesFromArray(buffer.array(), arrayOffset + bufferOffset);
  }


  /**************** operate on each field **********************/

  public int calculateNumMetaBytes(){
    int numBytes = 0;
    numBytes += UVIntTool.numBytes(version);
    numBytes += UVLongTool.numBytes(numMetaBytes);
    numBytes += UVIntTool.numBytes(numKeyValueBytes);
    ++numBytes;//os.write(getIncludesMvccVersion());

    numBytes += UVIntTool.numBytes(numRowBytes);
    numBytes += UVIntTool.numBytes(numFamilyBytes);
    numBytes += UVIntTool.numBytes(numQualifierBytes);
    numBytes += UVIntTool.numBytes(numTagsBytes);
    numBytes += UVIntTool.numBytes(numTimestampBytes);
    numBytes += UVIntTool.numBytes(numMvccVersionBytes);
    numBytes += UVIntTool.numBytes(numValueBytes);

    numBytes += UVIntTool.numBytes(nextNodeOffsetWidth);
    numBytes += UVIntTool.numBytes(familyOffsetWidth);
    numBytes += UVIntTool.numBytes(qualifierOffsetWidth);
    numBytes += UVIntTool.numBytes(tagsOffsetWidth);
    numBytes += UVIntTool.numBytes(timestampIndexWidth);
    numBytes += UVIntTool.numBytes(mvccVersionIndexWidth);
    numBytes += UVIntTool.numBytes(valueOffsetWidth);
    numBytes += UVIntTool.numBytes(valueLengthWidth);

    numBytes += UVIntTool.numBytes(rowTreeDepth);
    numBytes += UVIntTool.numBytes(maxRowLength);
    numBytes += UVIntTool.numBytes(maxQualifierLength);
    numBytes += UVIntTool.numBytes(maxTagsLength);

    numBytes += UVLongTool.numBytes(minTimestamp);
    numBytes += UVIntTool.numBytes(timestampDeltaWidth);
    numBytes += UVLongTool.numBytes(minMvccVersion);
    numBytes += UVIntTool.numBytes(mvccVersionDeltaWidth);
    ++numBytes;//os.write(getAllSameTypeByte());
    ++numBytes;//os.write(allTypes);

    numBytes += UVIntTool.numBytes(numUniqueRows);
    numBytes += UVIntTool.numBytes(numUniqueFamilies);
    numBytes += UVIntTool.numBytes(numUniqueQualifiers);
    numBytes += UVIntTool.numBytes(numUniqueTags);
    return numBytes;
  }

  public void writeVariableBytesToOutputStream(OutputStream os) throws IOException{
      UVIntTool.writeBytes(version, os);
      UVIntTool.writeBytes(numMetaBytes, os);
      UVIntTool.writeBytes(numKeyValueBytes, os);
      os.write(getIncludesMvccVersionByte());

      UVIntTool.writeBytes(numRowBytes, os);
      UVIntTool.writeBytes(numFamilyBytes, os);
      UVIntTool.writeBytes(numQualifierBytes, os);
      UVIntTool.writeBytes(numTagsBytes, os);
      UVIntTool.writeBytes(numTimestampBytes, os);
      UVIntTool.writeBytes(numMvccVersionBytes, os);
      UVIntTool.writeBytes(numValueBytes, os);

      UVIntTool.writeBytes(nextNodeOffsetWidth, os);
      UVIntTool.writeBytes(familyOffsetWidth, os);
      UVIntTool.writeBytes(qualifierOffsetWidth, os);
      UVIntTool.writeBytes(tagsOffsetWidth, os);
      UVIntTool.writeBytes(timestampIndexWidth, os);
      UVIntTool.writeBytes(mvccVersionIndexWidth, os);
      UVIntTool.writeBytes(valueOffsetWidth, os);
      UVIntTool.writeBytes(valueLengthWidth, os);

      UVIntTool.writeBytes(rowTreeDepth, os);
      UVIntTool.writeBytes(maxRowLength, os);
      UVIntTool.writeBytes(maxQualifierLength, os);
      UVIntTool.writeBytes(maxTagsLength, os);

      UVLongTool.writeBytes(minTimestamp, os);
      UVIntTool.writeBytes(timestampDeltaWidth, os);
      UVLongTool.writeBytes(minMvccVersion, os);
      UVIntTool.writeBytes(mvccVersionDeltaWidth, os);
      os.write(getAllSameTypeByte());
      os.write(allTypes);

      UVIntTool.writeBytes(numUniqueRows, os);
      UVIntTool.writeBytes(numUniqueFamilies, os);
      UVIntTool.writeBytes(numUniqueQualifiers, os);
      UVIntTool.writeBytes(numUniqueTags, os);
  }

  public void readVariableBytesFromInputStream(InputStream is) throws IOException{
      version = UVIntTool.getInt(is);
      numMetaBytes = UVIntTool.getInt(is);
      numKeyValueBytes = UVIntTool.getInt(is);
      setIncludesMvccVersion((byte) is.read());

      numRowBytes = UVIntTool.getInt(is);
      numFamilyBytes = UVIntTool.getInt(is);
      numQualifierBytes = UVIntTool.getInt(is);
      numTagsBytes = UVIntTool.getInt(is);
      numTimestampBytes = UVIntTool.getInt(is);
      numMvccVersionBytes = UVIntTool.getInt(is);
      numValueBytes = UVIntTool.getInt(is);

      nextNodeOffsetWidth = UVIntTool.getInt(is);
      familyOffsetWidth = UVIntTool.getInt(is);
      qualifierOffsetWidth = UVIntTool.getInt(is);
      tagsOffsetWidth = UVIntTool.getInt(is);
      timestampIndexWidth = UVIntTool.getInt(is);
      mvccVersionIndexWidth = UVIntTool.getInt(is);
      valueOffsetWidth = UVIntTool.getInt(is);
      valueLengthWidth = UVIntTool.getInt(is);

      rowTreeDepth = UVIntTool.getInt(is);
      maxRowLength = UVIntTool.getInt(is);
      maxQualifierLength = UVIntTool.getInt(is);
      maxTagsLength = UVIntTool.getInt(is);

      minTimestamp = UVLongTool.getLong(is);
      timestampDeltaWidth = UVIntTool.getInt(is);
      minMvccVersion = UVLongTool.getLong(is);
      mvccVersionDeltaWidth = UVIntTool.getInt(is);

      setAllSameType((byte) is.read());
      allTypes = (byte) is.read();

      numUniqueRows = UVIntTool.getInt(is);
      numUniqueFamilies = UVIntTool.getInt(is);
      numUniqueQualifiers = UVIntTool.getInt(is);
      numUniqueTags = UVIntTool.getInt(is);
  }

  public void readVariableBytesFromArray(byte[] bytes, int offset) {
    int position = offset;

    version = UVIntTool.getInt(bytes, position);
    position += UVIntTool.numBytes(version);
    numMetaBytes = UVIntTool.getInt(bytes, position);
    position += UVIntTool.numBytes(numMetaBytes);
    numKeyValueBytes = UVIntTool.getInt(bytes, position);
    position += UVIntTool.numBytes(numKeyValueBytes);
    setIncludesMvccVersion(bytes[position]);
    ++position;

    numRowBytes = UVIntTool.getInt(bytes, position);
    position += UVIntTool.numBytes(numRowBytes);
    numFamilyBytes = UVIntTool.getInt(bytes, position);
    position += UVIntTool.numBytes(numFamilyBytes);
    numQualifierBytes = UVIntTool.getInt(bytes, position);
    position += UVIntTool.numBytes(numQualifierBytes);
    numTagsBytes = UVIntTool.getInt(bytes, position);
    position += UVIntTool.numBytes(numTagsBytes);
    numTimestampBytes = UVIntTool.getInt(bytes, position);
    position += UVIntTool.numBytes(numTimestampBytes);
    numMvccVersionBytes = UVIntTool.getInt(bytes, position);
    position += UVIntTool.numBytes(numMvccVersionBytes);
    numValueBytes = UVIntTool.getInt(bytes, position);
    position += UVIntTool.numBytes(numValueBytes);

    nextNodeOffsetWidth = UVIntTool.getInt(bytes, position);
    position += UVIntTool.numBytes(nextNodeOffsetWidth);
    familyOffsetWidth = UVIntTool.getInt(bytes, position);
    position += UVIntTool.numBytes(familyOffsetWidth);
    qualifierOffsetWidth = UVIntTool.getInt(bytes, position);
    position += UVIntTool.numBytes(qualifierOffsetWidth);
    tagsOffsetWidth = UVIntTool.getInt(bytes, position);
    position += UVIntTool.numBytes(tagsOffsetWidth);
    timestampIndexWidth = UVIntTool.getInt(bytes, position);
    position += UVIntTool.numBytes(timestampIndexWidth);
    mvccVersionIndexWidth = UVIntTool.getInt(bytes, position);
    position += UVIntTool.numBytes(mvccVersionIndexWidth);
    valueOffsetWidth = UVIntTool.getInt(bytes, position);
    position += UVIntTool.numBytes(valueOffsetWidth);
    valueLengthWidth = UVIntTool.getInt(bytes, position);
    position += UVIntTool.numBytes(valueLengthWidth);

    rowTreeDepth = UVIntTool.getInt(bytes, position);
    position += UVIntTool.numBytes(rowTreeDepth);
    maxRowLength = UVIntTool.getInt(bytes, position);
    position += UVIntTool.numBytes(maxRowLength);
    maxQualifierLength = UVIntTool.getInt(bytes, position);
    position += UVIntTool.numBytes(maxQualifierLength);
    maxTagsLength = UVIntTool.getInt(bytes, position);
    position += UVIntTool.numBytes(maxTagsLength);
    minTimestamp = UVLongTool.getLong(bytes, position);
    position += UVLongTool.numBytes(minTimestamp);
    timestampDeltaWidth = UVIntTool.getInt(bytes, position);
    position += UVIntTool.numBytes(timestampDeltaWidth);
    minMvccVersion = UVLongTool.getLong(bytes, position);
    position += UVLongTool.numBytes(minMvccVersion);
    mvccVersionDeltaWidth = UVIntTool.getInt(bytes, position);
    position += UVIntTool.numBytes(mvccVersionDeltaWidth);

    setAllSameType(bytes[position]);
    ++position;
    allTypes = bytes[position];
    ++position;

    numUniqueRows = UVIntTool.getInt(bytes, position);
    position += UVIntTool.numBytes(numUniqueRows);
    numUniqueFamilies = UVIntTool.getInt(bytes, position);
    position += UVIntTool.numBytes(numUniqueFamilies);
    numUniqueQualifiers = UVIntTool.getInt(bytes, position);
    position += UVIntTool.numBytes(numUniqueQualifiers);
    numUniqueTags = UVIntTool.getInt(bytes, position);
    position += UVIntTool.numBytes(numUniqueTags);
  }

  //TODO method that can read directly from ByteBuffer instead of InputStream


  /*************** methods *************************/

  public int getKeyValueTypeWidth() {
    return allSameType ? 0 : 1;
  }

  public byte getIncludesMvccVersionByte() {
    return includesMvccVersion ? (byte) 1 : (byte) 0;
  }

  public void setIncludesMvccVersion(byte includesMvccVersionByte) {
    includesMvccVersion = includesMvccVersionByte != 0;
  }

  public byte getAllSameTypeByte() {
    return allSameType ? (byte) 1 : (byte) 0;
  }

  public void setAllSameType(byte allSameTypeByte) {
    allSameType = allSameTypeByte != 0;
  }

  public boolean isAllSameTimestamp() {
    return timestampIndexWidth == 0;
  }

  public boolean isAllSameMvccVersion() {
    return mvccVersionIndexWidth == 0;
  }

  public void setTimestampFields(LongEncoder encoder){
    this.minTimestamp = encoder.getMin();
    this.timestampIndexWidth = encoder.getBytesPerIndex();
    this.timestampDeltaWidth = encoder.getBytesPerDelta();
    this.numTimestampBytes = encoder.getTotalCompressedBytes();
  }

  public void setMvccVersionFields(LongEncoder encoder){
    this.minMvccVersion = encoder.getMin();
    this.mvccVersionIndexWidth = encoder.getBytesPerIndex();
    this.mvccVersionDeltaWidth = encoder.getBytesPerDelta();
    this.numMvccVersionBytes = encoder.getTotalCompressedBytes();
  }


  /*************** Object methods *************************/

  /**
   * Generated by Eclipse
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    PrefixTreeBlockMeta other = (PrefixTreeBlockMeta) obj;
    if (allSameType != other.allSameType)
      return false;
    if (allTypes != other.allTypes)
      return false;
    if (arrayOffset != other.arrayOffset)
      return false;
    if (bufferOffset != other.bufferOffset)
      return false;
    if (valueLengthWidth != other.valueLengthWidth)
      return false;
    if (valueOffsetWidth != other.valueOffsetWidth)
      return false;
    if (familyOffsetWidth != other.familyOffsetWidth)
      return false;
    if (includesMvccVersion != other.includesMvccVersion)
      return false;
    if (maxQualifierLength != other.maxQualifierLength)
      return false;
    if (maxTagsLength != other.maxTagsLength)
      return false;
    if (maxRowLength != other.maxRowLength)
      return false;
    if (mvccVersionDeltaWidth != other.mvccVersionDeltaWidth)
      return false;
    if (mvccVersionIndexWidth != other.mvccVersionIndexWidth)
      return false;
    if (minMvccVersion != other.minMvccVersion)
      return false;
    if (minTimestamp != other.minTimestamp)
      return false;
    if (nextNodeOffsetWidth != other.nextNodeOffsetWidth)
      return false;
    if (numValueBytes != other.numValueBytes)
      return false;
    if (numFamilyBytes != other.numFamilyBytes)
      return false;
    if (numMvccVersionBytes != other.numMvccVersionBytes)
      return false;
    if (numMetaBytes != other.numMetaBytes)
      return false;
    if (numQualifierBytes != other.numQualifierBytes)
      return false;
    if (numTagsBytes != other.numTagsBytes)
      return false;
    if (numRowBytes != other.numRowBytes)
      return false;
    if (numTimestampBytes != other.numTimestampBytes)
      return false;
    if (numUniqueFamilies != other.numUniqueFamilies)
      return false;
    if (numUniqueQualifiers != other.numUniqueQualifiers)
      return false;
    if (numUniqueTags != other.numUniqueTags)
      return false;
    if (numUniqueRows != other.numUniqueRows)
      return false;
    if (numKeyValueBytes != other.numKeyValueBytes)
      return false;
    if (qualifierOffsetWidth != other.qualifierOffsetWidth)
      return false;
    if(tagsOffsetWidth !=  other.tagsOffsetWidth) 
      return false;
    if (rowTreeDepth != other.rowTreeDepth)
      return false;
    if (timestampDeltaWidth != other.timestampDeltaWidth)
      return false;
    if (timestampIndexWidth != other.timestampIndexWidth)
      return false;
    if (version != other.version)
      return false;
    return true;
  }

  /**
   * Generated by Eclipse
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (allSameType ? 1231 : 1237);
    result = prime * result + allTypes;
    result = prime * result + arrayOffset;
    result = prime * result + bufferOffset;
    result = prime * result + valueLengthWidth;
    result = prime * result + valueOffsetWidth;
    result = prime * result + familyOffsetWidth;
    result = prime * result + (includesMvccVersion ? 1231 : 1237);
    result = prime * result + maxQualifierLength;
    result = prime * result + maxTagsLength;
    result = prime * result + maxRowLength;
    result = prime * result + mvccVersionDeltaWidth;
    result = prime * result + mvccVersionIndexWidth;
    result = prime * result + (int) (minMvccVersion ^ (minMvccVersion >>> 32));
    result = prime * result + (int) (minTimestamp ^ (minTimestamp >>> 32));
    result = prime * result + nextNodeOffsetWidth;
    result = prime * result + numValueBytes;
    result = prime * result + numFamilyBytes;
    result = prime * result + numMvccVersionBytes;
    result = prime * result + numMetaBytes;
    result = prime * result + numQualifierBytes;
    result = prime * result + numTagsBytes;
    result = prime * result + numRowBytes;
    result = prime * result + numTimestampBytes;
    result = prime * result + numUniqueFamilies;
    result = prime * result + numUniqueQualifiers;
    result = prime * result + numUniqueTags;
    result = prime * result + numUniqueRows;
    result = prime * result + numKeyValueBytes;
    result = prime * result + qualifierOffsetWidth;
    result = prime * result + tagsOffsetWidth;
    result = prime * result + rowTreeDepth;
    result = prime * result + timestampDeltaWidth;
    result = prime * result + timestampIndexWidth;
    result = prime * result + version;
    return result;
  }

  /**
   * Generated by Eclipse
   */
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("PtBlockMeta [arrayOffset=");
    builder.append(arrayOffset);
    builder.append(", bufferOffset=");
    builder.append(bufferOffset);
    builder.append(", version=");
    builder.append(version);
    builder.append(", numMetaBytes=");
    builder.append(numMetaBytes);
    builder.append(", numKeyValueBytes=");
    builder.append(numKeyValueBytes);
    builder.append(", includesMvccVersion=");
    builder.append(includesMvccVersion);
    builder.append(", numRowBytes=");
    builder.append(numRowBytes);
    builder.append(", numFamilyBytes=");
    builder.append(numFamilyBytes);
    builder.append(", numQualifierBytes=");
    builder.append(numQualifierBytes);
    builder.append(", numTimestampBytes=");
    builder.append(numTimestampBytes);
    builder.append(", numMvccVersionBytes=");
    builder.append(numMvccVersionBytes);
    builder.append(", numValueBytes=");
    builder.append(numValueBytes);
    builder.append(", numTagBytes=");
    builder.append(numTagsBytes);
    builder.append(", nextNodeOffsetWidth=");
    builder.append(nextNodeOffsetWidth);
    builder.append(", familyOffsetWidth=");
    builder.append(familyOffsetWidth);
    builder.append(", qualifierOffsetWidth=");
    builder.append(qualifierOffsetWidth);
    builder.append(", tagOffsetWidth=");
    builder.append(tagsOffsetWidth);
    builder.append(", timestampIndexWidth=");
    builder.append(timestampIndexWidth);
    builder.append(", mvccVersionIndexWidth=");
    builder.append(mvccVersionIndexWidth);
    builder.append(", valueOffsetWidth=");
    builder.append(valueOffsetWidth);
    builder.append(", valueLengthWidth=");
    builder.append(valueLengthWidth);
    builder.append(", rowTreeDepth=");
    builder.append(rowTreeDepth);
    builder.append(", maxRowLength=");
    builder.append(maxRowLength);
    builder.append(", maxQualifierLength=");
    builder.append(maxQualifierLength);
    builder.append(", maxTagLength=");
    builder.append(maxTagsLength);
    builder.append(", minTimestamp=");
    builder.append(minTimestamp);
    builder.append(", timestampDeltaWidth=");
    builder.append(timestampDeltaWidth);
    builder.append(", minMvccVersion=");
    builder.append(minMvccVersion);
    builder.append(", mvccVersionDeltaWidth=");
    builder.append(mvccVersionDeltaWidth);
    builder.append(", allSameType=");
    builder.append(allSameType);
    builder.append(", allTypes=");
    builder.append(allTypes);
    builder.append(", numUniqueRows=");
    builder.append(numUniqueRows);
    builder.append(", numUniqueFamilies=");
    builder.append(numUniqueFamilies);
    builder.append(", numUniqueQualifiers=");
    builder.append(numUniqueQualifiers);
    builder.append(", numUniqueTags=");
    builder.append(numUniqueTags);
    builder.append("]");
    return builder.toString();
  }


  /************** absolute getters *******************/

  public int getAbsoluteMetaOffset() {
    return arrayOffset + bufferOffset;
  }

  public int getAbsoluteRowOffset() {
    return getAbsoluteMetaOffset() + numMetaBytes;
  }

  public int getAbsoluteFamilyOffset() {
    return getAbsoluteRowOffset() + numRowBytes;
  }

  public int getAbsoluteQualifierOffset() {
    return getAbsoluteFamilyOffset() + numFamilyBytes;
  }

  public int getAbsoluteTagsOffset() {
    return getAbsoluteQualifierOffset() + numQualifierBytes;
  }

  public int getAbsoluteTimestampOffset() {
    return getAbsoluteTagsOffset() + numTagsBytes;
  }

  public int getAbsoluteMvccVersionOffset() {
    return getAbsoluteTimestampOffset() + numTimestampBytes;
  }

  public int getAbsoluteValueOffset() {
    return getAbsoluteMvccVersionOffset() + numMvccVersionBytes;
  }


  /*************** get/set ***************************/

  public int getTimestampDeltaWidth() {
    return timestampDeltaWidth;
  }

  public void setTimestampDeltaWidth(int timestampDeltaWidth) {
    this.timestampDeltaWidth = timestampDeltaWidth;
  }

  public int getValueOffsetWidth() {
    return valueOffsetWidth;
  }

  public int getTagsOffsetWidth() {
    return tagsOffsetWidth;
  }

  public void setValueOffsetWidth(int dataOffsetWidth) {
    this.valueOffsetWidth = dataOffsetWidth;
  }

  public void setTagsOffsetWidth(int dataOffsetWidth) {
    this.tagsOffsetWidth = dataOffsetWidth;
  }

  public int getValueLengthWidth() {
    return valueLengthWidth;
  }

  public void setValueLengthWidth(int dataLengthWidth) {
    this.valueLengthWidth = dataLengthWidth;
  }

  public int getMaxRowLength() {
    return maxRowLength;
  }

  public void setMaxRowLength(int maxRowLength) {
    this.maxRowLength = maxRowLength;
  }

  public long getMinTimestamp() {
    return minTimestamp;
  }

  public void setMinTimestamp(long minTimestamp) {
    this.minTimestamp = minTimestamp;
  }

  public byte getAllTypes() {
    return allTypes;
  }

  public void setAllTypes(byte allTypes) {
    this.allTypes = allTypes;
  }

  public boolean isAllSameType() {
    return allSameType;
  }

  public void setAllSameType(boolean allSameType) {
    this.allSameType = allSameType;
  }

  public int getNextNodeOffsetWidth() {
    return nextNodeOffsetWidth;
  }

  public void setNextNodeOffsetWidth(int nextNodeOffsetWidth) {
    this.nextNodeOffsetWidth = nextNodeOffsetWidth;
  }

  public int getNumRowBytes() {
    return numRowBytes;
  }

  public void setNumRowBytes(int numRowBytes) {
    this.numRowBytes = numRowBytes;
  }

  public int getNumTimestampBytes() {
    return numTimestampBytes;
  }

  public void setNumTimestampBytes(int numTimestampBytes) {
    this.numTimestampBytes = numTimestampBytes;
  }

  public int getNumValueBytes() {
    return numValueBytes;
  }

  public int getNumTagsBytes() {
    return numTagsBytes;
  }

  public void setNumTagsBytes(int numTagBytes){
    this.numTagsBytes = numTagBytes;
  }

  public void setNumValueBytes(int numValueBytes) {
    this.numValueBytes = numValueBytes;
  }

  public int getNumMetaBytes() {
    return numMetaBytes;
  }

  public void setNumMetaBytes(int numMetaBytes) {
    this.numMetaBytes = numMetaBytes;
  }

  public int getArrayOffset() {
    return arrayOffset;
  }

  public void setArrayOffset(int arrayOffset) {
    this.arrayOffset = arrayOffset;
  }

  public int getBufferOffset() {
    return bufferOffset;
  }

  public void setBufferOffset(int bufferOffset) {
    this.bufferOffset = bufferOffset;
  }

  public int getNumKeyValueBytes() {
    return numKeyValueBytes;
  }

  public void setNumKeyValueBytes(int numKeyValueBytes) {
    this.numKeyValueBytes = numKeyValueBytes;
  }

  public int getRowTreeDepth() {
    return rowTreeDepth;
  }

  public void setRowTreeDepth(int rowTreeDepth) {
    this.rowTreeDepth = rowTreeDepth;
  }

  public int getNumMvccVersionBytes() {
    return numMvccVersionBytes;
  }

  public void setNumMvccVersionBytes(int numMvccVersionBytes) {
    this.numMvccVersionBytes = numMvccVersionBytes;
  }

  public int getMvccVersionDeltaWidth() {
    return mvccVersionDeltaWidth;
  }

  public void setMvccVersionDeltaWidth(int mvccVersionDeltaWidth) {
    this.mvccVersionDeltaWidth = mvccVersionDeltaWidth;
  }

  public long getMinMvccVersion() {
    return minMvccVersion;
  }

  public void setMinMvccVersion(long minMvccVersion) {
    this.minMvccVersion = minMvccVersion;
  }

  public int getNumFamilyBytes() {
    return numFamilyBytes;
  }

  public void setNumFamilyBytes(int numFamilyBytes) {
    this.numFamilyBytes = numFamilyBytes;
  }

  public int getFamilyOffsetWidth() {
    return familyOffsetWidth;
  }

  public void setFamilyOffsetWidth(int familyOffsetWidth) {
    this.familyOffsetWidth = familyOffsetWidth;
  }

  public int getNumUniqueRows() {
    return numUniqueRows;
  }

  public void setNumUniqueRows(int numUniqueRows) {
    this.numUniqueRows = numUniqueRows;
  }

  public int getNumUniqueFamilies() {
    return numUniqueFamilies;
  }

  public void setNumUniqueFamilies(int numUniqueFamilies) {
    this.numUniqueFamilies = numUniqueFamilies;
  }

  public int getNumUniqueQualifiers() {
    return numUniqueQualifiers;
  }

  public void setNumUniqueQualifiers(int numUniqueQualifiers) {
    this.numUniqueQualifiers = numUniqueQualifiers;
  }

  public void setNumUniqueTags(int numUniqueTags) {
    this.numUniqueTags = numUniqueTags;
  }

  public int getNumUniqueTags() {
    return numUniqueTags;
  }
  public int getNumQualifierBytes() {
    return numQualifierBytes;
  }

  public void setNumQualifierBytes(int numQualifierBytes) {
    this.numQualifierBytes = numQualifierBytes;
  }

  public int getQualifierOffsetWidth() {
    return qualifierOffsetWidth;
  }

  public void setQualifierOffsetWidth(int qualifierOffsetWidth) {
    this.qualifierOffsetWidth = qualifierOffsetWidth;
  }

  public int getMaxQualifierLength() {
    return maxQualifierLength;
  }

  // TODO : decide on some max value for this ? INTEGER_MAX?
  public void setMaxQualifierLength(int maxQualifierLength) {
    this.maxQualifierLength = maxQualifierLength;
  }

  public int getMaxTagsLength() {
    return this.maxTagsLength;
  }

  public void setMaxTagsLength(int maxTagLength) {
    this.maxTagsLength = maxTagLength;
  }

  public int getTimestampIndexWidth() {
    return timestampIndexWidth;
  }

  public void setTimestampIndexWidth(int timestampIndexWidth) {
    this.timestampIndexWidth = timestampIndexWidth;
  }

  public int getMvccVersionIndexWidth() {
    return mvccVersionIndexWidth;
  }

  public void setMvccVersionIndexWidth(int mvccVersionIndexWidth) {
    this.mvccVersionIndexWidth = mvccVersionIndexWidth;
  }

  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }

  public boolean isIncludesMvccVersion() {
    return includesMvccVersion;
  }

  public void setIncludesMvccVersion(boolean includesMvccVersion) {
    this.includesMvccVersion = includesMvccVersion;
  }

}
