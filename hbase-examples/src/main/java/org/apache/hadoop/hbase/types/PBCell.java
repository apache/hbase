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
package org.apache.hadoop.hbase.types;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import java.io.IOException;
import org.apache.hadoop.hbase.protobuf.generated.CellProtos;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * An example for using protobuf objects with {@link DataType} API.
 */
@InterfaceAudience.Private
public class PBCell extends PBType<CellProtos.Cell> {
  @Override
  public Class<CellProtos.Cell> encodedClass() {
    return CellProtos.Cell.class;
  }

  @Override
  public int skip(PositionedByteRange src) {
    CellProtos.Cell.Builder builder = CellProtos.Cell.newBuilder();
    CodedInputStream is = inputStreamFromByteRange(src);
    is.setSizeLimit(src.getLength());
    try {
      builder.mergeFrom(is);
      int consumed = is.getTotalBytesRead();
      src.setPosition(src.getPosition() + consumed);
      return consumed;
    } catch (IOException e) {
      throw new RuntimeException("Error while skipping type.", e);
    }
  }

  @Override
  public CellProtos.Cell decode(PositionedByteRange src) {
    CellProtos.Cell.Builder builder = CellProtos.Cell.newBuilder();
    CodedInputStream is = inputStreamFromByteRange(src);
    is.setSizeLimit(src.getLength());
    try {
      CellProtos.Cell ret = builder.mergeFrom(is).build();
      src.setPosition(src.getPosition() + is.getTotalBytesRead());
      return ret;
    } catch (IOException e) {
      throw new RuntimeException("Error while decoding type.", e);
    }
  }

  @Override
  public int encode(PositionedByteRange dst, CellProtos.Cell val) {
    CodedOutputStream os = outputStreamFromByteRange(dst);
    try {
      int before = os.spaceLeft(), after, written;
      val.writeTo(os);
      after = os.spaceLeft();
      written = before - after;
      dst.setPosition(dst.getPosition() + written);
      return written;
    } catch (IOException e) {
      throw new RuntimeException("Error while encoding type.", e);
    }
  }
}
