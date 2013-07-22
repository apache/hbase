/**
 * Copyright 2013 The Apache Software Foundation
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
package org.apache.hadoop.hbase.mapred;

import java.io.IOException;
import java.math.BigInteger;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class HexStringTableRecordReaderImpl extends TableRecordReaderImpl {
  final Log LOG = LogFactory.getLog(HexStringTableRecordReaderImpl.class);

  long startRowInLong = 0;
  long stopRowInLong = 0;
  int stringLength = 8;
  boolean showProgress = true;

  public HexStringTableRecordReaderImpl(int stringLength) {
    this.stringLength = stringLength;
  }

  @Override
  public void init() throws IOException {
    super.init();

    String start, end;
    if (startRow != null) {
      start = Bytes.toString(startRow);
      if (startRow.length > stringLength) {
        start = start.substring(0, stringLength);
      } else {
        start += StringUtils.repeat("0", stringLength - startRow.length);
      }
    } else {
      start = StringUtils.repeat("0", stringLength);
    }
    this.startRowInLong = new BigInteger(start, 16).longValue();

    if (endRow != null) {
      end = Bytes.toString(endRow);
      if (endRow.length > stringLength) {
        end = end.substring(0, stringLength);
      } else {
        end += StringUtils.repeat("f", stringLength - endRow.length);
      }
    } else {
      end = StringUtils.repeat("f", stringLength);
    }
    this.stopRowInLong = new BigInteger(end, 16).longValue();
    LOG.info("Start row :" + startRowInLong + ", End row :" + stopRowInLong);
  }

  @Override
  public float getProgress() {
    float progress = 0;
    try {
      if (showProgress && ((this.stopRowInLong - this.startRowInLong) != 0) &&
          this.lastRow != null) {
        long currentRowInLong =
            (new BigInteger(Bytes.toString(this.lastRow).substring(0, stringLength), 16)).longValue();
        progress = ((float)(currentRowInLong - this.startRowInLong))/
            (this.stopRowInLong - this.startRowInLong);
      }
    } catch (Exception e) {
      LOG.warn("Cannot decode current row:" + Bytes.toString(lastRow) +
          " Disabling progress reporting. Exception:" + e);
      this.showProgress = false;
    }
    return progress;
  }
}

