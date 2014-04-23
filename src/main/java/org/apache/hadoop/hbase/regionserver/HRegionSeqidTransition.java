/**
 * Copyright 2014 The Apache Software Foundation
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
package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * HRegionSeqidTransition records the last and next hlog sequence id of region
 * during region opening and moving
 */
public final class HRegionSeqidTransition {
  private static final Log LOG = LogFactory.getLog(HRegionSeqidTransition.class);
  private static final long VERSION = 1;
  private final long lastSeqid;
  private final long nextSeqid;

  public HRegionSeqidTransition(final long lastSeqid, final long nextSeqid) {
    if (lastSeqid < 0 || nextSeqid < 0) {
      throw new IllegalArgumentException("Seqids cannot be negative!");
    }

    this.lastSeqid = lastSeqid;
    this.nextSeqid = nextSeqid;
  }

  public long getLastSeqid() {
    return lastSeqid;
  }

  public long getNextSeqid() {
    return nextSeqid;
  }

  /**
   *
   * @param fromByte a byte array containing version, the last and next seqids
   * @throws java.lang.IllegalArgumentException
   */
  public static HRegionSeqidTransition fromBytes(byte[] fromByte) throws IllegalArgumentException {
    if (fromByte == null) {
      return null;
    }

    if (fromByte.length < 3 * Bytes.SIZEOF_LONG) {
      throw new IllegalArgumentException("Bytes array too short!");
    }

    if (Bytes.toLong(fromByte, 0) != VERSION) {
      // can be changed to deal versions later
      throw new IllegalArgumentException("Only version 1 exists!!");
    }

    return new HRegionSeqidTransition(
        Bytes.toLong(fromByte, Bytes.SIZEOF_LONG),
        Bytes.toLong(fromByte, 2 * Bytes.SIZEOF_LONG));
  }

  /**
   * @return bytes containing version and both seqids, null if not valid
   */
  public static byte[] toBytes(final HRegionSeqidTransition instance) {
    if (instance == null) {
      return null;
    }
    byte[] outBytes = new byte[3 * Bytes.SIZEOF_LONG];
    int pos = Bytes.putLong(outBytes, 0, VERSION);
    pos = Bytes.putLong(outBytes, pos, instance.lastSeqid);
    Bytes.putLong(outBytes, pos, instance.nextSeqid);
    return outBytes;
  }

  public String toString() {
    return "Region sequence id transition: " + lastSeqid + "->" + nextSeqid +".";
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    HRegionSeqidTransition other = (HRegionSeqidTransition) obj;

    return (VERSION == other.VERSION) && (lastSeqid == other.lastSeqid) &&
      (nextSeqid == other.nextSeqid);
  }
}
