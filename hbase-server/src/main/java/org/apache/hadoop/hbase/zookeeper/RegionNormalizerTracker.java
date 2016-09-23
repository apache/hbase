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

package org.apache.hadoop.hbase.zookeeper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionNormalizerProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;

/**
 * Tracks region normalizer state up in ZK
 */
public class RegionNormalizerTracker extends ZooKeeperNodeTracker {
  private static final Log LOG = LogFactory.getLog(RegionNormalizerTracker.class);

  public RegionNormalizerTracker(ZooKeeperWatcher watcher,
                             Abortable abortable) {
    super(watcher, watcher.znodePaths.regionNormalizerZNode, abortable);
  }

  /**
   * Return true if region normalizer is on, false otherwise
   */
  public boolean isNormalizerOn() {
    byte [] upData = super.getData(false);
    try {
      // if data in ZK is null, use default of on.
      return upData == null || parseFrom(upData).getNormalizerOn();
    } catch (DeserializationException dex) {
      LOG.error("ZK state for RegionNormalizer could not be parsed "
        + Bytes.toStringBinary(upData));
      // return false to be safe.
      return false;
    }
  }

  /**
   * Set region normalizer on/off
   * @param normalizerOn whether normalizer should be on or off
   * @throws KeeperException
   */
  public void setNormalizerOn(boolean normalizerOn) throws KeeperException {
    byte [] upData = toByteArray(normalizerOn);
    try {
      ZKUtil.setData(watcher, watcher.znodePaths.regionNormalizerZNode, upData);
    } catch(KeeperException.NoNodeException nne) {
      ZKUtil.createAndWatch(watcher, watcher.znodePaths.regionNormalizerZNode, upData);
    }
    super.nodeDataChanged(watcher.znodePaths.regionNormalizerZNode);
  }

  private byte [] toByteArray(boolean isNormalizerOn) {
    RegionNormalizerProtos.RegionNormalizerState.Builder builder =
      RegionNormalizerProtos.RegionNormalizerState.newBuilder();
    builder.setNormalizerOn(isNormalizerOn);
    return ProtobufUtil.prependPBMagic(builder.build().toByteArray());
  }

  private RegionNormalizerProtos.RegionNormalizerState parseFrom(byte [] pbBytes)
    throws DeserializationException {
    ProtobufUtil.expectPBMagicPrefix(pbBytes);
    RegionNormalizerProtos.RegionNormalizerState.Builder builder =
      RegionNormalizerProtos.RegionNormalizerState.newBuilder();
    try {
      int magicLen = ProtobufUtil.lengthOfPBMagic();
      ProtobufUtil.mergeFrom(builder, pbBytes, magicLen, pbBytes.length - magicLen);
    } catch (IOException e) {
      throw new DeserializationException(e);
    }
    return builder.build();
  }
}
