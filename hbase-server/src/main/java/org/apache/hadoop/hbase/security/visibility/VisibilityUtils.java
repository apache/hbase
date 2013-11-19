/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.security.visibility;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.MultiUserAuthorizations;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.UserAuthorizations;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.VisibilityLabel;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.VisibilityLabelsRequest;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ZeroCopyLiteralByteString;

/**
 * Utility method to support visibility
 */
@InterfaceAudience.Private
public class VisibilityUtils {

  public static final String VISIBILITY_LABEL_GENERATOR_CLASS = 
      "hbase.regionserver.scan.visibility.label.generator.class";
  public static final byte VISIBILITY_TAG_TYPE = (byte) 2;
  public static final String SYSTEM_LABEL = "system";

  /**
   * Creates the labels data to be written to zookeeper.
   * @param existingLabels
   * @return Bytes form of labels and their ordinal details to be written to zookeeper.
   */
  public static byte[] getDataToWriteToZooKeeper(Map<String, Integer> existingLabels) {
    VisibilityLabelsRequest.Builder visReqBuilder = VisibilityLabelsRequest.newBuilder();
    for (Entry<String, Integer> entry : existingLabels.entrySet()) {
      VisibilityLabel.Builder visLabBuilder = VisibilityLabel.newBuilder();
      visLabBuilder.setLabel(ZeroCopyLiteralByteString.wrap(Bytes.toBytes(entry.getKey())));
      visLabBuilder.setOrdinal(entry.getValue());
      visReqBuilder.addVisLabel(visLabBuilder.build());
    }
    return ProtobufUtil.prependPBMagic(visReqBuilder.build().toByteArray());
  }

  /**
   * Creates the user auth data to be written to zookeeper.
   * @param userAuths
   * @return Bytes form of user auths details to be written to zookeeper.
   */
  public static byte[] getUserAuthsDataToWriteToZooKeeper(Map<String, List<Integer>> userAuths) {
    MultiUserAuthorizations.Builder builder = MultiUserAuthorizations.newBuilder();
    for (Entry<String, List<Integer>> entry : userAuths.entrySet()) {
      UserAuthorizations.Builder userAuthsBuilder = UserAuthorizations.newBuilder();
      userAuthsBuilder.setUser(ZeroCopyLiteralByteString.wrap(Bytes.toBytes(entry.getKey())));
      for (Integer label : entry.getValue()) {
        userAuthsBuilder.addAuth(label);
      }
      builder.addUserAuths(userAuthsBuilder.build());
    }
    return ProtobufUtil.prependPBMagic(builder.build().toByteArray());
  }

  /**
   * Reads back from the zookeeper. The data read here is of the form written by
   * writeToZooKeeper(Map<byte[], Integer> entries).
   * 
   * @param data
   * @return Labels and their ordinal details
   * @throws DeserializationException
   */
  public static List<VisibilityLabel> readLabelsFromZKData(byte[] data)
      throws DeserializationException {
    if (ProtobufUtil.isPBMagicPrefix(data)) {
      int pblen = ProtobufUtil.lengthOfPBMagic();
      try {
        VisibilityLabelsRequest request = VisibilityLabelsRequest.newBuilder()
            .mergeFrom(data, pblen, data.length - pblen).build();
        return request.getVisLabelList();
      } catch (InvalidProtocolBufferException e) {
        throw new DeserializationException(e);
      }
    }
    return null;
  }

  /**
   * Reads back User auth data written to zookeeper.
   * @param data
   * @return User auth details
   * @throws DeserializationException
   */
  public static MultiUserAuthorizations readUserAuthsFromZKData(byte[] data) 
      throws DeserializationException {
    if (ProtobufUtil.isPBMagicPrefix(data)) {
      int pblen = ProtobufUtil.lengthOfPBMagic();
      try {
        MultiUserAuthorizations multiUserAuths = MultiUserAuthorizations.newBuilder()
            .mergeFrom(data, pblen, data.length - pblen).build();
        return multiUserAuths;
      } catch (InvalidProtocolBufferException e) {
        throw new DeserializationException(e);
      }
    }
    return null;
  }

  public static ScanLabelGenerator getScanLabelGenerator(Configuration conf) {
    Class<? extends ScanLabelGenerator> scanLabelGeneratorKlass = conf
        .getClass(VISIBILITY_LABEL_GENERATOR_CLASS, DefaultScanLabelGenerator.class,
            ScanLabelGenerator.class);
    return ReflectionUtils.newInstance(scanLabelGeneratorKlass, conf);
  }
}
