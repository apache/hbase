/**
 * Copyright The Apache Software Foundation
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

package org.apache.hadoop.hbase.rsgroup;

import com.google.common.collect.Lists;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupProtos;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

//TODO do better encapsulation of SerDe logic from GroupInfoManager and GroupTracker
@InterfaceAudience.Private
public class RSGroupSerDe {
  private static final Log LOG = LogFactory.getLog(RSGroupSerDe.class);

  public RSGroupSerDe() {

  }

  public List<RSGroupInfo> retrieveGroupList(Table groupTable) throws IOException {
    List<RSGroupInfo> RSGroupInfoList = Lists.newArrayList();
    for (Result result : groupTable.getScanner(new Scan())) {
      RSGroupProtos.RSGroupInfo proto =
          RSGroupProtos.RSGroupInfo.parseFrom(
              result.getValue(
                  RSGroupInfoManager.META_FAMILY_BYTES,
                  RSGroupInfoManager.META_QUALIFIER_BYTES));
      RSGroupInfoList.add(ProtobufUtil.toGroupInfo(proto));
    }
    return RSGroupInfoList;
  }

  public List<RSGroupInfo> retrieveGroupList(ZooKeeperWatcher watcher,
                                             String groupBasePath) throws IOException {
    List<RSGroupInfo> RSGroupInfoList = Lists.newArrayList();
    //Overwrite any info stored by table, this takes precedence
    try {
      if(ZKUtil.checkExists(watcher, groupBasePath) != -1) {
        for(String znode: ZKUtil.listChildrenAndWatchForNewChildren(watcher, groupBasePath)) {
          byte[] data = ZKUtil.getData(watcher, ZKUtil.joinZNode(groupBasePath, znode));
          if(data.length > 0) {
            ProtobufUtil.expectPBMagicPrefix(data);
            ByteArrayInputStream bis = new ByteArrayInputStream(
                data, ProtobufUtil.lengthOfPBMagic(), data.length);
            RSGroupInfoList.add(ProtobufUtil.toGroupInfo(RSGroupProtos.RSGroupInfo.parseFrom(bis)));
          }
        }
        LOG.debug("Read ZK GroupInfo count:" + RSGroupInfoList.size());
      }
    } catch (KeeperException e) {
      throw new IOException("Failed to read rsGroupZNode",e);
    } catch (DeserializationException e) {
      throw new IOException("Failed to read rsGroupZNode",e);
    } catch (InterruptedException e) {
      throw new IOException("Failed to read rsGroupZNode",e);
    }
    return RSGroupInfoList;
  }
}
