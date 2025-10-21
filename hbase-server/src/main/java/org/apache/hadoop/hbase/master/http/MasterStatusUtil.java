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
package org.apache.hadoop.hbase.master.http;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.assignment.RegionStateNode;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Utility used by the web UI JSP pages.
 */
@InterfaceAudience.Private
public final class MasterStatusUtil {

  private MasterStatusUtil() {
    // Do not instantiate.
  }

  public static String getUserTables(HMaster master, List<TableDescriptor> tables) {
    if (master.isInitialized()) {
      try {
        Map<String, TableDescriptor> descriptorMap = master.getTableDescriptors().getAll();
        if (descriptorMap != null) {
          for (TableDescriptor desc : descriptorMap.values()) {
            if (!desc.getTableName().isSystemTable()) {
              tables.add(desc);
            }
          }
        }
      } catch (IOException e) {
        return "Got user tables error, " + e.getMessage();
      }
    }
    return null;
  }

  public static Map<String, Integer> getFragmentationInfo(HMaster master, Configuration conf)
    throws IOException {
    boolean showFragmentation = conf.getBoolean("hbase.master.ui.fragmentation.enabled", false);
    if (showFragmentation) {
      return FSUtils.getTableFragmentation(master);
    } else {
      return null;
    }
  }

  public static ServerName getMetaLocationOrNull(HMaster master) {
    RegionStateNode rsn = master.getAssignmentManager().getRegionStates()
      .getRegionStateNode(RegionInfoBuilder.FIRST_META_REGIONINFO);
    if (rsn != null) {
      return rsn.isInState(RegionState.State.OPEN) ? rsn.getRegionLocation() : null;
    }
    return null;
  }

  public static String serverNameLink(HMaster master, ServerName serverName) {
    int infoPort = master.getRegionServerInfoPort(serverName);
    String url = "//" + serverName.getHostname() + ":" + infoPort + "/regionserver.jsp";
    if (infoPort > 0) {
      return "<a href=\"" + url + "\">" + serverName.getServerName() + "</a>";
    } else {
      return serverName.getServerName();
    }
  }
}
