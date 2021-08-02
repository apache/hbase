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
package org.apache.hadoop.hbase.master.http;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.assignment.RegionStateNode;
import org.apache.hadoop.hbase.master.assignment.TransitRegionStateProcedure;
import org.apache.hadoop.hbase.util.GsonUtil;
import org.apache.hbase.thirdparty.com.google.gson.Gson;
import org.apache.hbase.thirdparty.org.eclipse.jetty.servlet.DefaultServlet;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class RitServlet extends DefaultServlet {
  private static final long serialVersionUID = 1L;
  private static final Gson GSON = GsonUtil.createGson().create();

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
    throws IOException {
    HMaster master = (HMaster) getServletContext().getAttribute(HMaster.MASTER);
    assert master != null : "No Master in context!";

    response.setContentType("application/json");
    OutputStream os = response.getOutputStream();

    try (PrintWriter out = new PrintWriter(os)) {

      AssignmentManager am = master.getAssignmentManager();
      if (am == null) {
        out.println("AssignmentManager is not initialized");
        return;
      }

      Map<String, List<Map<String, Object>>> map = new HashMap<>();
      List<Map<String, Object>> rits = new ArrayList<>();
      map.put("rits", rits);
      for (RegionStateNode regionStateNode : am.getRegionsInTransition()) {
        Map<String, Object> rit = new HashMap<>();
        rit.put("region", regionStateNode.getRegionInfo().getEncodedName());
        rit.put("table", regionStateNode.getRegionInfo().getTable().getNameAsString());
        rit.put("state", regionStateNode.getState());
        rit.put("server", regionStateNode.getRegionLocation().getServerName());

        TransitRegionStateProcedure procedure = regionStateNode.getProcedure();
        if (procedure != null) {
          rit.put("procedureId", procedure.getProcId());
          rit.put("procedureState", procedure.getState().toString());
        }

        RegionState rs = regionStateNode.toRegionState();
        rit.put("startTime", rs.getStamp());
        rit.put("duration", System.currentTimeMillis() - rs.getStamp());

        rits.add(rit);
      }

      out.write(GSON.toJson(map));
      out.flush();
    }
  }
}
