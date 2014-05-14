/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestServerLoadMap {

  @Test
  public void testOperations() {
    ServerLoadMap<Integer> m = new ServerLoadMap<Integer>();
    m.updateServerLoad("a", 2);
    assertEquals(2, m.get("a").intValue());
    m.removeServerLoad("a");
    assertNull(m.get("a"));
    m.updateServerLoad("a", 3);
    assertEquals(3, m.get("a").intValue());
    m.updateServerLoad("b", 1);
    m.updateServerLoad("c", 2);
    m.updateServerLoad("d", 2);
    m.updateServerLoad("e", 3);
    assertEquals("{1=[b], 2=[c, d]}", m.getLightServers(3).toString());
    assertEquals("{2=[c, d], 3=[a, e]}", m.getHeavyServers(2).toString());
    List<Map.Entry<String, Integer>> entries = m.entries();
    Collections.sort(entries, new Comparator<Map.Entry<String, Integer>>() {
      @Override
      public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
        return o1.getKey().compareTo(o2.getKey());
      }
    });
    assertEquals("[a=3, b=1, c=2, d=2, e=3]", entries.toString());
    assertEquals(5, m.size());
    assertTrue(m.isMostLoadedServer("a"));
    assertTrue(m.isMostLoadedServer("e"));
    assertFalse(m.isMostLoadedServer("b"));
    assertFalse(m.isMostLoadedServer("c"));
    assertFalse(m.isMostLoadedServer("d"));
    assertEquals(2, m.numServersByLoad(3));
    assertEquals(2, m.numServersByLoad(2));
    assertEquals(1, m.numServersByLoad(1));
    assertEquals(0, m.numServersByLoad(-135));
  }

  
  @Test
  public void testRandomizedPutRemove() {
    final boolean debug = false;
    ServerLoadMap<Integer> m = new ServerLoadMap<Integer>();
    Map<String, Integer> serverToLoad = new HashMap<String, Integer>();
    Map<Integer, Set<String>> loadToServers =  new HashMap<Integer, Set<String>>();
    Random rand = new Random(23984727L);
    for (int i = 0; i < 100000; ++i) {
      String serverName = "server" + rand.nextInt(100);
      if (rand.nextBoolean()) {
        int load = rand.nextInt(50);
        if (debug) {
          System.err.println("Put: " + serverName + " -> " + load);
        }
        m.updateServerLoad(serverName, load);
        Integer oldLoad = serverToLoad.remove(serverName);
        if (oldLoad != null) {
          loadToServers.get(oldLoad).remove(serverName);
          if (loadToServers.get(oldLoad).isEmpty()) {
            loadToServers.remove(oldLoad);
          }
        }
        serverToLoad.put(serverName, load);
        if (!loadToServers.containsKey(load)) {
          loadToServers.put(load, new TreeSet<String>());
        }
        loadToServers.get(load).add(serverName);
      } else {
        if (debug) {
          System.err.println("Remove: " + serverName);
        }
        m.removeServerLoad(serverName);
        Integer load = serverToLoad.remove(serverName);
        if (load != null && loadToServers.containsKey(load)) {
          loadToServers.get(load).remove(serverName);
          if (loadToServers.get(load).isEmpty()) {
            loadToServers.remove(load);
          }
        }
      }
      Map<Integer, Collection<String>> mLoadToSrv = m.getLightServers(Integer.MAX_VALUE);
      if (debug) {
        System.err.println("  serverToLoad: " + serverToLoad);
        System.err.println("  loadToServers: " + loadToServers);
        System.err.println("  m.entries: " + m.entries());
        System.err.println("  m.size: " + m.size());
        System.err.println("  mLoadToSrv: " + mLoadToSrv);
      }
      assertEquals(serverToLoad.size(), m.size());
      assertEquals(loadToServers.size(), mLoadToSrv.size());
      for (Map.Entry<Integer, Collection<String>> entry : mLoadToSrv.entrySet()) {
        List<String> mServers = new ArrayList<String>(entry.getValue());
        Collections.sort(mServers);
        assertEquals("Error at iteration #" + i + " for load " + entry.getKey(),
            loadToServers.get(entry.getKey()).toString(), mServers.toString());
      }
    } 
  }

}
