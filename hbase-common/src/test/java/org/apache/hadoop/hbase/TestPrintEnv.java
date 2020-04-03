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
package org.apache.hadoop.hbase;

import com.sun.management.UnixOperatingSystemMXBean;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestPrintEnv {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestPrintEnv.class);
  private static Object S = new Object();
  private static int COUNT = 0;

  @Test
  public void testupu() throws Exception {
    Process p = Runtime.getRuntime().exec(new String [] {"/bin/bash", "-c", "ulimit -u"});
    int errCode = p.waitFor();
    System.out.println("errCode=" + errCode);
    BufferedReader is = new BufferedReader(new InputStreamReader(p.getInputStream()));
    String line;
    while ((line = is.readLine()) != null) {
      System.out.println(line);
    }
    is.close();
    is = new BufferedReader(new InputStreamReader(p.getErrorStream()));
    while ((line = is.readLine()) != null) {
      System.out.println(line);
    }
    p = Runtime.getRuntime().exec(new String [] {"/bin/bash", "-c", "ulimit -u 10001"});
    errCode = p.waitFor();
    System.out.println("errCode=" + errCode);
    is = new BufferedReader(new InputStreamReader(p.getInputStream()));
    while ((line = is.readLine()) != null) {
      System.out.println(line);
    }
    is.close();
    is = new BufferedReader(new InputStreamReader(p.getErrorStream()));
    while ((line = is.readLine()) != null) {
      System.out.println(line);
    }
    p = Runtime.getRuntime().exec(new String [] {"/bin/bash", "-c", "ulimit -u"});
    errCode = p.waitFor();
    System.out.println("errCode=" + errCode);
    is = new BufferedReader(new InputStreamReader(p.getInputStream()));
    while ((line = is.readLine()) != null) {
      System.out.println(line);
    }
    is.close();
    is = new BufferedReader(new InputStreamReader(p.getErrorStream()));
    while ((line = is.readLine()) != null) {
      System.out.println(line);
    }
  }

  @Test
  public void testmvnversion() throws Exception {
    Process p = Runtime.getRuntime().exec(new String [] {"/bin/bash", "-c", "mvn --version"});
    int errCode = p.waitFor();
    System.out.println("errCode=" + errCode);
    BufferedReader is = new BufferedReader(new InputStreamReader(p.getInputStream()));
    String line;
    while ((line = is.readLine()) != null) {
      System.out.println(line);
    }
    is.close();
    is = new BufferedReader(new InputStreamReader(p.getErrorStream()));
    while ((line = is.readLine()) != null) {
      System.out.println(line);
    }
  }

  @Test
  public void testn() throws Exception {
    Process p = Runtime.getRuntime().exec(new String [] {"/bin/bash", "-c", "ulimit -n"});
    int errCode = p.waitFor();
    System.out.println("errCode=" + errCode);
    BufferedReader is = new BufferedReader(new InputStreamReader(p.getInputStream()));
    String line;
    while ((line = is.readLine()) != null) {
      System.out.println(line);
    }
    is.close();
    is = new BufferedReader(new InputStreamReader(p.getErrorStream()));
    while ((line = is.readLine()) != null) {
      System.out.println(line);
    }
  }

  @Test
  public void testa() throws Exception {
    Process p = Runtime.getRuntime().exec(new String [] {"/bin/bash", "-c", "ulimit -a"});
    int errCode = p.waitFor();
    System.out.println("errCode=" + errCode);
    BufferedReader is = new BufferedReader(new InputStreamReader(p.getInputStream()));
    String line;
    while ((line = is.readLine()) != null) {
      System.out.println(line);
    }
    is.close();
    is = new BufferedReader(new InputStreamReader(p.getErrorStream()));
    while ((line = is.readLine()) != null) {
      System.out.println(line);
    }
  }

  @Test
  public void testfds() throws InterruptedException, IOException {
    final UnixOperatingSystemMXBean osMBean =
      (UnixOperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    System.out.println("opendfs: " + osMBean.getOpenFileDescriptorCount());
    System.out.println("maxfds:  " + osMBean.getMaxFileDescriptorCount());
  }

/*  
  @org.junit.Ignore @Test
  public void testthreds() throws InterruptedException, IOException {
    try {
      Runtime rt = Runtime.getRuntime();
      while (true) {
        new Thread(new Runnable() {
          public void run() {
            synchronized (S) {
              COUNT += 1;
              System.err.println("New thread #" + COUNT);
            }
            for (; ; ) {
              try {
                Thread.sleep(10);
              } catch (Exception e) {
                System.err.println(e);
              }
            }
          }
        }).start();
        Thread.sleep(10);
      }
    } catch (Throwable t) {
    }
  }
  */
}
