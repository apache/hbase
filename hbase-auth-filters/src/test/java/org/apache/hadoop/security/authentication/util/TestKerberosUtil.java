/**
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
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.security.authentication.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

import org.apache.kerby.kerberos.kerb.keytab.Keytab;
import org.apache.kerby.kerberos.kerb.keytab.KeytabEntry;
import org.apache.kerby.kerberos.kerb.type.KerberosTime;
import org.apache.kerby.kerberos.kerb.type.base.EncryptionKey;
import org.apache.kerby.kerberos.kerb.type.base.EncryptionType;
import org.apache.kerby.kerberos.kerb.type.base.PrincipalName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class TestKerberosUtil {
  static String testKeytab = "test.keytab";
  static String[] testPrincipals = new String[]{
      "HTTP@testRealm",
      "test/testhost@testRealm",
      "HTTP/testhost@testRealm",
      "HTTP1/testhost@testRealm",
      "HTTP/testhostanother@testRealm"
  };

  @AfterEach
  public void deleteKeytab() {
    File keytabFile = new File(testKeytab);
    if (keytabFile.exists()){
      keytabFile.delete();
    }
  }

  @Test
  public void testGetServerPrincipal()
      throws IOException, UnknownHostException {
    String service = "TestKerberosUtil";
    String localHostname = KerberosUtil.getLocalHostName();
    String testHost = "FooBar";
    String defaultRealm = KerberosUtil.getDefaultRealmProtected();

    String atDefaultRealm;
    if (defaultRealm == null || defaultRealm.equals("")) {
      atDefaultRealm = "";
    } else {
      atDefaultRealm = "@" + defaultRealm;
    }
    // check that the test environment is as expected
    assertEquals(KerberosUtil.getDomainRealm(service + "/" + localHostname.toLowerCase(Locale.US)),
        defaultRealm, "testGetServerPrincipal assumes localhost realm is default");
    assertEquals(KerberosUtil.getDomainRealm(service + "/" + testHost.toLowerCase(Locale.US)),
        defaultRealm, "testGetServerPrincipal assumes realm of testHost 'FooBar' is default");

    // send null hostname
    assertEquals(service + "/" + localHostname.toLowerCase(Locale.US) + atDefaultRealm,
        KerberosUtil.getServicePrincipal(service, null),
        "When no hostname is sent");
    // send empty hostname
    assertEquals(service + "/" + localHostname.toLowerCase(Locale.US) + atDefaultRealm,
        KerberosUtil.getServicePrincipal(service, ""),
        "When empty hostname is sent");
    // send 0.0.0.0 hostname
    assertEquals(service + "/" + localHostname.toLowerCase(Locale.US) + atDefaultRealm,
        KerberosUtil.getServicePrincipal(service, "0.0.0.0"),
        "When 0.0.0.0 hostname is sent");
    // send uppercase hostname
    assertEquals(service + "/" + testHost.toLowerCase(Locale.US) + atDefaultRealm,
        KerberosUtil.getServicePrincipal(service, testHost),
        "When uppercase hostname is sent");
    // send lowercase hostname
    assertEquals(service + "/" + testHost.toLowerCase(Locale.US) + atDefaultRealm,
        KerberosUtil.getServicePrincipal(service, testHost.toLowerCase(Locale.US)),
        "When lowercase hostname is sent");
  }

  @Test
  public void testGetPrincipalNamesMissingKeytab() {
    try {
      KerberosUtil.getPrincipalNames(testKeytab);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      //expects exception
    } catch (IOException e) {
    }
  }

  @Test
  public void testGetPrincipalNamesMissingPattern() throws IOException {
    createKeyTab(testKeytab, new String[]{"test/testhost@testRealm"});
    try {
      KerberosUtil.getPrincipalNames(testKeytab, null);
      fail("Exception should have been thrown");
    } catch (Exception e) {
      //expects exception
    }
  }

  @Test
  public void testGetPrincipalNamesFromKeytab() throws IOException {
    createKeyTab(testKeytab, testPrincipals); 
    // read all principals in the keytab file
    String[] principals = KerberosUtil.getPrincipalNames(testKeytab);
    assertNotNull(principals, "principals cannot be null");
    
    int expectedSize = 0;
    List<String> principalList = Arrays.asList(principals);
    for (String principal : testPrincipals) {
      assertTrue(principalList.contains(principal), "missing principal "+principal);
      expectedSize++;
    }
    assertEquals(expectedSize, principals.length);
  }
  
  @Test
  public void testGetPrincipalNamesFromKeytabWithPattern() throws IOException {
    createKeyTab(testKeytab, testPrincipals); 
    // read the keytab file
    // look for principals with HTTP as the first part
    Pattern httpPattern = Pattern.compile("HTTP/.*");
    String[] httpPrincipals =
        KerberosUtil.getPrincipalNames(testKeytab, httpPattern);
    assertNotNull(httpPrincipals, "principals cannot be null");
    
    int expectedSize = 0;
    List<String> httpPrincipalList = Arrays.asList(httpPrincipals);
    for (String principal : testPrincipals) {
      if (httpPattern.matcher(principal).matches()) {
        assertTrue(httpPrincipalList.contains(principal),
            "missing principal "+principal);
        expectedSize++;
      }
    }
    assertEquals(expectedSize, httpPrincipals.length);
  }
  
  private void createKeyTab(String fileName, String[] principalNames)
      throws IOException {
    //create a test keytab file
    List<KeytabEntry> lstEntries = new ArrayList<KeytabEntry>();
    for (String principal : principalNames){
      // create 3 versions of the key to ensure methods don't return
      // duplicate principals
      for (int kvno=1; kvno <= 3; kvno++) {
        EncryptionKey key = new EncryptionKey(
            EncryptionType.NONE, "samplekey1".getBytes(), kvno);
        KeytabEntry keytabEntry = new KeytabEntry(
            new PrincipalName(principal), new KerberosTime(), (byte) 1, key);
        lstEntries.add(keytabEntry);      
      }
    }
    Keytab keytab = new Keytab();
    keytab.addKeytabEntries(lstEntries);
    keytab.store(new File(testKeytab));
  }

  @Test
  public void testServicePrincipalDecode() throws Exception {
    // test decoding standard krb5 tokens and spnego wrapped tokens
    // for principals with the default realm, and a non-default realm.
    String krb5Default =
        "YIIB2AYJKoZIhvcSAQICAQBuggHHMIIBw6ADAgEFoQMCAQ6iBwMFACAAAACj" +
        "gethgegwgeWgAwIBBaENGwtFWEFNUExFLkNPTaIcMBqgAwIBAKETMBEbBEhU" +
        "VFAbCWxvY2FsaG9zdKOBsDCBraADAgERoQMCAQGigaAEgZ23QsT1+16T23ni" +
        "JI1uFRU0FN13hhPSLAl4+oAqpV5s1Z6E+G2VKGx2+rUF21utOdlwUK/J5CKF" +
        "HxM4zfNsmzRFhdk5moJW6AWHuRqGJ9hrZgTxA2vOBIn/tju+n/vJVEcUvW0f" +
        "DiPfjPIPFOlc7V9GlWvZFyr5NMJSFwspKJXYh/FSNpSVTecfGskjded9TZzR" +
        "2tOVzgpjFvAu/DETpIG/MIG8oAMCARGigbQEgbGWnbKlV1oo7/gzT4hi/Q41" +
        "ff2luDnSxADEmo6M8LC42scsYMLNgU4iLJhuf4YLb7ueh790HrbB6Kdes71/" +
        "gSBiLI2/mn3BqNE43gt94dQ8VFBix4nJCsYnuORYxLJjRSJE+3ImJNsSjqaf" +
        "GRI0sp9w3hc4IVm8afb3Ggm6PgRIyyGNdTzK/p03v+zA01MJh3htuOgLKUOV" +
        "z002pHnGzu/purZ5mOyaQT12vHxJ2T+Cwi8=";

    String krb5Other =
        "YIIB2AYJKoZIhvcSAQICAQBuggHHMIIBw6ADAgEFoQMCAQ6iBwMFACAAAACj" +
        "gethgegwgeWgAwIBBaENGwtBQkNERUZHLk9SR6IcMBqgAwIBAKETMBEbBEhU" +
        "VFAbCW90aGVyaG9zdKOBsDCBraADAgERoQMCAQGigaAEgZ23QsT1+16T23ni" +
        "JI1uFRU0FN13hhPSLAl4+oAqpV5s1Z6E+G2VKGx2+rUF21utOdlwUK/J5CKF" +
        "HxM4zfNsmzRFhdk5moJW6AWHuRqGJ9hrZgTxA2vOBIn/tju+n/vJVEcUvW0f" +
        "DiPfjPIPFOlc7V9GlWvZFyr5NMJSFwspKJXYh/FSNpSVTecfGskjded9TZzR" +
        "2tOVzgpjFvAu/DETpIG/MIG8oAMCARGigbQEgbGWnbKlV1oo7/gzT4hi/Q41" +
        "ff2luDnSxADEmo6M8LC42scsYMLNgU4iLJhuf4YLb7ueh790HrbB6Kdes71/" +
        "gSBiLI2/mn3BqNE43gt94dQ8VFBix4nJCsYnuORYxLJjRSJE+3ImJNsSjqaf" +
        "GRI0sp9w3hc4IVm8afb3Ggm6PgRIyyGNdTzK/p03v+zA01MJh3htuOgLKUOV" +
        "z002pHnGzu/purZ5mOyaQT12vHxJ2T+Cwi8K";

    String spnegoDefault =
        "YIICCQYGKwYBBQUCoIIB/TCCAfmgDTALBgkqhkiG9xIBAgKhBAMCAXaiggHg" +
        "BIIB3GCCAdgGCSqGSIb3EgECAgEAboIBxzCCAcOgAwIBBaEDAgEOogcDBQAg" +
        "AAAAo4HrYYHoMIHloAMCAQWhDRsLRVhBTVBMRS5DT02iHDAaoAMCAQChEzAR" +
        "GwRIVFRQGwlsb2NhbGhvc3SjgbAwga2gAwIBEaEDAgEBooGgBIGdBWbzvV1R" +
        "Iqb7WuPIW3RTkFtwjU9P/oFAbujGPd8h/qkCszroNdvHhUkPntuOqhFBntMo" +
        "bilgTqNEdDUGvBbfkJaRklNGqT/IAOUV6tlGpBUCXquR5UdPzPpUvGZiVRUu" +
        "FGH5DGGHvYF1CwXPp2l1Jq373vSLQ1kBl6TXl+aKLsZYhVUjKvE7Auippclb" +
        "hv/GGGex/TcjNH48k47OQaSBvzCBvKADAgERooG0BIGxeChp3TMVtWbCdFGo" +
        "YL+35r2762j+OEwZRfcj4xCK7j0mUTcxLtyVGxyY9Ax+ljl5gTwzRhXcJq0T" +
        "TjiQwKJckeZ837mXQAURbfJpFc3VLAXGfNkMFCR7ZkWpGA1Vzc3PeUNczn2D" +
        "Lpu8sme55HFFQDi/0akW6Lwv/iCrpwIkZPyZPjaEmwLVALu4E8m0Ka3fJkPV" +
        "GAhamg9OQpuREIK0pCk3ZSHhJz8qMwduzRZHc4vN";

    String spnegoOther =
        "YIICCQYGKwYBBQUCoIIB/TCCAfmgDTALBgkqhkiG9xIBAgKhBAMCAXaiggHg" +
        "BIIB3GCCAdgGCSqGSIb3EgECAgEAboIBxzCCAcOgAwIBBaEDAgEOogcDBQAg" +
        "AAAAo4HrYYHoMIHloAMCAQWhDRsLQUJDREVGRy5PUkeiHDAaoAMCAQChEzAR" +
        "GwRIVFRQGwlvdGhlcmhvc3SjgbAwga2gAwIBEaEDAgEBooGgBIGdBWbzvV1R" +
        "Iqb7WuPIW3RTkFtwjU9P/oFAbujGPd8h/qkCszroNdvHhUkPntuOqhFBntMo" +
        "bilgTqNEdDUGvBbfkJaRklNGqT/IAOUV6tlGpBUCXquR5UdPzPpUvGZiVRUu" +
        "FGH5DGGHvYF1CwXPp2l1Jq373vSLQ1kBl6TXl+aKLsZYhVUjKvE7Auippclb" +
        "hv/GGGex/TcjNH48k47OQaSBvzCBvKADAgERooG0BIGxeChp3TMVtWbCdFGo" +
        "YL+35r2762j+OEwZRfcj4xCK7j0mUTcxLtyVGxyY9Ax+ljl5gTwzRhXcJq0T" +
        "TjiQwKJckeZ837mXQAURbfJpFc3VLAXGfNkMFCR7ZkWpGA1Vzc3PeUNczn2D" +
        "Lpu8sme55HFFQDi/0akW6Lwv/iCrpwIkZPyZPjaEmwLVALu4E8m0Ka3fJkPV" +
        "GAhamg9OQpuREIK0pCk3ZSHhJz8qMwduzRZHc4vNCg==";


    assertEquals("HTTP/localhost@EXAMPLE.COM", getPrincipal(krb5Default));
    assertEquals("HTTP/otherhost@ABCDEFG.ORG", getPrincipal(krb5Other));
    assertEquals("HTTP/localhost@EXAMPLE.COM", getPrincipal(spnegoDefault));
    assertEquals("HTTP/otherhost@ABCDEFG.ORG", getPrincipal(spnegoOther));
  }

  private static String getPrincipal(String token) {
    return KerberosUtil.getTokenServerName(
        Base64.getDecoder().decode(token));
  }
}