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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.ImmutableMap;

@Category(SmallTests.class)
public class TestHBaseConfiguration {

  private static final Log LOG = LogFactory.getLog(TestHBaseConfiguration.class);

  private static HBaseCommonTestingUtility UTIL = new HBaseCommonTestingUtility();

  @AfterClass
  public static void tearDown() throws IOException {
    UTIL.cleanupTestDir();
  }

  @Test
  public void testGetIntDeprecated() {
    int VAL = 1, VAL2 = 2;
    String NAME = "foo";
    String DEPRECATED_NAME = "foo.deprecated";

    Configuration conf = HBaseConfiguration.create();
    conf.setInt(NAME, VAL);
    assertEquals(VAL, HBaseConfiguration.getInt(conf, NAME, DEPRECATED_NAME, 0));

    conf = HBaseConfiguration.create();
    conf.setInt(DEPRECATED_NAME, VAL);
    assertEquals(VAL, HBaseConfiguration.getInt(conf, NAME, DEPRECATED_NAME, 0));

    conf = HBaseConfiguration.create();
    conf.setInt(DEPRECATED_NAME, VAL);
    conf.setInt(NAME, VAL);
    assertEquals(VAL, HBaseConfiguration.getInt(conf, NAME, DEPRECATED_NAME, 0));

    conf = HBaseConfiguration.create();
    conf.setInt(DEPRECATED_NAME, VAL);
    conf.setInt(NAME, VAL2); // deprecated value will override this
    assertEquals(VAL, HBaseConfiguration.getInt(conf, NAME, DEPRECATED_NAME, 0));
  }

  @Test
  public void testSubset() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    // subset is used in TableMapReduceUtil#initCredentials to support different security
    // configurations between source and destination clusters, so we'll use that as an example
    String prefix = "hbase.mapred.output.";
    conf.set("hbase.security.authentication", "kerberos");
    conf.set("hbase.regionserver.kerberos.principal", "hbasesource");
    HBaseConfiguration.setWithPrefix(conf, prefix,
        ImmutableMap.of(
            "hbase.regionserver.kerberos.principal", "hbasedest",
            "", "shouldbemissing")
            .entrySet());

    Configuration subsetConf = HBaseConfiguration.subset(conf, prefix);
    assertNull(subsetConf.get(prefix + "hbase.regionserver.kerberos.principal"));
    assertEquals("hbasedest", subsetConf.get("hbase.regionserver.kerberos.principal"));
    assertNull(subsetConf.get("hbase.security.authentication"));
    assertNull(subsetConf.get(""));

    Configuration mergedConf = HBaseConfiguration.create(conf);
    HBaseConfiguration.merge(mergedConf, subsetConf);

    assertEquals("hbasedest", mergedConf.get("hbase.regionserver.kerberos.principal"));
    assertEquals("kerberos", mergedConf.get("hbase.security.authentication"));
    assertEquals("shouldbemissing", mergedConf.get(prefix));
  }

  @Test
  public void testGetPassword() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.set(ReflectiveCredentialProviderClient.CREDENTIAL_PROVIDER_PATH, "jceks://file"
        + new File(UTIL.getDataTestDir().toUri().getPath(), "foo.jks").getCanonicalPath());
    ReflectiveCredentialProviderClient client = new ReflectiveCredentialProviderClient();
    if (client.isHadoopCredentialProviderAvailable()) {
      char[] keyPass = { 'k', 'e', 'y', 'p', 'a', 's', 's' };
      char[] storePass = { 's', 't', 'o', 'r', 'e', 'p', 'a', 's', 's' };
      client.createEntry(conf, "ssl.keypass.alias", keyPass);
      client.createEntry(conf, "ssl.storepass.alias", storePass);

      String keypass = HBaseConfiguration.getPassword(conf, "ssl.keypass.alias", null);
      assertEquals(keypass, new String(keyPass));

      String storepass = HBaseConfiguration.getPassword(conf, "ssl.storepass.alias", null);
      assertEquals(storepass, new String(storePass));
    }
  }

  private static class ReflectiveCredentialProviderClient {
    public static final String HADOOP_CRED_PROVIDER_FACTORY_CLASS_NAME =
        "org.apache.hadoop.security.alias.JavaKeyStoreProvider$Factory";
    public static final String
      HADOOP_CRED_PROVIDER_FACTORY_GET_PROVIDERS_METHOD_NAME = "getProviders";

    public static final String HADOOP_CRED_PROVIDER_CLASS_NAME =
        "org.apache.hadoop.security.alias.CredentialProvider";
    public static final String
        HADOOP_CRED_PROVIDER_GET_CREDENTIAL_ENTRY_METHOD_NAME =
        "getCredentialEntry";
    public static final String
        HADOOP_CRED_PROVIDER_GET_ALIASES_METHOD_NAME = "getAliases";
    public static final String
        HADOOP_CRED_PROVIDER_CREATE_CREDENTIAL_ENTRY_METHOD_NAME =
        "createCredentialEntry";
    public static final String HADOOP_CRED_PROVIDER_FLUSH_METHOD_NAME = "flush";

    public static final String HADOOP_CRED_ENTRY_CLASS_NAME =
        "org.apache.hadoop.security.alias.CredentialProvider$CredentialEntry";
    public static final String HADOOP_CRED_ENTRY_GET_CREDENTIAL_METHOD_NAME =
        "getCredential";

    public static final String CREDENTIAL_PROVIDER_PATH =
        "hadoop.security.credential.provider.path";

    private static Object hadoopCredProviderFactory = null;
    private static Method getProvidersMethod = null;
    private static Method getAliasesMethod = null;
    private static Method getCredentialEntryMethod = null;
    private static Method getCredentialMethod = null;
    private static Method createCredentialEntryMethod = null;
    private static Method flushMethod = null;
    private static Boolean hadoopClassesAvailable = null;

    /**
     * Determine if we can load the necessary CredentialProvider classes. Only
     * loaded the first time, so subsequent invocations of this method should
     * return fast.
     *
     * @return True if the CredentialProvider classes/methods are available,
     *         false otherwise.
     */
    private boolean isHadoopCredentialProviderAvailable() {
      if (null != hadoopClassesAvailable) {
        // Make sure everything is initialized as expected
        if (hadoopClassesAvailable && null != getProvidersMethod
            && null != hadoopCredProviderFactory
            && null != getCredentialEntryMethod && null != getCredentialMethod) {
          return true;
        } else {
          // Otherwise we failed to load it
          return false;
        }
      }

      hadoopClassesAvailable = false;

      // Load Hadoop CredentialProviderFactory
      Class<?> hadoopCredProviderFactoryClz = null;
      try {
        hadoopCredProviderFactoryClz = Class
            .forName(HADOOP_CRED_PROVIDER_FACTORY_CLASS_NAME);
      } catch (ClassNotFoundException e) {
        return false;
      }
      // Instantiate Hadoop CredentialProviderFactory
      try {
        hadoopCredProviderFactory = hadoopCredProviderFactoryClz.newInstance();
      } catch (InstantiationException e) {
        return false;
      } catch (IllegalAccessException e) {
        return false;
      }

      try {
        getProvidersMethod = loadMethod(hadoopCredProviderFactoryClz,
            HADOOP_CRED_PROVIDER_FACTORY_GET_PROVIDERS_METHOD_NAME,
            Configuration.class);
        // Load Hadoop CredentialProvider
        Class<?> hadoopCredProviderClz = null;
        hadoopCredProviderClz = Class.forName(HADOOP_CRED_PROVIDER_CLASS_NAME);
        getCredentialEntryMethod = loadMethod(hadoopCredProviderClz,
            HADOOP_CRED_PROVIDER_GET_CREDENTIAL_ENTRY_METHOD_NAME, String.class);

        getAliasesMethod = loadMethod(hadoopCredProviderClz,
            HADOOP_CRED_PROVIDER_GET_ALIASES_METHOD_NAME);

        createCredentialEntryMethod = loadMethod(hadoopCredProviderClz,
            HADOOP_CRED_PROVIDER_CREATE_CREDENTIAL_ENTRY_METHOD_NAME,
            String.class, char[].class);

        flushMethod = loadMethod(hadoopCredProviderClz,
            HADOOP_CRED_PROVIDER_FLUSH_METHOD_NAME);

        // Load Hadoop CredentialEntry
        Class<?> hadoopCredentialEntryClz = null;
        try {
          hadoopCredentialEntryClz = Class
              .forName(HADOOP_CRED_ENTRY_CLASS_NAME);
        } catch (ClassNotFoundException e) {
          LOG.error("Failed to load class:" + e);
          return false;
        }

        getCredentialMethod = loadMethod(hadoopCredentialEntryClz,
            HADOOP_CRED_ENTRY_GET_CREDENTIAL_METHOD_NAME);
      } catch (Exception e1) {
        return false;
      }

      hadoopClassesAvailable = true;
      LOG.info("Credential provider classes have been" +
          " loaded and initialized successfully through reflection.");
      return true;

    }

    private Method loadMethod(Class<?> clz, String name, Class<?>... classes)
        throws Exception {
      Method method = null;
      try {
        method = clz.getMethod(name, classes);
      } catch (SecurityException e) {
        fail("security exception caught for: " + name + " in " +
      clz.getCanonicalName());
        throw e;
      } catch (NoSuchMethodException e) {
        LOG.error("Failed to load the " + name + ": " + e);
        fail("no such method: " + name + " in " + clz.getCanonicalName());
        throw e;
      }
      return method;
    }

    /**
     * Wrapper to fetch the configured {@code List<CredentialProvider>}s.
     *
     * @param conf
     *    Configuration with GENERAL_SECURITY_CREDENTIAL_PROVIDER_PATHS defined
     * @return List of CredentialProviders, or null if they could not be loaded
     */
    @SuppressWarnings("unchecked")
    protected  List<Object> getCredentialProviders(Configuration conf) {
      // Call CredentialProviderFactory.getProviders(Configuration)
      Object providersObj = null;
      try {
        providersObj = getProvidersMethod.invoke(hadoopCredProviderFactory,
            conf);
      } catch (IllegalArgumentException e) {
        LOG.error("Failed to invoke: " + getProvidersMethod.getName() +
            ": " + e);
        return null;
      } catch (IllegalAccessException e) {
        LOG.error("Failed to invoke: " + getProvidersMethod.getName() +
            ": " + e);
        return null;
      } catch (InvocationTargetException e) {
        LOG.error("Failed to invoke: " + getProvidersMethod.getName() +
            ": " + e);
        return null;
      }

      // Cast the Object to List<Object> (actually List<CredentialProvider>)
      try {
        return (List<Object>) providersObj;
      } catch (ClassCastException e) {
        return null;
      }
    }

    /**
     * Create a CredentialEntry using the configured Providers.
     * If multiple CredentialProviders are configured, the first will be used.
     *
     * @param conf
     *          Configuration for the CredentialProvider
     * @param name
     *          CredentialEntry name (alias)
     * @param credential
     *          The credential
     */
    public  void createEntry(Configuration conf, String name, char[] credential)
        throws Exception {

      if (!isHadoopCredentialProviderAvailable()) {
        return;
      }

      List<Object> providers = getCredentialProviders(conf);
      if (null == providers) {
        throw new IOException("Could not fetch any CredentialProviders, " +
            "is the implementation available?");
      }

      Object provider = providers.get(0);
      createEntryInProvider(provider, name, credential);
    }

    /**
     * Create a CredentialEntry with the give name and credential in the
     * credentialProvider. The credentialProvider argument must be an instance
     * of Hadoop
     * CredentialProvider.
     *
     * @param credentialProvider
     *          Instance of CredentialProvider
     * @param name
     *          CredentialEntry name (alias)
     * @param credential
     *          The credential to store
     */
    private void createEntryInProvider(Object credentialProvider,
        String name, char[] credential) throws Exception {

      if (!isHadoopCredentialProviderAvailable()) {
        return;
      }

      try {
        createCredentialEntryMethod.invoke(credentialProvider, name, credential);
      } catch (IllegalArgumentException e) {
        return;
      } catch (IllegalAccessException e) {
        return;
      } catch (InvocationTargetException e) {
        return;
      }

      try {
        flushMethod.invoke(credentialProvider);
      } catch (IllegalArgumentException e) {
        throw e;
      } catch (IllegalAccessException e) {
        throw e;
      } catch (InvocationTargetException e) {
        throw e;
      }
    }
  }
}
