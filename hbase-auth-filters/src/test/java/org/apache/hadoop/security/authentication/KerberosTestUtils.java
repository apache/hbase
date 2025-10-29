/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package org.apache.hadoop.security.authentication;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;

import org.apache.hadoop.security.authentication.util.KerberosUtil;

import java.io.File;
import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.UUID;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import static org.apache.hadoop.util.PlatformName.IBM_JAVA;

/**
 * Test helper class for Java Kerberos setup.
 */
public class KerberosTestUtils {
  private static String keytabFile = new File(System.getProperty("test.dir", "target"),
          UUID.randomUUID().toString()).getAbsolutePath();

  public static String getRealm() {
    return "EXAMPLE.COM";
  }

  public static String getClientPrincipal() {
    return "client@EXAMPLE.COM";
  }

  public static String getServerPrincipal() {
    return "HTTP/localhost@EXAMPLE.COM";
  }

  public static String getKeytabFile() {
    return keytabFile;
  }

  private static class KerberosConfiguration extends Configuration {
    private String principal;

    public KerberosConfiguration(String principal) {
      this.principal = principal;
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
      Map<String, String> options = new HashMap<String, String>();
      if (IBM_JAVA) {
        options.put("useKeytab", KerberosTestUtils.getKeytabFile().startsWith("file://") ?   
                    KerberosTestUtils.getKeytabFile() : "file://" +  KerberosTestUtils.getKeytabFile());
        options.put("principal", principal);
        options.put("refreshKrb5Config", "true");
        options.put("credsType", "both");
      } else {
        options.put("keyTab", KerberosTestUtils.getKeytabFile());
        options.put("principal", principal);
        options.put("useKeyTab", "true");
        options.put("storeKey", "true");
        options.put("doNotPrompt", "true");
        options.put("useTicketCache", "true");
        options.put("renewTGT", "true");
        options.put("refreshKrb5Config", "true");
        options.put("isInitiator", "true");
      } 
      String ticketCache = System.getenv("KRB5CCNAME");
      if (ticketCache != null) {
        if (IBM_JAVA) {
          // IBM JAVA only respect system property and not env variable
          // The first value searched when "useDefaultCcache" is used.
          System.setProperty("KRB5CCNAME", ticketCache);
          options.put("useDefaultCcache", "true");
          options.put("renewTGT", "true");
        } else {
          options.put("ticketCache", ticketCache);
        }
      }
      options.put("debug", "true");

      return new AppConfigurationEntry[]{
        new AppConfigurationEntry(KerberosUtil.getKrb5LoginModuleName(),
                                  AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                                  options),};
    }
  }

  public static <T> T doAs(String principal, final Callable<T> callable) throws Exception {
    LoginContext loginContext = null;
    try {
      Set<Principal> principals = new HashSet<>();
      principals.add(new KerberosPrincipal(KerberosTestUtils.getClientPrincipal()));
      Subject subject = new Subject(false, principals, new HashSet<>(), new HashSet<>());
      loginContext = new LoginContext("", subject, null, new KerberosConfiguration(principal));
      loginContext.login();
      subject = loginContext.getSubject();
      return Subject.doAs(subject, new PrivilegedExceptionAction<T>() {
        @Override
        public T run() throws Exception {
          return callable.call();
        }
      });
    } catch (PrivilegedActionException ex) {
      throw ex.getException();
    } finally {
      if (loginContext != null) {
        loginContext.logout();
      }
    }
  }

  public static <T> T doAsClient(Callable<T> callable) throws Exception {
    return doAs(getClientPrincipal(), callable);
  }

  public static <T> T doAsServer(Callable<T> callable) throws Exception {
    return doAs(getServerPrincipal(), callable);
  }

}
