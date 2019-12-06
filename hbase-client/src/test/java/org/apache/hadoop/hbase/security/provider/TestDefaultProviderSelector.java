package org.apache.hadoop.hbase.security.provider;

import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

public class TestDefaultProviderSelector {

  DefaultProviderSelector selector;
  @Before
  public void setup() {
    selector = new DefaultProviderSelector();
  }
  

  @Test(expected = IllegalStateException.class)
  public void testExceptionOnMissingProviders() {
    selector.configure(new Configuration(false), Collections.emptyMap());
  }

  @Test(expected = NullPointerException.class)
  public void testNullConfiguration() {
    selector.configure(null, Collections.emptyMap());
  }

  @Test(expected = NullPointerException.class)
  public void testNullProviderMap() {
    selector.configure(new Configuration(false), null);
  }

  @Test(expected = IllegalStateException.class)
  public void testDuplicateProviders() {
    Map<Byte,SaslClientAuthenticationProvider> providers = new HashMap<>();
    providers.put((byte) 1, new SimpleSaslClientAuthenticationProvider());
    providers.put((byte) 2, new SimpleSaslClientAuthenticationProvider());
    selector.configure(new Configuration(false), providers);
  }

  @Test
  public void testExpectedProviders() {
    Map<Byte,SaslClientAuthenticationProvider> providers = new HashMap<>();

    for (SaslClientAuthenticationProvider provider : Arrays.asList(
        new SimpleSaslClientAuthenticationProvider(), new GssSaslClientAuthenticationProvider(),
        new DigestSaslClientAuthenticationProvider())) {
      providers.put(provider.getSaslAuthMethod().getCode(), provider);
    }

    selector.configure(new Configuration(false), providers);

    assertNotNull("Simple provider was null", selector.simpleAuth);
    assertNotNull("Kerberos provider was null", selector.krbAuth);
    assertNotNull("Digest provider was null", selector.digestAuth);
  }
}
