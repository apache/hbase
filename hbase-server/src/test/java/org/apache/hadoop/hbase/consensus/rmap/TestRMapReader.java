package org.apache.hadoop.hbase.consensus.rmap;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestRMapReader {
  static String base = "file:/some/rmap.json";

  RMapReader reader;
  URI current, next, v2, v3;

  @Test
  public void shouldReturnVersionedURI() throws URISyntaxException {
    assertEquals(v2, RMapReader.getVersionedURI(base, 2));
  }

  @Test
  public void shouldReturnCurrentVersionedURI() throws URISyntaxException {
    assertEquals(current, RMapReader.getVersionedURI(base, RMapReader.CURRENT));
  }

  @Test
  public void shouldReturnNextVersionedURI() throws URISyntaxException {
    assertEquals(next, RMapReader.getVersionedURI(base, RMapReader.NEXT));
  }

  @Test
  public void shouldReturnAbsoluteVersion() throws URISyntaxException {
    assertEquals(2, RMapReader.getVersion(v2));
  }

  @Test
  public void shouldReturnCurrentSymbolicVersion() throws URISyntaxException {
    assertEquals(RMapReader.CURRENT, RMapReader.getVersion(current));
  }

  @Test
  public void shouldReturnNextSymbolicVersion() throws URISyntaxException {
    assertEquals(RMapReader.NEXT, RMapReader.getVersion(next));
  }

  @Test
  public void shouldReturnUnknownSymbolicVersion() throws URISyntaxException {
    assertEquals(RMapReader.UNKNOWN,
            RMapReader.getVersion(new URI(base + "?version=FOO")));
  }

  @Test
  public void shouldResolveSymbolicVersionAndReturnRMap()
          throws URISyntaxException, IOException, RMapException {
    // Stub the abstract methods and forward call to RMapReader.readRMap().
    // This is a bit frowned upon.
    when(reader.resolveSymbolicVersion(current)).thenReturn(v2);
    when(reader.readRMapAsString(v2)).thenReturn("{}");
    when(reader.readRMap(current)).thenCallRealMethod();

    RMapJSON rmap = reader.readRMap(current);
    assertEquals(v2, rmap.uri);
    assertEquals("{}", rmap.rmap.toString());
  }

  @Test
  public void shouldReturnMD5HashAsHex() throws NoSuchAlgorithmException {
    assertEquals("99914b932bd37a50b983c5e7c90ae93b",
            RMapReader.getSignature("{}"));
  }

  @Test
  public void shouldReturnCurrentVersion() throws URISyntaxException {
    when(reader.resolveSymbolicVersion(current)).thenReturn(v2);
    when(reader.getCurrentVersion(base)).thenCallRealMethod();

    assertEquals(2, reader.getCurrentVersion(base));
  }

  @Test
  public void shoudlReturnNextVersion() throws URISyntaxException {
    when(reader.resolveSymbolicVersion(next)).thenReturn(v3);
    when(reader.getNextVersion(base)).thenCallRealMethod();

    assertEquals(3, reader.getNextVersion(base));
  }

  @Before
  public void setUp() throws URISyntaxException, IOException, RMapException {
    reader = mock(RMapReader.class);
    // URIs can not be created outside of the method.
    current = RMapReader.getVersionedURI(base, RMapReader.CURRENT);
    next = RMapReader.getVersionedURI(base, RMapReader.NEXT);
    v2 = RMapReader.getVersionedURI(base, 2);
    v3 = RMapReader.getVersionedURI(base, 3);
  }
}
