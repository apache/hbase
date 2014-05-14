package org.apache.hadoop.hbase.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import static junit.framework.Assert.*;

/**
 * Test the ParamFormatter framework
 */
@Category(SmallTests.class)
public class TestParamFormatter {
  private final static Log LOG = LogFactory.getLog(TestParamFormatter.class);


  @Test
  public void test1() throws Exception {
    MyLocalServer server = new MyLocalServer();

    ParamFormatHelper<MyLocalServer> simple =
        new ParamFormatHelper<MyLocalServer>(server);

    ParamFormatHelper<MyLocalServer> detailed =
        new ParamFormatHelper<MyLocalServer>(server, server.getClass(),
            ParamFormat.FormatTypes.DETAILED);



    Object[] params1 = server.testFunction1(1, "one");
    Object[] params2 = server.testFunction2(2, new String[]{"two"});

    Method func1 =
        server.getClass().getMethod("testFunction1", int.class, String.class);

    Method func2 =
        server.getClass().getMethod("testFunction2", long.class, String[].class);

    // we don't have pretty printing for func1 (detailed)
    assertNull(detailed.getMap(func1, params1));

    // no info for func2 on simple formatType
    assertNull(simple.getMap(func2, params2));

    Map<String, Object> simpleRes = simple.getMap(func1, params1);
    assertEquals(simpleRes.get("p1"), params1[0]);
    assertEquals(simpleRes.get("p2"), params1[1]);

    Map<String, Object> detailedRes = detailed.getMap(func2, params2);
    assertEquals(detailedRes.get("p1"), params2[0]);
    assertEquals(detailedRes.get("p2"), params2[1]);
  }


  @Test
  public void test2() {
    MyFailureCaseServer server = new MyFailureCaseServer();
    try {
      ParamFormatHelper<MyFailureCaseServer> simple =
          new ParamFormatHelper<MyFailureCaseServer>(server);
    }
    catch(RuntimeException e) {
      return;
    }
    fail("Should not succeed when the Pretty printer is private");
  }

  @Test
  public void test3() {
    try {
      MyFailureCaseServer2 server = new MyFailureCaseServer2();
      ParamFormatHelper<MyFailureCaseServer2> simple =
          new ParamFormatHelper<MyFailureCaseServer2>(server);
    }
    catch(RuntimeException e) {
      return;
    }
    fail("Should not succeed when the Pretty printer is not static");
  }

}

/**
 * Simple class (the context) for testing pretty printing of function args
 */
class MyLocalServer {
  @ParamFormat(clazz = MyTwoParamFormatter.class)
  public Object[] testFunction1(int a, String b) {
    return new Object[] {a, b};
  }

  @ParamFormat(clazz = MyTwoParamFormatter.class, formatType = ParamFormat.FormatTypes.DETAILED)
  public Object[] testFunction2(long param1, String[] param2) {
    return new Object[] {param1, param2};
  }

  /**
   * A Simple pretty printer for methods with two args. Simply returns a
   * map with p1 -> first param and p2 -> second param
   */
  public static class MyTwoParamFormatter implements ParamFormatter<MyLocalServer>
  {
    @Override
    public Map<String, Object> getMap(Object[] params, MyLocalServer myLocalServer) {
      Map<String, Object> res = new HashMap<String, Object>();
      res.put("p1", params[0]);
      res.put("p2", params[1]);
      return res;
    }
  }
}

/**
 * Class used to check that we fail on instantiation of the pretty print class
 */
class MyFailureCaseServer {
  @ParamFormat(clazz = MyPrivateParamFormatter.class)
  public void testFunc(int a, String b) {}

  // should fail because it is private
  private static class MyPrivateParamFormatter implements ParamFormatter<MyFailureCaseServer>
  {
    @Override
    public Map<String, Object> getMap(Object[] params, MyFailureCaseServer myLocalServer) {
      return null; }
  }
}

/**
 * Class used to check that we fail on instantiation of the pretty print class
 */
class MyFailureCaseServer2 {
  @ParamFormat(clazz = MyPrivateParamFormatter.class)
  public void testFunc(int a, String b) {}

  // should fail because it is not static
  public class MyPrivateParamFormatter implements ParamFormatter<MyFailureCaseServer2>
  {
    @Override
    public Map<String, Object> getMap(Object[] params, MyFailureCaseServer2 myLocalServer) {
      return null; }
  }
}
