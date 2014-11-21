package org.apache.hadoop.hbase.consensus;

import java.util.Arrays;
import java.util.List;

public class RaftTestDataProvider {

  public static List<Object[]> getRaftBasicLogTestData() {
    List<int[]> test1 = Arrays.asList(new int[][]{
      {1},
      {1},
      {1},
      {1},
      {1}
    });

    List<int[]> test2 = Arrays.asList(new int[][] {
      {1, 2},
      {1},
      {1},
      {1},
      {1}
    });

    List<int[]> test3 = Arrays.asList(new int[][] {
      {1, 2},
      {1, 2},
      {1},
      {1},
      {1}
    });

    List<int[]> test4 = Arrays.asList(new int[][] {
      {1, 2},
      {1, 2},
      {1, 2},
      {1},
      {1}
    });


    List<int[]> test5 = Arrays.asList(new int[][] {
      {1, 2},
      {1, 2},
      {1, 2},
      {1, 2},
      {1}
    });


    List<int[]> test6 = Arrays.asList(new int[][] {
      {1, 2},
      {1, 2},
      {1, 3},
      {1, 3},
      {1}
    });

    List<int[]> test7 = Arrays.asList(new int[][] {
      {1, 2},
      {1, 2},
      {1, 3},
      {1, 3},
      {1, 3}
    });

    List<int[]> test8 = Arrays.asList(new int[][] {
      {1, 1, 2},
      {1, 1, 1},
      {1, 1, 2},
      {1, 1, 1},
      {}
    });

    List<int[]> test9 = Arrays.asList(new int[][] {
      {1, 1},
      {1},
      {1},
      {},
      {}
    });

    List<int[]> test10 = Arrays.asList(new int[][] {
      {1, 1, 1, 4, 4, 5, 5, 6, 6, 6},
      {1, 1, 1, 4},
      {1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 7},
      {},
      {}
    });


    Object[][] data = new Object[][] {
      { test1 },
      { test2 },
      { test3 },
      { test4 },
      { test5 },
      { test6 },
      { test7 },
      { test8 },
      { test9 },
      { test10 }
    };
    return Arrays.asList(data);
  }

  public static List<Object[]> getRaftBasicLogTestSeedData() {
    List<int[]> test1 = Arrays.asList(new int[][]{
      {1, 1, 1, 1, 1, 1 ,1},
      {1, 1, 1},
      {1, 1, 1, 1, 1, 1, 1},
      {1, 1, 1, 1},
      {1, 1, 1, 1, 1, 1, 1}
    });

    List<int[]> test2 = Arrays.asList(new int[][] {
      {1, 1, 1, 1, 1, 1 ,1},
      {1, 1, 1, 1, 1, 1 ,1},
      {1, 1, 2},
      {1},
      {1, 1, 1, 1, 1, 1 ,1}
    });

    Object[][] data = new Object[][] {
      { test1 },
      { test2 },
    };
    return Arrays.asList(data);
  }
}
