package org.apache.hadoop.hbase.io.compress.lz4;

import java.io.File;

final class NativeLoader {
    private static volatile boolean loaded = false;

    static synchronized boolean load() {
        if (loaded) return true;

        try {
            // 尝试使用环境变量
            String lib = System.getenv("HBASE_LZ4RVV_LIB");
            if (lib != null && !lib.isEmpty()) {
                File f = new File(lib);
                System.load(f.getAbsolutePath());
            } else {
                // 使用相对路径加载
                String relativePath = "libhbase_lz4rvv.so";
                File f = new File(relativePath);
                System.load(f.getAbsolutePath());
            }
            loaded = true;
        } catch (Throwable t) {
            loaded = false;
            System.err.println("[NativeLoader ERROR] Failed to load native library: " + t);
            t.printStackTrace();
        }

        return loaded;
    }

    static boolean isLoaded() {
        return loaded;
    }

    private NativeLoader() {}
}

