package org.apache.hadoop.hbase.io;

import org.apache.hadoop.fs.CanUnbuffer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;

public class TestFSDataInputStreamWrapper {
    private Method unbuffer = null;

    @Test
    public void unbuffer() {
        InputStream wrappedStream = new SonStream();
        final Class<? extends InputStream> streamClass = wrappedStream.getClass();
        Class<?>[] streamInterfaces = streamClass.getInterfaces();
        for (Class c : streamInterfaces) {
            if (c.getCanonicalName().toString().equals("org.apache.hadoop.fs.CanUnbuffer")) {
                try {
                    this.unbuffer = streamClass.getDeclaredMethod("unbuffer");
                } catch (NoSuchMethodException | SecurityException e) {
                    return;
                }
                break;
            }
        }
        Assert.assertEquals(false, unbuffer != null);
        unbuffer = null;
        if (wrappedStream instanceof CanUnbuffer) {
            try {
                this.unbuffer = streamClass.getDeclaredMethod("unbuffer");
            } catch (NoSuchMethodException | SecurityException e) {
                return;
            }
        }
        Assert.assertEquals(true, unbuffer != null);
    }

    public class SonStream extends FatherStream {
        @Override
        public void unbuffer() {

        }
    }

    public class FatherStream extends InputStream implements CanUnbuffer {

        @Override
        public void unbuffer() {

        }

        @Override
        public int read() throws IOException {
            return 0;
        }
    }
}
