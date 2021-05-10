package io.github.whippetdb.util;

import org.junit.Test;

import io.github.whippetdb.memory.basic.SimpleFileDataBuffer;
import io.github.whippetdb.util.FileUtil;
import io.github.whippetdb.util.Util;

public class FileRemoveTest {
   @Test
   public void test() throws Exception {
      String path = "tmp/" + getClass().getSimpleName();
      SimpleFileDataBuffer mem = new SimpleFileDataBuffer(path, 1024, true);
      mem.writeLong(1016, 111111);
      mem.close();
      
      FileUtil.remove(path);
      
      mem = new SimpleFileDataBuffer(path, 1024, true);
      Util.assertEquals(mem.readLong(1016), 0L);
      
      FileUtil.remove(path);
   }
}
