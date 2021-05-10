package io.github.whippetdb.memory.basic;

import java.io.File;
import org.junit.Test;

import io.github.whippetdb.memory.basic.SimpleFileDataIO;
import io.github.whippetdb.util.Util;

public class SimpleFileDataIOTest {
   @Test
   public void test() throws Exception {
      SimpleFileDataIO mem = new SimpleFileDataIO("tmp/", 32) {
         protected void checkWrite(long addr, int len) {
            if(addr < 0) throw new IllegalArgumentException("Writing to negative address: " + addr);
            if(addr > size) throw new IllegalArgumentException("Writing past boundary: " + addr);
            ensureSize(addr + len);
         }
      };
      
      mem.fillByte(0, 10, 0);
      mem.fillLong(10, 20, 0x0807060504030201L);
      mem.fillByte(30, 10, 0);
      Util.assertEquals(mem.toString(), "[0,0,0,0,0,0,0,0,0,0,1,2,3,4,5,6,7,8,1,2,3,4,5,6,7,8,1,2,3,4,0,0,0,0,0,0,0,0,0,0]");
      
      File f = mem.file();
      mem.close();
      mem = null;
      System.gc();
      
      Thread.sleep(300);
      Util.assertEquals(f.exists(), false);
   }
}
