package io.github.whippetdb.memory.basic;

import org.junit.Test;

import io.github.whippetdb.memory.basic.SimpleHeapDataSpace;
import io.github.whippetdb.util.Util;

public class SimpleHeapDataSpaceTest {
   @Test
   public void test() {
      SimpleHeapDataSpace mem = new SimpleHeapDataSpace();
      Util.assertEquals(mem.toString(), "[]");
      long a = mem.allocate(11, true);
      Util.assertEquals(mem.toString(), "[0,0,0,0,0,0,0,0,0,0,0]");
      mem.fillLong(a, 11, 0x0807060504030201L);
      Util.assertEquals(mem.toString(), "[1,2,3,4,5,6,7,8,1,2,3]");
   }
}
