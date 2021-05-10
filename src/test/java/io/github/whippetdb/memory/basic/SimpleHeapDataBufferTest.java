package io.github.whippetdb.memory.basic;

import org.junit.Test;

import io.github.whippetdb.memory.basic.SimpleHeapDataBuffer;
import io.github.whippetdb.util.Util;

public class SimpleHeapDataBufferTest {
   @Test
   public void test() {
      SimpleHeapDataBuffer mem = new SimpleHeapDataBuffer();
      Util.assertEquals(mem.toString(), "[]");
      mem.fillByte(0, 5, 123);
      Util.assertEquals(mem.toString(), "[123,123,123,123,123]");
      mem.fillLong(5, 2, 0x0807060504030201L);
      Util.assertEquals(mem.toString(), "[123,123,123,123,123,1,2]");
      mem.fillLong(7, 17, 0x0807060504030201L);
      Util.assertEquals(mem.toString(), "[123,123,123,123,123,1,2,1,2,3,4,5,6,7,8,1,2,3,4,5,6,7,8,1]");
      Util.assertEquals(mem.readLong(5), 0x0605040302010201L);
      Util.assertEquals(mem.readLong(15), 0x0807060504030201L);
      Util.assertEquals(mem.readLong(16), 0x0108070605040302L);
   }
}
