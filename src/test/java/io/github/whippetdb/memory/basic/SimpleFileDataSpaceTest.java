package io.github.whippetdb.memory.basic;

import java.io.IOException;

import org.junit.Test;

import io.github.whippetdb.memory.basic.SimpleFileDataSpace;
import io.github.whippetdb.util.Util;

public class SimpleFileDataSpaceTest {
   @Test
   public void test() throws IOException {
      SimpleFileDataSpace ms=new SimpleFileDataSpace("tmp", 0);
      ms.allocate(20, true);
      ms.write(0, Util.bytes(1,2,3,4,5), 0, 5);
      Util.assertEquals(ms.toString(0, 5), "[1,2,3,4,5]");
   }
}
