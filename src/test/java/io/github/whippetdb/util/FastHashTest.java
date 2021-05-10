package io.github.whippetdb.util;

import static io.github.whippetdb.util.FastHash.*;

import org.junit.Test;

import io.github.whippetdb.memory.api.MemDataArray;
import io.github.whippetdb.memory.api.MemDataSpace;
import io.github.whippetdb.memory.basic.SimpleHeapDataBuffer;
import io.github.whippetdb.memory.basic.SimpleHeapDataSpace;
import io.github.whippetdb.util.Util;

public class FastHashTest {
   @Test
   public void test() {
      byte[] data=new byte[]{
         0,1,2,3,4,5,6,7,8,9,
         0,1,2,3,
         0,1,2,3,4,5,6,7,8,9
      };
      MemDataArray str=new SimpleHeapDataBuffer(data);
      MemDataArray str1=new SimpleHeapDataBuffer(new byte[]{
         -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
         -1,-1,-1,-1,
         0,1,2,3,4,5,6,7,8,9
      });
      
      MemDataSpace mem = new SimpleHeapDataSpace();
      long addr = mem.allocate((int)str1.size(), false);
      mem.write(addr, str1, 0, (int)str1.size());
      Util.assertEquals(hash64(str1,0,(int)str1.size()), hash64(mem,addr,(int)str1.size()));
      
      Util.assertEquals(hash64(str,0,10), hash64(str,14,10));
      Util.assertEquals(hash64(str,0,4), hash64(str,10,4));
      Util.assertEquals(hash64(str,1,9), hash64(str,15,9));
      Util.assertEquals(hash64(str,1,9), hash64(str1,15,9));
      
      Util.assertNotEquals(hash64(str,0,10), hash64(str,1,10));
      Util.assertNotEquals(hash64(str,0,10), hash64(str,2,10));
      Util.assertNotEquals(hash64(str,0,10), hash64(str,3,10));
      Util.assertNotEquals(hash64(str,0,10), hash64(str,4,10));
      
      Util.assertEquals(hash64(str,0,8), hash64(str.readLong(0)));
      Util.assertEquals(hash64(str,5,8), hash64(str.readLong(5)));
      Util.assertNotEquals(hash64(str,5,8), hash64(str.readLong(6)));
   }
}
