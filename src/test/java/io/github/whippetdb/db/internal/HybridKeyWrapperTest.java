package io.github.whippetdb.db.internal;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.Test;

import io.github.whippetdb.memory.api.MemIO;
import io.github.whippetdb.memory.basic.SimpleDirectDataBuffer;

public class HybridKeyWrapperTest {

   @Test
   public void test() {
      MemIO data = new SimpleDirectDataBuffer();
      data.write(0, new byte[]{0,0,0,1,2,3,4,5,6,7,8,9}, 0, 12);
      HybridKeyWrapper wrapper = new HybridKeyWrapper();
      int wsize = 15;
      byte[] out = new byte[wsize];
      
      wrapper.setData(wsize, data, 3, 9);
      
      assertEquals(wrapper.toString(), "[9,1,2,3,4,5,6,7,8,9,0,0,0,0,0]");
      wrapper.read(0, out, 0, wsize);
      assertEquals(Arrays.toString(out), "[9, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 0, 0, 0, 0]");
      
      wrapper.setData(wsize, data, 3, 4);
      
      assertEquals(wrapper.toString(), "[4,1,2,3,4,0,0,0,0,0,0,0,0,0,0]");
      wrapper.read(0, out, 0, wsize);
      assertEquals(Arrays.toString(out), "[4, 1, 2, 3, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]");
   }
}
