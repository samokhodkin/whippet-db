package io.github.whippetdb.memory.api;

import static io.github.whippetdb.memory.api.BytesLE.*;

import java.util.Arrays;
import org.junit.Test;

import io.github.whippetdb.util.Util;

public class BytesLeTest {
   @Test
   public void test() {
      byte[] data = {(byte)0xf1, (byte)0xf2, (byte)0xf3, (byte)0xf4, (byte)0xf5, (byte)0xf6, (byte)0xf7, (byte)0xf8, (byte)0xf9};
      Util.assertEquals(readShort(data, 0), 0xf2f1);
      Util.assertEquals(readInt(data, 0), 0xf4f3f2f1);
      Util.assertEquals(readLong(data, 0), 0xf8f7f6f5f4f3f2f1L);
      
      writeBytes(data, 0, 0L, 8);
      writeBytes(data, 8, 0L, 1);
      Util.assertEquals(Arrays.toString(data), "[0, 0, 0, 0, 0, 0, 0, 0, 0]");
      
      writeBytes(data, 0, 0x0807060504030201L, 8, 0, 5);
      Util.assertEquals(Arrays.toString(data), "[1, 2, 3, 4, 5, 0, 0, 0, 0]");
      
      writeBytes(data, 5, 0x0807060504030201L, 8, 5, 8);
      Util.assertEquals(Arrays.toString(data), "[1, 2, 3, 4, 5, 6, 7, 8, 0]");
      
      Util.assertEquals(readBytes(data, 0, 8, 0, 5), 0x0504030201L);
      Util.assertEquals(readBytes(data, 5, 8, 5, 8), 0x0807060000000000L);
      Util.assertEquals(readBytes(data, 0, 8, 0, 8), 0x0807060504030201L);
   }
}
