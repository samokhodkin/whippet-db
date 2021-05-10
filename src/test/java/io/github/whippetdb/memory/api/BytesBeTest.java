package io.github.whippetdb.memory.api;

import static io.github.whippetdb.memory.api.BytesBE.*;

import java.util.Arrays;
import org.junit.Test;

import io.github.whippetdb.util.Util;

public class BytesBeTest {
   @Test
   public void test() {
      byte[] data = {(byte)0xf1, (byte)0xf2, (byte)0xf3, (byte)0xf4, (byte)0xf5, (byte)0xf6, (byte)0xf7, (byte)0xf8, (byte)0xf9};
      Util.assertEquals(readShort(data, 0), 0xf1f2);
      Util.assertEquals(readInt(data, 0), 0xf1f2f3f4);
      Util.assertEquals(readLong(data, 0), 0xf1f2f3f4f5f6f7f8L);
      
      writeBytes(data, 0, 0L, 8);
      writeBytes(data, 8, 0L, 1);
      Util.assertEquals(Arrays.toString(data), "[0, 0, 0, 0, 0, 0, 0, 0, 0]");
      
      writeBytes(data, 0, 0x0807060504030201L, 8, 0, 5);
      Util.assertEquals(Arrays.toString(data), "[8, 7, 6, 5, 4, 0, 0, 0, 0]");
      
      writeBytes(data, 5, 0x0807060504030201L, 8, 5, 8);
      Util.assertEquals(Arrays.toString(data), "[8, 7, 6, 5, 4, 3, 2, 1, 0]");
      
      Util.assertEquals(readBytes(data, 0, 8, 0, 5), 0x0807060504000000L);
      Util.assertEquals(readBytes(data, 5, 8, 5, 8), 0x0000000000030201L);
      Util.assertEquals(readBytes(data, 0, 8, 0, 8), 0x0807060504030201L);
   }
}
