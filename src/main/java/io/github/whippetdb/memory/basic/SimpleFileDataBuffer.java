package io.github.whippetdb.memory.basic;

import java.io.IOException;
import java.nio.channels.FileChannel.MapMode;

import io.github.whippetdb.memory.api.MemDataBuffer;
import io.github.whippetdb.util.Util;

public class SimpleFileDataBuffer extends SimpleFileDataIO implements MemDataBuffer{
   public SimpleFileDataBuffer(String path, long initialSize, boolean... persist) throws IOException {
      super(path, initialSize, persist);
   }

   public SimpleFileDataBuffer(String path, MapMode mapMode, long initialSize, boolean... persist) throws IOException {
      super(path, mapMode, initialSize, persist);
   }

   @Override
   public void setSize(long v) {
      if(v > size) ensureCapacity(v);
      size = v;
   }

   @Override
   public long size() {
      return size;
   }
   
   @Override
   protected void checkWrite(long addr, int len) {
      Util.checkAppend(addr, size);
      ensureSize(addr + len);
   }
}
