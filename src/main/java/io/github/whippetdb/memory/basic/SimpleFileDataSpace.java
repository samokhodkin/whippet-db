package io.github.whippetdb.memory.basic;

import java.io.IOException;
import java.nio.channels.FileChannel.MapMode;

import io.github.whippetdb.memory.api.MemDataSpace;
import io.github.whippetdb.util.Util;

public class SimpleFileDataSpace extends SimpleFileDataIO implements MemDataSpace {
   protected int pageSize;

   public SimpleFileDataSpace(String path, long reserved) throws IOException {
      this(path, reserved, 0);
   }

   public SimpleFileDataSpace(String path, MapMode mapMode, long reserved) throws IOException {
      this(path, mapMode, reserved, 0);
   }
   
   public SimpleFileDataSpace(String path, long reserved, int lgPageSize) throws IOException {
      super(path, Util.ceil2(reserved, 1<<lgPageSize));
      this.pageSize = 1<<lgPageSize;
   }

   public SimpleFileDataSpace(String path, MapMode mapMode, long reserved, int lgPageSize) throws IOException {
      super(path, mapMode, Util.ceil2(reserved, 1<<lgPageSize));
      this.pageSize = 1<<lgPageSize;
   }

   public long allocate(int len, boolean clear) {
      long addr = size;
      long newSize = addr + Util.ceil2(len, pageSize);
      
      ensureCapacity(newSize);
      
      size = newSize;
      if(clear) fillLong(addr, len, 0);
      return addr;
   }

   public long allocatedSize() {
      return size;
   }
   
   @Override
   public void setSize(long v) {
      super.setSize(Util.ceil2(v, pageSize));
   }

   @Override
   protected void checkWrite(long addr, int len) {
      Util.checkWrite(addr, len, size);
   }
}
