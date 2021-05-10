package io.github.whippetdb.db.api.types;

import io.github.whippetdb.db.api.TypeIO;
import io.github.whippetdb.memory.api.MemArray;
import io.github.whippetdb.memory.api.MemDataArray;
import io.github.whippetdb.memory.basic.SimpleHeapDataBuffer;

public class FixedBytesIO implements TypeIO<byte[]> {
   private final int size;
   
   public FixedBytesIO (int size) {
      this.size = size;
   }
   
   @Override
   public boolean isApplicable(Object o) {
      return o instanceof byte[];
   }

   @Override
   public MemArray convert(byte[] obj, MemDataArray tmpBuf) {
      return new SimpleHeapDataBuffer(obj, size);
   }

   @Override
   public void writeObject(byte[] obj, MemDataArray buf) {
      buf.write(0, obj, 0, size);
   }

   @Override
   public byte[] readObject(MemDataArray buf) {
      byte[] bytes = new byte[size];
      buf.read(0, bytes, 0, size);
      return bytes;
   }

   @Override
   public Class<byte[]> dataType() {
      return byte[].class;
   }
   
   @Override
   public String toString() {
      return toJson();
   }
}
