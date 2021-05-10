package io.github.whippetdb.db.api.types;

import java.nio.ByteBuffer;

import io.github.whippetdb.db.api.TypeIO;
import io.github.whippetdb.memory.api.MemArray;
import io.github.whippetdb.memory.api.MemDataArray;

public class ByteBufferIO implements TypeIO<ByteBuffer> {
   private final Integer avg, max;
   
   /**
    * @param args - average, max
    */
   public ByteBufferIO(Integer... args) {
      avg = args.length > 0? args[0]: null;
      max = args.length > 1? args[1]: null;
   }
   
   @Override
   public Integer size() {
      return null;
   }
   
   @Override
   public Integer maxSize() {
      return max;
   }
   
   @Override
   public Integer avgSize() {
      return avg;
   }
   
   @Override
   public boolean isApplicable(Object o) {
      return o instanceof ByteBuffer;
   }

   @Override
   public MemArray convert(ByteBuffer obj, MemDataArray tmpBuf) {
      tmpBuf.write(0, obj, 0, obj.capacity());
      return tmpBuf;
   }

   @Override
   public void writeObject(ByteBuffer obj, MemDataArray buf) {
      buf.write(0, obj, 0, obj.capacity());
   }

   @Override
   public ByteBuffer readObject(MemDataArray buf) {
      ByteBuffer bb = ByteBuffer.allocate((int) buf.size());
      buf.read(0, bb, 0, bb.capacity());
      return bb;
   }

   @Override
   public Class<ByteBuffer> dataType() {
      return ByteBuffer.class;
   }
   
   @Override
   public String toString() {
      return toJson();
   }
}
