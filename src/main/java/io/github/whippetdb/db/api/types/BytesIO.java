package io.github.whippetdb.db.api.types;

import io.github.whippetdb.db.api.TypeIO;
import io.github.whippetdb.memory.api.MemArray;
import io.github.whippetdb.memory.api.MemDataArray;
import io.github.whippetdb.memory.basic.SimpleHeapDataBuffer;

/**
 * Variable-length byte array
 */

public class BytesIO implements TypeIO<byte[]> {
   private final Integer avg, max;
   
   /**
    * @param args - average, max
    */
   public BytesIO(Integer... args) {
      avg = args.length > 0? args[0]: null;
      max = args.length > 1? args[1]: null;
   }
   
   @Override
   public Integer avgSize() {
      return avg;
   }
   
   @Override
   public Integer maxSize() {
      return max;
   }
   
   @Override
   public boolean isApplicable(Object o) {
      return o instanceof byte[];
   }

   @Override
   public MemArray convert(byte[] obj, MemDataArray tmpBuf) {
      return new SimpleHeapDataBuffer(obj, obj.length);
   }

   @Override
   public void writeObject(byte[] obj, MemDataArray buf) {
      buf.write(0, obj, 0, obj.length);
   }

   @Override
   public byte[] readObject(MemDataArray buf) {
      byte[] bytes = new byte[(int) buf.size()];
      buf.read(0, bytes, 0, bytes.length);
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
