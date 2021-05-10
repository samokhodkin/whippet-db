package io.github.whippetdb.db.api.types;

import io.github.whippetdb.db.api.TypeIO;
import io.github.whippetdb.memory.api.MemArray;
import io.github.whippetdb.memory.api.MemDataArray;
import io.github.whippetdb.memory.basic.StringWrapper;

public class CharsIO implements TypeIO<CharSequence> {
   private final Integer avgSize;
   private final Integer maxSize;
   
   /**
    * @param args - expected [max size] or [average size, max size], may be nulls
    */
   public CharsIO(Integer... args) {
      if(args.length == 0) {
         maxSize = avgSize = null;
      }
      else if(args.length == 1) {
         maxSize = args[0] != null? args[0]<<1: null;
         avgSize = null;
      }
      else {
         avgSize = args[0] != null? args[0]<<1: null;
         maxSize = args[1] != null? args[1]<<1: null;
      }
   }
   
   public Integer maxSize() {
      return maxSize;
   }
   
   public Integer avgSize() {
      return avgSize;
   }

   public MemArray convert(CharSequence obj, MemDataArray tmpBuf) {
      return new StringWrapper(obj);
   }

   public void writeObject(CharSequence obj, MemDataArray buf) {
      buf.write(0, obj);
      setSize(buf, obj.length() << 1);
   }
   
   public CharSequence readObject(MemDataArray buf) {
      return buf == null? null : buf.readString(0);
   }

   @Override
   public boolean isApplicable(Object o) {
      return o instanceof CharSequence;
   }

   @Override
   public Class<CharSequence> dataType() {
      return CharSequence.class;
   }
   
   @Override
   public String toString() {
      return toJson();
   }
}
