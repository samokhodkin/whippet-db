package io.github.whippetdb.db.api;

import io.github.whippetdb.memory.api.MemArray;
import io.github.whippetdb.memory.api.MemDataArray;
import io.github.whippetdb.memory.api.MemDataBuffer;

/**
 * Serialize/deserialize objects
 */
public interface TypeIO<T> {
   
   public Class<T> dataType();
   
   /**
    * Reccommended to override using instanceof!
    */
   default boolean isApplicable(Object o) {
      return dataType().isInstance(o);
   }
   
   /**
    * Convert object to memory.
    * Implementation may choose between creating new instance of memory
    * and writing to existing buffer, which one is faster.
    * This is intended for representing a db key as memory chunk.
    * If the type is variable-size (size() returns null), the 
    * size should be set through casting tmpBuf as MemDataBuffer.
    * 
    * @param obj the object to convert.
    * @param tmpBuf the buffer that may be written to and returned as the result.
    * @return memory
    */
   public MemArray convert(T obj, MemDataArray tmpBuf);
   
   /**
    * Write object to memory.
    * If the type is var-size, the size should be set through casting the buf as MemDataBuffer.
    * 
    * @param obj
    * @param buf
    */
   public void writeObject(T obj, MemDataArray buf);
   
   /**
    * Read object from memory.
    * 
    * @param obj
    * @param buf
    */
   public T readObject(MemDataArray buf);
   
   // type has fixed size in memory
   default Integer size() {
      return null;
   }
   
   default Integer maxSize() {
      return size();
   }
   
   default Integer avgSize() {
      return size();
   }
   
   default void setSize(MemDataArray buf, long size) {
      ((MemDataBuffer)buf).setSize(size);
   }
   
   default String toJson() {
      return "{name: " + getClass().getSimpleName() + 
            ", dataType: " + dataType().getSimpleName() + 
            ", size: " + size() + 
            ", avg: " + avgSize() + 
            ", max: " + maxSize() + "}";
   }
}
