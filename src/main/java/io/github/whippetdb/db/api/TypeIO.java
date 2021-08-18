package io.github.whippetdb.db.api;

import io.github.whippetdb.memory.api.MemArray;
import io.github.whippetdb.memory.api.MemDataArray;
import io.github.whippetdb.memory.api.MemDataBuffer;

/**
 * Serialize/deserialize objects.
 * When implemeting, be careful with size(), maxSize(), avgSize(),
 * they should be correct and consistent, otherwise the db engine may fail.
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
    * Convert an object to bytes in memory.
    * This is specifically intended converting db keys, so the returned memory chunck 
    * will be used only for reading.
    * Implementation may choose between creating new instance of memory
    * or writing to the supplied buffer, which one is faster.
    * If the current type is variable-size (that is, size() returns null), the size of 
    * the returned memory chunk should be set through casting tmpBuf as MemDataBuffer.
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
   
   /**
    * @return If the type is fixed-size, return the size, otherwise return null. 
    */
   default Integer size() {
      return null;
   }
   
   /**
    * @return max size in bytes, if known, or null; if the type is fixed-size, return the size.
    */
   default Integer maxSize() {
      return size();
   }
   
   /**
    * @return average size in bytes, if known, or null; if the type is fixed-size, return the size. 
    */
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
