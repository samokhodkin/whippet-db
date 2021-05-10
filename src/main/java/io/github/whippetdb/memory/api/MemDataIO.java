package io.github.whippetdb.memory.api;

/**
 * MemIO with added methods for integer types. It is byte order-aware.
 * Default implementaions are slow, implementations should override.
 */
public interface MemDataIO extends MemIO{
   default int readShort(long addr) {
      return Bytes.readShort(this, addr);
   }
   default int readInt(long addr) {
      return Bytes.readInt(this, addr);
   }
   default long readLong(long addr) {
      return Bytes.readLong(this, addr);
   }
   default long readBytes(long addr, int len) {
      return Bytes.readBytes(this, addr, len);
   }
   default long readBytes(long addr, int len, int from, int to) {
      return Bytes.readBytes(this, addr, len, from, to);
   }
   
   default void writeShort(long addr, int v) {
      Bytes.writeShort(this, addr, v);
   }
   default void writeInt(long addr, int v) {
      Bytes.writeInt(this, addr, v);
   }
   default void writeLong(long addr, long v) {
      Bytes.writeLong(this, addr, v);
   }
   default void writeBytes(long addr, long v, int len) {
      Bytes.writeBytes(this, addr, v, len);
   }
   default void writeBytes(long addr, long v, int len, int from, int to) {
      Bytes.writeBytes(this, addr, v, len, from, to);
   }
   
   default void fillLong(long addr, int len, long v) {
      if(len < 8) {
         writeBytes(addr, v, len);
         return;
      }
      writeLong(addr, v);
      int written = 8;
      if(len > 16) {
         writeLong(addr + 8, v);
         written = 16;
      }
      while(len-written > written) {
         write(addr+written, this, addr, written);
         written <<= 1;
      }
      write(addr+written, this, addr, len-written);
   }
   
   default boolean equals(long addr, MemDataIO other, long otherAddr, int len) {
      int compared = 0;
      while((len-compared) >= 8) {
         if(readLong(addr+compared) != other.readLong(otherAddr+compared)) return false;
         compared += 8;
      }
      return len == compared? true: readBytes(addr + compared, len - compared) == other.readBytes(otherAddr + compared, len - compared);
   }
}
