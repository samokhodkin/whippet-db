package io.github.whippetdb.db.api;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

import io.github.whippetdb.memory.api.MemArray;
import io.github.whippetdb.memory.api.MemDataArray;
import io.github.whippetdb.memory.api.MemIO;

/**
 * Raw interface to a database.
 * Defines CRUD operations on key-value pairs and lifecycle commands.
 */
public interface Db {
   public enum Type {
      Fixed, Var;
   }
   
   /**
    * Interface to receive a result of seek() operation and optionally convert it to an object.
    * @param <T> a type to convert to.
    */
   public interface SeekHandler<T> {
      /**
       * @param data - a value data; variable-size value may be cast to MemDataBuffer (see valueSize())
       * @param delete - deletes this key-value pair
       * @return - value converted to object
       */
      public T run(MemDataArray data, Runnable delete);
   }
   
   /**
    * Interface to receive a result of put() operation and optionally convert it to an object.
    * @param <T> a type to convert to.
    */
   public interface PutHandler<T> {
      /**
       * @param created - if a record was just created
       * @param data - a value data; variable-size value may be cast to MemDataBuffer
       * @param delete - deletes this key-value pair
       * @return - value converted to object
       */
      public T run(Boolean created, MemDataArray data, Runnable delete);
   }
   
   /**
    * Interface to receive a result of scan() operation.
    */
   public interface ScanHandler {
      /**
       * @return whether to stop the scanning
       */
      public boolean run(MemDataArray key, MemDataArray data, Runnable delete);
   }
   
   /**
    * Key size, non-null indicates that this db has fixed-size keys.
    * If present, keyLen parameter in seek()/put()/delete() is ignored.
    */
   public Integer keySize();
   
   /**
    * Max key size, if known.
    * If present, keyLen parameter in seek()/put()/delete() is checked against it.
    */
   default Integer maxKeySize() {
      return keySize();
   }
   
   /**
    * Value size, non-null indicates that this db has fixed-size values.
    */
   public Integer valueSize();
   
   /**
    * Max value size, if known.
    * If present, operations on value in seek()/put() are checked against it.
    */
   default Integer maxValueSize() {
      return keySize();
   }
   
   /**
    * Visit a value for a key.
    * If the key doesn't exist, a handler will receive null arguments.
    * A handler is allowed to change the value on the fly.
    * 
    * If the db has fixed-size keys:
    *   - keyLen is ignored (the db will use the size it knows).
    * 
    * If the db has fixed-size values:
    *   - reading/writing is checked against this size,
    *   - initially value is filled by zeros, all changes are persistent.
    * 
    * If the db has variable-size keys:
    *   - keyLen is respected,
    *   - if maxKeySize is present, keyLen is checked against it.
    *   
    * If the db has variable-size values:
    *   - value may be cast to MemDataBuffer and resized
    *   - reading/writing follow the rules for MemDataBuffer
    *   - if maxValueSize is present, reading/writing/resizing is checked against it
    * 
    * @param key - key memory
    * @param keyOff - key start
    * @param keyLen - key length
    * @param handler - value handler
    */
   public <T> T seek(MemIO key, long keyOff, int keyLen, SeekHandler<T> handler);
   
   /**
    * Insert or visit a value for a key.
    * The boolean parameter of a handler indicates whether the key was just inserted. 
    * The value is always non-null.
    * A handler is allowed to change the value on the fly.
    * 
    * If the db has fixed-size keys:
    *   - keyLen is ignored (the db will use the size it knows).
    * 
    * If the db has fixed-size values:
    *   - reading/writing is checked against this size,
    *   - initially value is filled by zeros, all changes are persistent.
    * 
    * If the db has variable-size keys:
    *   - keyLen is respected,
    *   - if maxKeySize is present, keyLen is checked against it.
    *   
    * If the db has variable-size values:
    *   - value may be cast to MemDataBuffer and resized,
    *   - reading/writing follow the rules for MemDataBuffer,
    *   - initially value is empty (zero-size), all changes are persistent,
    *   - if maxValueSize is present, reading/writing/resizing is checked against it.
    * 
    * @param key - key memory
    * @param keyOff - actual key start
    * @param keyLen - actual key length
    * @param consumer - visitor
    */
   public <T> T put(MemIO key, long keyOff, int keyLen, PutHandler<T> handler);
   
   public void scan(ScanHandler handler);
   
   /**
    * Mark the end of transaction.
    * In general, the transaction is NOT immediately persisted.
    * The immediate durability may be enabled by DbBuilder flags.  
    */
   public void commit();
   
   /**
    * Discard changes after last commit
    */
   public void discard();
   
   /**
    * Immediately persist the commited transations.   
    */
   public void flush();
   
   /**
    * Close the backing file.
    * If journaling is enabled, persist all transactions 
    * to the main file and delete the transaction log.    
    */
   public void close();
   
   /**
    * Total allocated memory for this db, file or RAM.
    */
   public long allocatedSize();
   
   /*
    * Utility methods
    */
   
   default <T> T seek(MemArray key, SeekHandler<T> consumer) {
      return seek(key, 0L, (int)key.size(), consumer);
   }
   default void seek(MemArray key, Consumer<MemDataArray> consumer) {
      seek(key, 0, (int) key.size(), (value, delete) -> {
         consumer.accept(value);
         return (Void)null;
      });
   }
   
   default <T> T put(MemArray key, PutHandler<T> consumer) {
      return put(key, 0L, (int)key.size(), consumer);
   }
   default void put(MemArray key, BiConsumer<Boolean, MemDataArray> consumer) {
      put(key, 0L, (int)key.size(), (created, value, delete) -> {
         consumer.accept(created, value);
         return (Void)null;
      });
   }
   
   default void delete(MemIO key, long keyOff, int keyLen) {
      seek(key, keyOff, keyLen, (v, del) -> {
         if(v != null) del.run();
         return null;
      });
   }
   
   default void clear() {
      scan((key, data, del) -> {
         del.run();
         return false;
      });
   }
}
