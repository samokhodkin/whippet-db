package io.github.whippetdb.db.internal;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import io.github.whippetdb.db.api.Db;
import io.github.whippetdb.db.api.TypeIO;
import io.github.whippetdb.memory.api.MemDataBuffer;
import io.github.whippetdb.memory.api.MemIO;
import io.github.whippetdb.memory.basic.SimpleDirectDataBuffer;

public class DbUtil {
   public static Db synchronize(Db db) {
      return new Db() {
         public Integer keySize() {
            return db.keySize();
         }

         public Integer valueSize() {
            return db.valueSize();
         }
         
         @Override
         public Integer maxKeySize() {
            return db.maxKeySize();
         }
         
         @Override
         public Integer maxValueSize() {
            return db.maxValueSize();
         }

         @Override
         public synchronized <T> T seek(MemIO key, long keyOff, int keyLen, SeekHandler<T> handler) {
            return db.seek(key, keyOff, keyLen, handler);
         }

         @Override
         public synchronized <T> T put(MemIO key, long keyOff, int keyLen, PutHandler<T> handler) {
            return db.put(key, keyOff, keyLen, handler);
         }

         @Override
         public synchronized void scan(ScanHandler handler) {
            db.scan(handler);
         }

         @Override
         public synchronized void commit() {
            db.commit();
         }

         @Override
         public synchronized void discard() {
            db.discard();
         }

         @Override
         public synchronized void flush() {
            db.flush();
         }

         @Override
         public synchronized void close() {
            db.close();
         }

         @Override
         public synchronized long allocatedSize() {
            return db.allocatedSize();
         }
         
      };
   }
   
   public static <K,V> Map<K,V> asMap(Db db, TypeIO<K> keyType, TypeIO<V> valueType) {
      return new Map<K,V>(){
         private MemDataBuffer keyBuffer = new SimpleDirectDataBuffer();
         {
            if(db.keySize() != null) keyBuffer.setSize(db.keySize());
            else if(db.maxKeySize() != null) keyBuffer.setSize(db.maxKeySize());
         }
         
         public int size() {
            int[] size = new int[1];
            db.scan((k,v,del) -> {
               size[0]++;
               return false;
            });
            return size[0];
         }

         @Override
         public boolean isEmpty() {
            boolean[] empty = new boolean[]{true};
            db.scan((k,v,del) -> {
               empty[0] = false;
               return true;
            });
            return empty[0];
         }

         @SuppressWarnings("unchecked")
         @Override
         public boolean containsKey(Object key) {
            if(!keyType.isApplicable(key)) return false; 
            return db.seek(keyType.convert((K)key, keyBuffer), (data,del) -> {
               return data != null;
            });
         }

         @Override
         public boolean containsValue(Object value) {
            boolean[] ok = new boolean[1];
            db.scan((k, data, del) -> {
               Object existing = valueType.readObject(data);
               if(Objects.equals(value, existing)) {
                  return ok[0] = true;
               }
               return false;
            });
            return ok[0];
         }

         @SuppressWarnings("unchecked")
         @Override
         public V get(Object key) {
            if(!keyType.isApplicable(key)) return null; 
            return db.seek(keyType.convert((K)key, keyBuffer), (data, del) -> valueType.readObject(data));
         }

         @Override
         public V put(K key, V value) {
            return db.put(keyType.convert((K)key, keyBuffer), (created, data, del) -> {
               V old = created? null: valueType.readObject(data);
               valueType.writeObject(value, data);
               return old;
            });
         }

         @SuppressWarnings("unchecked")
         @Override
         public V remove(Object key) {
            return db.seek(keyType.convert((K)key, keyBuffer), (data, del) -> {
               if(data == null) return null;
               V old = valueType.readObject(data);
               del.run();
               return old;
            });
         }

         @Override
         public void putAll(Map<? extends K, ? extends V> m) {
            m.forEach((k, v) -> {
               db.put(keyType.convert(k, keyBuffer), (created, data) ->  valueType.writeObject(v, data));
            });
         }

         @Override
         public void clear() {
            db.clear();
         }

         @Override
         public Set<K> keySet() {
            HashSet<K> set = new HashSet<>();
            db.scan((k,v,del) -> {
               set.add(keyType.readObject(k));
               return false;
            });
            return set;
         }

         @Override
         public Collection<V> values() {
            HashSet<V> set = new HashSet<>();
            db.scan((k,v,del) -> {
               set.add(valueType.readObject(v));
               return false;
            });
            return set;
         }

         @Override
         public Set<java.util.Map.Entry<K, V>> entrySet() {
            HashSet<Map.Entry<K, V>> set = new HashSet<>();
            db.scan((k,v,del) -> {
               K key = keyType.readObject(k);
               V val = valueType.readObject(v);
               set.add(new Map.Entry<K,V>(){
                  public K getKey() {
                     return key;
                  }
                  public V getValue() {
                     return val;
                  }
                  public V setValue(V value) {
                     return null;
                  }
                  public String toString() {
                     return key + "->" + val;
                  }
               });
               return false;
            });
            return set;
         }
      };
   }
}
