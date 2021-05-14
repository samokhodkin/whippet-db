package io.github.whippetdb.db.api;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import io.github.whippetdb.db.api.Db.Type;
import io.github.whippetdb.db.internal.DbUtil;
import io.github.whippetdb.db.internal.FixedDbUtil;
import io.github.whippetdb.db.internal.HybridDb;
import io.github.whippetdb.db.internal.VarDbUtil;
import io.github.whippetdb.memory.db.MemMap;
import io.github.whippetdb.util.Util;

/**
 * Entry point to the database API.
 * 
 * @param <K>
 * @param <V>
 */

public class DbBuilder<K,V> {
   private static final int DEFAULT_DATA_PAGE_SIZE = 24; // 2^n-8
   private static final int MAX_HYBRID_SIZE = 512; //
   private static final int MIN_ALIGN = 5;
   
   private final TypeIO<K> keyType;
   private final TypeIO<V> valueType;
   
   // input params
   private double speedVsCapacity = 0.5;
   private double journalSizeVsSpeed = 0.5;
   private double journalDurabilityVsSpeed = 0.5;
   private boolean journaling, autocommit = true, autoflush;
   private boolean synchronize;
   private int align;
   
   // inferred params
   private Db.Type type;
   private int maxListSize;
   private int keySize, valueSize;
   private boolean hybridKey, hybridValue; // use limited-size fields in a fixed-size record
   private boolean journalParamsOk;
   private int journalDirtySize, journalMaxSize;
   
   private Db db;
   
   /**
    * Build a database with unlimited variable-size key and value and no Map interface.
    * Use db() to obtain a raw interface.
    */
   public DbBuilder() {
      keyType = null;
      valueType = null;
      type = Type.Var;
      hybridKey = false;
      hybridValue = false;
      keySize = DEFAULT_DATA_PAGE_SIZE;
      valueSize = DEFAULT_DATA_PAGE_SIZE;
   }
   
   /**
    * Build a database with fixed- or hybrid-size fields and no Map interface.
    * 
    * @param keySize - key size, ether fixed or limited by this parameter
    * @param valueSize - value size, ether fixed or limited by this parameter
    * @param hybrid - key hybrid flag, value hybrid flag
    */
   public DbBuilder(int keySize, int valueSize, boolean... hybrid) {
      keyType = null;
      valueType = null;
      type = Type.Fixed;
      hybridKey = hybrid.length > 0 && hybrid[0];
      hybridValue = hybrid.length > 1 && hybrid[1];
      this.keySize = hybridKey? keySize + 1: keySize;
      this.valueSize = hybridValue? valueSize + 1: valueSize;
   }
   
   /**
    * Build a database a Map interface. Field sizes are defined by their TypeIO.
    * 
    * @param keyType - defines key type and size
    * @param valueType - defines value type and size
    */
   public DbBuilder(TypeIO<K> keyType, TypeIO<V> valueType) {
      Util.notNull(keyType, "keyType");
      Util.notNull(valueType, "valueType");
      this.keyType = keyType;
      this.valueType = valueType;
   }
   
   /**
    * Controls the tradeoff between the capacity and speed.
    * 
    * @param v - value in the range 0 (better speed) to 1 (better capacity), default is 0.5
    * @return - this
    */
   public DbBuilder<K,V> speedVsCapacity(float v) {
      if(v < 0 || v > 1) throw new IllegalArgumentException(v + ": must be in the range 0..1");
      speedVsCapacity = v;
      return this;
   }
   
   /**
    * Enable journaling.
    * In this mode the changes are persisted in an atomic way, guaranteeing the db integrity 
    * in the case of an application or system failure. This also causes a significant slowdown 
    * of the db. The inserts are slowed down by 20-100 times, depending on the next flags, 
    * and the reads by 3-5 times. 
    * 
    * In journaled mode the db is less compact, because the records are page aligned.
    * 
    * If the db was created in the journaled mode, it may be later opened in either journaled or plain mode.
    * If it was created in the plain mode, it may be later opened only in the plain mode.
    * 
    * The maximum observed insertion speed in journaling mode with SSD disk was around 260k op/sec.
    * 
    * @param b whether to enable journaling
    * @return this
    */
   public DbBuilder<K,V> journaling(boolean b) {
      journaling = b;
      return this;
   }
   
   /**
    * Chooses the max journal size from a meaningful range, in an exponential way,
    * where 0 corresponds to 2mb, 0.5 to 23mb, and 1 to 130mb. 
    * It controls the tradeoff between the average write speed and the journal RAM footprint. 
    * 
    * On one hand, the larger is this parameter, the more RAM is taken by the page table.
    * 
    * On the other hand, as this parameter increases, the db file is flushed to disk less often,
    * so the average write speed also increases.
    * 
    * In the experiments with SSD disk the write speed depended on this parameter non-monotonically,
    * reaching the maximum at 0.55, which is against the expectations and is not yet explained.   
    * Thus if you need the maximum write speed, it is recommended to choose this parameter experimentally 
    * from the range 0.5-1, or leave the default value untouched.
    * 
    * @param v - journal max size preference, the allowed range is 0..1, the default value is 0.5 
    * @return - this
    */
   public DbBuilder<K,V> journalSizeVsSpeed(float v) {
      if(v < 0 || v > 1) throw new IllegalArgumentException(v + ": must be in the range 0..1");
      journalSizeVsSpeed = v;
      return this;
   }
   
   /**
    * Controls the tradeoff between the average write speed and the durability of writes.
    * 
    * If the choice is maximum durability (at v=0), each write instantly becomes durable (persistent),
    * but the average write speed is low. The expected speed numbers are 1000 inserts/sec for SSD and 
    * 100 inserts/sec for HDD.
    * 
    * If the choice is maximum speed (at v=1), a number of last writes may be lost in the case 
    * of a system failure. In the case of an application failure the last writes are not at risk.  
    * 
    * In most situations the recommended value is 1 (max speed). 
    * 
    * In any case, the atomicity of writes is not affected, in journaling mode they are always atomic.
    * 
    * @param v - a preference in the range of 0..1, where 0 is for max durability, 1 for max speed, default is 0.5
    * @return - this
    */
   public DbBuilder<K,V> journalDurabilityVsSpeed(float v) {
      if(v < 0 || v > 1) throw new IllegalArgumentException(v + ": must be in the range 0..1");
      journalDurabilityVsSpeed = v;
      return this;
   }
   
   /**
    * Wheter to automatically commit each write in journaling mode. Enabled by default.
    * 
    * If you need manual control of commits, disable this parameter and use db().commit() 
    * and db().discard() as needed. 
    * 
    * Note that commits in Whippet are only about the atomicity of writes and only in journaling mode.
    * The durability may be forced by db().flush().
    * 
    * @param b - a choice; default is true
    * @return - this
    */ 
   public DbBuilder<K,V> autocommit(boolean b) {
      autocommit = b;
      return this;
   }
   
   /**
    * Whether to instantly persist each write.
    * Applies both to journaling and plain mode.
    *  
    * @param b - a choice; default is false
    * @return - this
    */
   public DbBuilder<K,V> autoflush(boolean b) {
      autoflush = b;
      return this;
   }
   
   
   /**
    * Whether to ensure a thread safety.
    * 
    * @param b - a choice; default is false
    * @return - this
    */
   public DbBuilder<K,V> synchronize(boolean b) {
      synchronize = b;
      return this;
   }
   
   /**
    * Creates the in-memory database.
    * 
    * @return - this
    */
   public DbBuilder<K,V> create() {
      inferDbParams();
      if(type == Type.Fixed) {
         db = FixedDbUtil.create(keySize, valueSize, maxListSize);
         if(hybridKey || hybridValue) db = new HybridDb(db, hybridKey, hybridValue);
      }
      else {
         db = VarDbUtil.create(keySize, valueSize, maxListSize);
      }
      if(synchronize) db = DbUtil.synchronize(db);
      return this;
   }
   
   /**
    * Creates the on-disk database over a specified file.
    * 
    * If the specified path is a directory, it creates and uses a temporary file in this directory.
    * This file is deleted when the db is closed or the application exits.
    * 
    * @path - the file or directory path
    * @return - this
    */
   public DbBuilder<K,V> create(String path) throws RuntimeIOException {
      inferDbParams();
      inferJounalParams();
      try {
         if(type == Type.Fixed) {
            db = journaling?
                  FixedDbUtil.createJournaled(
                        path, keySize, valueSize, maxListSize, align, journalDirtySize, journalMaxSize,
                        autocommit, autoflush):
                  FixedDbUtil.create(path, keySize, valueSize, maxListSize, align, autoflush);
            if(hybridKey || hybridValue) db = new HybridDb(db, hybridKey, hybridValue);
         }
         else {
            db = journaling?
               VarDbUtil.createJournaled(
                     path, keySize, valueSize, maxListSize, align, journalDirtySize, journalMaxSize,
                     autocommit, autoflush):
               VarDbUtil.create(path, keySize, valueSize, maxListSize, align, autoflush); 
         }
      }
      catch (IOException e) {
         throw new RuntimeIOException(e);
      }
      if(synchronize) db = DbUtil.synchronize(db);
      return this;
   }
   
   /**
    * Opens the existing on-disk database.
    * 
    * @path - the file path
    * @return - this
    */
   public DbBuilder<K,V> open(String path) throws RuntimeIOException {
      inferDbParams();
      inferJounalParams();
      try {
         if(type == Type.Fixed) {
            db = journaling?
                  FixedDbUtil.openJournaled(path, journalDirtySize, journalMaxSize, autocommit, autoflush) :
                  FixedDbUtil.open(path, autoflush);
            if(hybridKey || hybridValue) db = new HybridDb(db, hybridKey, hybridValue);
         }
         else {
            db = journaling?
               VarDbUtil.openJournaled(path, journalDirtySize, journalMaxSize, autocommit, autoflush) :
               VarDbUtil.open(path, autoflush); 
         }
      }
      catch (IOException e) {
         throw new RuntimeIOException(e);
      }
      if(synchronize) db = DbUtil.synchronize(db);
      return this;
   }
   
   /**
    * If the file exists tries to open it, otherwise creates a new on-disk database.
    * 
    * @path - the file path
    * @return - this
    */
   public DbBuilder<K,V> openOrCreate(String path) throws RuntimeIOException {
      return new File(path).exists()? open(path): create(path);
   }
   
   /**
    * Represents the database as a java.util.Map.
    * The database should be opened or created beforehand.
    * The builder must have a key and value data types.
    * @see io.github.whippetdb.db.api.DbBuilder#DbBuilder(TypeIO, TypeIO)
    * 
    * @return - a map
    */
   public Map<K,V> asMap() {
      return DbUtil.asMap(db, keyType, valueType);
   }
   
   /**
    * Return raw interface to the database.
    * Provides access to lifecycle methods (e.g. close()) and
    * advanced data access methods.
    * @see io.github.whippetdb.db.api.Db
    * 
    * @return the actual interface to the database
    */
   public Db db() {
      return db;
   }
   
   Type type() {
      return type;
   }
   
   TypeIO<K> keyType() {
      return keyType;
   }
   
   TypeIO<V> valueType() {
      return valueType;
   }
   
   private void inferDbParams() {
      inferMaxListSize();
      
      if(type != null) return;
      
      inferMaxListSize();
      
      if(keyType.size() != null && valueType.size() != null) {
         type = Type.Fixed;
         keySize = keyType.size();
         valueSize = valueType.size();
         hybridKey = hybridValue = false;
      }
      else if(
            keyType.maxSize() != null && keyType.maxSize() < 20 && valueType.maxSize() != null && 
            MemMap.recordSize(keyType.maxSize() + 1, valueType.maxSize() + 1) <= MAX_HYBRID_SIZE) {
         type = Type.Fixed;
         hybridKey = keyType.size() == null;
         hybridValue = valueType.size() == null;
         keySize = hybridKey ? keyType.maxSize() + 1: keyType.size();
         valueSize = hybridValue? valueType.maxSize() + 1: valueType.size();
      }
      else {
         type = Type.Var;
         keySize = adviceDataPageSize(keyType);
         valueSize = adviceDataPageSize(valueType);
         hybridKey = hybridValue = false;
      }
   }
   
   private void inferJounalParams() {
      inferDbParams();
      if(journalParamsOk) return;
      if(!journaling) {
         journalParamsOk = true;
         return;
      }
      
      // right now the journal's align must match the memspace align, see comments in SimpleJournalingMemSpace.
      // Thus we have to force the non-zero align for both. This may be fixed later.
      if(align <= 0) {
         if(type == Type.Fixed) {
            align = adviceAlign(MemMap.recordSize(keySize, valueSize));
         }
         else {
            align = adviceAlign(MemMap.recordSize(16, 16));
            align = Math.min(align, adviceAlign(valueSize + 8));
            align = Math.min(align, adviceAlign(keySize + 8));
         }
      }
      
      int expectedPagesPerNewRecord = type == Type.Fixed?
            // allocate record block + update 2 links
            (Util.ceil2(MemMap.recordSize(keySize, valueSize), 1<<align) >> align) + 2 :
            // allocate record block + 2 data pages + update 2 links
            (Util.ceil2(MemMap.recordSize(16, 16), 1<<align) >> align) + 4;
      int expectedBytesPerNewRecord = expectedPagesPerNewRecord << align;
      
      // min flush time on ssd is ~ 1 mls; 10_000 records per flush 
      // is 10^7 records per second, which is not reachable anyway.
      int maxMeaninfulDirtySize = expectedBytesPerNewRecord * 10_000;
      
      journalDirtySize = (int) (maxMeaninfulDirtySize * adjust(journalDurabilityVsSpeed));
      journalMaxSize = (int) ((128<<20) * adjust(journalSizeVsSpeed) + (2<<20));
   }
   
   private void inferMaxListSize() {
      // empirical formula, 0 -> 4, 0.5 -> 11, 1 -> 22
      // maxListSize=10 should be avoided as hell! The reason is that for decimal strings,
      // which are quite common data type, the maxListSize=10 causes capacity disaster 
      if(maxListSize == 0) maxListSize = (int)Math.round(4 + 18 * Math.pow(speedVsCapacity, 1.41));
   }
   
   private int adviceDataPageSize(TypeIO<?> type) {
      Integer v;
      if((v = type.size()) != null) return v;
      if((v = type.avgSize()) != null) return v;
      return DEFAULT_DATA_PAGE_SIZE;
   }
   
   @Override
   public String toString() {
      return "{\r\n" + 
            "  keyType: " + keyType + ",\r\n" +
            "  valueType: " + valueType + ",\r\n" +
            "  dbType: " + type + ",\r\n" +
            "  key: {size=" + keySize + ", hybrid=" + hybridKey + "},\r\n" + 
            "  value: {size=" + valueSize + ", hybrid=" + hybridValue + "},\r\n" + 
            "  maxListSize: " + maxListSize + ",\r\n" +
            "  synchronize: " + synchronize + ",\r\n" +
            "  journal: {enabled=" + journaling + ", dirtySize=" + journalDirtySize + ", maxSize=" + journalMaxSize + "},\r\n" + 
            "  align: " + align + "\r\n}";
   }
   
   private static int adviceAlign(int size) {
      int pageSize = Util.ceil2(size);
      while((pageSize - (size % pageSize)) > 0.15*size) pageSize >>= 1;
      return Math.max(MIN_ALIGN, Util.log2(pageSize));
   }
   
   private static double adjust(double v) {
      return (Math.exp(Math.pow(v, 2)) - 1) / (Math.E - 1);
   }
}