package io.github.whippetdb.memory.db;

import static io.github.whippetdb.memory.db.CacheManager.*;

import java.util.function.Supplier;

import io.github.whippetdb.memory.api.MemDataArray;
import io.github.whippetdb.memory.api.MemDataSpace;
import io.github.whippetdb.memory.api.MemIO;
import io.github.whippetdb.memory.basic.ProxyDataArray;
import io.github.whippetdb.util.LongList;
import io.github.whippetdb.util.Util;

public class MemMap{
   static final int LOG_ADDR_SIZE = 3;
   static final int ADDR_SIZE = 8;
   static final long NULL = 0L;
   
   /* base block: 
    *   entryAddr(8)+tableCache(8)+dataCache(8)+
    *   keySize(4)+valueSize(4)+maxListSize(4)
    */
   public static final int ENTRY_ADDR_OFF=0;
   public static final int DATA_CACHE_OFF=ADDR_SIZE;
   public static final int TABLE_CACHE_OFF=DATA_CACHE_OFF+ADDR_SIZE;
   public static final int KEY_SIZE_OFF=TABLE_CACHE_OFF+ADDR_SIZE;
   public static final int VALUE_SIZE_OFF=KEY_SIZE_OFF+4;
   public static final int MAX_LIST_SIZE_OFF=VALUE_SIZE_OFF+4;
   public static final int BASE_BLOCK_SIZE=MAX_LIST_SIZE_OFF+4;
   
   //any type block:
   public static final int TYPE_OFF=0;
   public static final int TYPE_SIZE=1;
   public static final int PARENT_ADDR_OFF=TYPE_OFF+TYPE_SIZE;
   public static final int PARENT_OFF_OFF=PARENT_ADDR_OFF+8;
   
   //data block:  type(1)+parentAddr(8)+parentOff(2)+nextAddr(8)+key(keySize)+value(valueSize)
   public static final int NEXT_ADDR_OFF=PARENT_OFF_OFF+2;//11
   public static final int KEY_OFF=NEXT_ADDR_OFF+8; 
   
   //table block: type(1)+parentAddr(8)+parentOff(2)+tableSize(2)+table(8*256)
   public static final int TABLE_SIZE_OFF=NEXT_ADDR_OFF;
   public static final int TABLE_OFF=TABLE_SIZE_OFF+2; //13
   public static final int TABLE_BLOCK_SIZE=TABLE_OFF+8*256;
   
   public static final int TYPE_FREE=0; //see MemDbUtil.releaseBlock()
   public static final int TYPE_DATA=TYPE_FREE+1;
   public static final int TYPE_TABLE=TYPE_DATA+1;
   
   public final long baseAddr;
   
   public final int keySize, valueSize;
   public final long entryAddr;
   
   private final long dataCacheAddr;
   private final long tableCacheAddr;
   private final int valueOff;
   private final int dataBlockSize;
   private final int maxListSize;
   
   private int size=0;
   
   //create new db in ms
   public MemMap(MemDataSpace ms, int keySize, int valueSize, int maxListSize){
      baseAddr=ms.allocate(BASE_BLOCK_SIZE, true);
      tableCacheAddr=baseAddr+TABLE_CACHE_OFF;
      dataCacheAddr=baseAddr+DATA_CACHE_OFF;
      entryAddr=ms.allocate(TABLE_BLOCK_SIZE, true);
      this.keySize=keySize;
      this.valueSize=valueSize;
      this.maxListSize=maxListSize;
      ms.writeByte(entryAddr+TYPE_OFF, TYPE_TABLE);
      ms.writeLong(baseAddr+ENTRY_ADDR_OFF,entryAddr);
      ms.writeInt(baseAddr+KEY_SIZE_OFF,keySize);
      ms.writeInt(baseAddr+VALUE_SIZE_OFF,valueSize);
      ms.writeInt(baseAddr+MAX_LIST_SIZE_OFF,maxListSize);
      valueOff=KEY_OFF+keySize;
      dataBlockSize=valueOff+valueSize;
   }
   
   //load existing db
   public MemMap(MemDataSpace ms, long baseAddr){
      this.baseAddr=baseAddr;
      tableCacheAddr=baseAddr+TABLE_CACHE_OFF;
      dataCacheAddr=baseAddr+DATA_CACHE_OFF;
      entryAddr=ms.readLong(baseAddr+ENTRY_ADDR_OFF);
      keySize=ms.readInt(baseAddr+KEY_SIZE_OFF);
      valueSize=ms.readInt(baseAddr+VALUE_SIZE_OFF);
      maxListSize=ms.readInt(baseAddr+MAX_LIST_SIZE_OFF);
      valueOff=KEY_OFF+keySize;
      dataBlockSize=valueOff+valueSize;
   }
   
   public static int recordSize(int keySize, int valueSize) {
      return KEY_OFF + keySize + valueSize;
   }
   
   public class Cursor{
      public final MemDataSpace ms;
      protected long currentAddr;
      
      public final MemDataArray key, value;
      
      protected class Region extends ProxyDataArray{
         private final int off, size;
         
         Region(MemDataSpace ms, int off, int size){
            parent=ms;
            this.off = off;
            this.size = size;
         }
         @Override
         public long size() {
            return size;
         }
         @Override
         protected long offset() {
            return currentAddr + off;
         }
         @Override
         protected void checkRead(long addr, int len) {
            if(currentAddr==NULL) throw new RuntimeException("Reading from NULL");
            Util.checkRead(addr, len, size);
         }
         @Override
         protected void checkWrite(long addr, int len) {
            if(currentAddr==NULL) throw new UnsupportedOperationException("Writing to NULL");
            Util.checkWrite(addr, len, size);
         }
         
         public String toString_d() {
            return "Region{off="+off+", size="+size+", offset()->"+offset()+", value="+ms.toString(offset(), size)+", ms="+ms+"}";
         }
      }
      
      public Cursor(MemDataSpace ms){
         this.ms=ms;
         key=new Region(ms, KEY_OFF, keySize){
            @Override //readonly
            protected void checkWrite(long addr, int len){
               throw new UnsupportedOperationException();
            }
         };
         value=new Region(ms, valueOff, valueSize);
      }
      
      public boolean seek(MemIO key, long off){
         return seek(entryAddr, 0, key, off);
      }
      
      public boolean put(MemIO key, long off){
//if(DEBUG) System.out.println("MemMap.put("+key+","+off+")");
         return put2table(entryAddr, 0, key, off);
      }
      
      public void addValue(){
         if(currentAddr==NULL) throw new IllegalStateException("not positioned");
         long newAddr=newDataBlock();
         ms.writeLong(newAddr+PARENT_ADDR_OFF, currentAddr);
         ms.writeShort(newAddr+PARENT_OFF_OFF, NEXT_ADDR_OFF);
         ms.write(newAddr+KEY_OFF, key, 0, keySize);
         ms.writeLong(newAddr+NEXT_ADDR_OFF, ms.readLong(currentAddr+NEXT_ADDR_OFF));
         ms.writeLong(currentAddr+NEXT_ADDR_OFF, newAddr);
         setAddress(newAddr);
      }
      
      public boolean nextValue(){
         if(currentAddr==NULL) throw new IllegalStateException("not positioned");
         long next=ms.readLong(currentAddr+NEXT_ADDR_OFF);
         if(next==NULL || !ms.equals(next+KEY_OFF, key, 0, keySize)) return false;
         setAddress(next);
         return true;
      }
      
      public void delete(){
         if(currentAddr==NULL) throw new IllegalStateException("not positioned");
         delete(currentAddr);
         setAddress(NULL);
      }
      
      public void scan(Supplier<Boolean> feed){
         scan(entryAddr, feed);
      }
      
      protected void delete(long addr){
         long next=ms.readLong(addr+NEXT_ADDR_OFF);
         long prev=ms.readLong(addr+PARENT_ADDR_OFF);
         int prevOff=ms.readShort(addr+PARENT_OFF_OFF);
         if(next!=NULL){
            ms.writeLong(prev+prevOff,next);
            ms.writeLong(next+PARENT_ADDR_OFF,prev);
            ms.writeShort(next+PARENT_OFF_OFF,prevOff);
         }
         else{
            if(prevOff>=TABLE_OFF) removeTableEntry(prev,prevOff);
            else ms.writeLong(prev+prevOff,NULL);
         }
         releaseDataBlock(addr);
         size--;
      }
      
      private final boolean seek(long addr, int level, MemIO key, long keyOff){
//System.out.println("seek("+addr+","+level+","+key+","+keyOff+")");
         int type=ms.readByte(addr+TYPE_OFF);
         switch(type){
            case TYPE_DATA:
               while(addr!=NULL){
                  if(ms.equals(addr+KEY_OFF+level,key,keyOff+level,keySize-level)){
                     setAddress(addr);
                     return true;
                  }
                  addr=ms.readLong(addr+NEXT_ADDR_OFF);
               }
               return false;
            case TYPE_TABLE:
               int tableKey=key.readByte(keyOff+level);
               //int off=TABLE_OFF+tableKey*ADDR_SIZE;
               int off=TABLE_OFF+(tableKey<<LOG_ADDR_SIZE);
               long nextAddr=ms.readLong(addr+off);
               if(nextAddr==NULL) return false;
               return seek(nextAddr,level+1,key,keyOff);
         }
         throw new Error("bad type: "+type+" at block #"+addr);
      }
      
      final void scan(long addr, final Supplier<Boolean> feed){
         final int type=ms.readByte(addr+TYPE_OFF);
         switch(type){
            case TYPE_DATA:{
               while(addr!=NULL){
                  setAddress(addr);
                  if(feed.get()) return;
                  addr=ms.readLong(addr+NEXT_ADDR_OFF);
               }
               return;
            }
            case TYPE_TABLE:{
               int p=TABLE_OFF;
               for(int i=0;i<256;p+=ADDR_SIZE,i++){
                  long nextAddr=ms.readLong(addr+p);
                  if(nextAddr!=NULL) scan(nextAddr, feed);
               }
               return;
            }
         }
      }
      
      protected void setAddress(long addr){
         currentAddr=addr;
      }
      
      //session-wide balance
      public int size(){
         return size;
      }

      private final boolean put2table(
         long addr, int level, MemIO key, long keyOff
      ){
         int tableKey=key.readByte(keyOff+level);
         //int off=TABLE_OFF+tableKey*ADDR_SIZE;
         int off=TABLE_OFF+(tableKey<<LOG_ADDR_SIZE);
         long nextAddr=ms.readLong(addr+off);
         boolean b=put(nextAddr, level+1, addr, off, key, keyOff);
         if(nextAddr==NULL){
            ms.writeShort(addr+TABLE_SIZE_OFF, ms.readShort(addr+TABLE_SIZE_OFF)+1);
         }
         return b;
      }
      
      private final boolean put(
         long addr, int level, long parent, int parentOff, MemIO key, long keyOff
      ){
         if(addr==NULL){
            addr=newDataBlock();
            ms.writeLong(parent+parentOff,addr);
            ms.writeLong(addr+PARENT_ADDR_OFF,parent);
            ms.writeShort(addr+PARENT_OFF_OFF,parentOff);
            ms.writeLong(addr+NEXT_ADDR_OFF, NULL);
            ms.write(addr+KEY_OFF,key,keyOff,keySize);
            setAddress(addr);
            size++;
            return true;
         }
         int type=ms.readByte(addr+TYPE_OFF);
         switch(type){
            case TYPE_DATA:
               int n=0;
               final long first=addr;
               long last=addr;
               while(addr!=NULL){
                  if(ms.equals(addr+KEY_OFF+level,key,keyOff+level,keySize-level)){
                     setAddress(addr);
                     return false;
                  }
                  last=addr;
                  addr=ms.readLong(addr+NEXT_ADDR_OFF);
                  n++;
               }
               //list is short, add new list item
               if(n<maxListSize) return put(
                  NULL, level, last, NEXT_ADDR_OFF, key, keyOff
               );
               
               //otherwise allocate new table, map and relink old items
               addr=list2table(first, level);
               ms.writeLong(parent+parentOff,addr);
               ms.writeLong(addr+PARENT_ADDR_OFF,parent);
               ms.writeShort(addr+PARENT_OFF_OFF,parentOff);
               //go on, put to table
            
            case TYPE_TABLE:
               return put2table(addr, level, key, keyOff);
         }
         throw new IllegalStateException("Bad type: "+type+" at "+addr);
      }
      
      private final void removeTableEntry(long table, int off){
         int size=ms.readShort(table+TABLE_SIZE_OFF);
         if(size<=0) throw new Error("attemp to remove entry from the empty table");
         if(size>1 || table==entryAddr){
            ms.writeLong(table+off, NULL);
            ms.writeShort(table+TABLE_SIZE_OFF, size-1);
            return;
         }
         long prev=ms.readLong(table+PARENT_ADDR_OFF);
         int prevOff=ms.readShort(table+PARENT_OFF_OFF);
         releaseTableBlock(table);
         removeTableEntry(prev,prevOff);
      }
      
      private final long list2table(long addr, int level){
//if(DEBUG) System.out.println("list2table("+addr+","+level+")");
         final long table=newTableBlock();
//if(DEBUG) System.out.println("  table="+blockToString(table,false));
         int n=0; //number of assigned keys
         while(addr!=NULL){
//if(DEBUG) System.out.println("  addr="+addr+", block="+blockToString(addr,false));
            long next=ms.readLong(addr+NEXT_ADDR_OFF);//memorize the next in list
//if(DEBUG) System.out.println("  next="+next);
            int tableKey=ms.readByte(addr+KEY_OFF+level); //table key
            //int off=TABLE_OFF+tableKey*ADDR_SIZE;
            int off=TABLE_OFF+(tableKey<<LOG_ADDR_SIZE);
            long tail=ms.readLong(table+off);//what's already at that key?
//if(DEBUG) System.out.println("  tail="+tail);
            //make back reference
            if(tail!=NULL){
               ms.writeLong(tail+PARENT_ADDR_OFF,addr);
               ms.writeShort(tail+PARENT_OFF_OFF,NEXT_ADDR_OFF);
//if(DEBUG) System.out.println("  tail -> "+blockToString(tail,false));
            }
            else n++; //new key
            
            //make forward reference
            ms.writeLong(addr+NEXT_ADDR_OFF,tail);
            //insert to table
            ms.writeLong(table+off,addr);
            //make back reference
            ms.writeLong(addr+PARENT_ADDR_OFF,table);
            ms.writeShort(addr+PARENT_OFF_OFF,off);
//if(DEBUG) System.out.println("  block -> "+blockToString(addr,false));
            
            addr=next;
         }
         ms.writeShort(table+TABLE_SIZE_OFF, n);
         return table;
      }
      
      protected long newDataBlock(){
         long addr=allocateBlock(ms, dataCacheAddr, dataBlockSize, false);
         ms.writeByte(addr+TYPE_OFF, TYPE_DATA);
         return addr;
      }
      
      protected void releaseDataBlock(long addr){
         releaseBlock(ms, dataCacheAddr, addr);
      }
      
      protected long newTableBlock(){
         long addr=allocateBlock(ms, tableCacheAddr, TABLE_BLOCK_SIZE, true);
         ms.writeByte(addr+TYPE_OFF, TYPE_TABLE);
         return addr;
      }
      
      protected void releaseTableBlock(long addr){
         releaseBlock(ms, tableCacheAddr, addr);
      }
      
      public String toString(){
         final StringBuilder sb=new StringBuilder("{");
         scan(() -> {
            if(sb.length()>1) sb.append(", ");
            sb.append(key.toString()).append("=").append(value.toString());
            return false;
         });
         return sb.append("}").toString();
      }
      
      public String toString_d(){
         return toString_d(false);
      }
      
      public String toString_d(boolean blocks){
         return toString_d(blocks, false);
      }
      public String toString_d(boolean blocks, boolean cache){
         StringBuilder sb=new StringBuilder(
            "MemMap(\n"+
               "  keySize="+keySize+"\n"+
               "  valueSize="+valueSize+"\n"+
               "  baseAddr="+baseAddr+"\n"+
               "  entryAddr="+entryAddr+"\n"+
               "  valueOff="+valueOff+"\n"+
               "  dataBlockSize="+dataBlockSize+"\n"+
               "  maxListSize="+maxListSize+"\n"+
               "  NULL="+NULL+"\n"+
               "  size="+size()+"\n"+
               "  data size="+ms.allocatedSize()+"\n"+
            ")\n"
         );
         if(blocks){
            sb.append("used blocks:\n");
            sb.append(blockToString(entryAddr,true));
            sb.append("\n--------\n");
         }
         if(cache){
            sb.append("cached data blocks:\n");
            scanCache(ms, dataCacheAddr, addr->{
               sb.append(blockToString(addr, false));
            });
            sb.append("\n--------\n");
            sb.append("cached table blocks:\n");
            scanCache(ms, tableCacheAddr, addr->{
               sb.append(blockToString(addr, false));
            });
            sb.append("\n--------\n");
         }
         return sb.toString();
      }
      
      // result = [num tables, num lists, num records, num records(level=0), num records(level=1), ...]
      public LongList stat() {
         LongList stat = new LongList();
         stat(entryAddr, stat, 0);
         return stat;
      }
      
      private void stat(long addr, LongList stat, int level){
         if(addr==NULL) return;
         int type=ms.readByte(addr+TYPE_OFF);
         switch(type){
            case TYPE_DATA:{
               stat.inc(1, 1);
               do{
                  stat.inc(2, 1);
                  stat.inc(3 + level, 1);
                  addr=ms.readLong(addr+NEXT_ADDR_OFF);
               }
               while(addr!=NULL);
               return;
            }
            case TYPE_TABLE:
               stat.inc(0, 1);
               for(int i=0;i<256;i++){
                  long ref=ms.readLong(addr+TABLE_OFF+i*ADDR_SIZE);
                  if(ref!=0){
                     stat(ref, stat, level + 1);
                  }
               }
         }
      }
      
      private String blockToString(long addr, boolean recurse){
         if(addr==NULL) return "null";
         int type=ms.readByte(addr+TYPE_OFF);
         switch(type){
            case TYPE_DATA:{
               String s=addr+": data list {\n";
               do{
                  s+="    "+addr+"("+ms.readByte(addr+TYPE_OFF)+","+
                     ms.readLong(addr+PARENT_ADDR_OFF)+","+
                     ms.readLong(addr+NEXT_ADDR_OFF)+"): "+
                     ms.toString(addr+KEY_OFF, keySize)
                  ;
                  s+=ms.toString(addr + valueOff, valueSize)+"\n";
                  addr=ms.readLong(addr+NEXT_ADDR_OFF);
               }
               while(recurse && addr!=NULL);
               return s+"}\n";
            }
            case TYPE_TABLE:
               int size=ms.readShort(addr+TABLE_SIZE_OFF);
               String s=addr+":{table("+size+","+ms.readLong(addr+PARENT_ADDR_OFF)+"):\n";
               for(int i=0;i<256;i++){
                  long ref=ms.readLong(addr+TABLE_OFF+i*ADDR_SIZE);
                  if(ref!=NULL){
                     s+="    "+i+": "+ref+"\n";
                  }
               }
               s+="}\n";
               if(recurse) for(int i=0;i<256;i++){
                  long ref=ms.readLong(addr+TABLE_OFF+i*ADDR_SIZE);
                  if(ref!=0){
                     s+=blockToString(ref, recurse);//+"\n";
                  }
               }
               return s;
         }
         return addr+":<bad type: "+type+">";
      }
   }
}
