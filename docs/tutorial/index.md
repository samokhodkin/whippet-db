Introduction to whippet-db
==========================

## How to include whippet-db to a project

For maven projects add the dependency:
````
<dependency>
  <groupId>io.github.samokhodkin</groupId>
  <artifactId>whippet-db</artifactId>
  <version>1.0.1</version>
</dependency>
````
The latest version number can be found [here](https://search.maven.org/artifact/io.github.samokhodkin/whippet-db) 

Otherwise just add a jar file to your class path. Whippet-db has no dependencies.
The file can be found on the [release page](https://github.com/samokhodkin/whippet-db/releases).

## The hello world

````
import io.github.whippetdb.db.api.*;
import io.github.whippetdb.db.api.types.*;

....

Map<Integer, CharSequence> db = new DbBuilder(new IntIO(), new CharsIO()) // define data types
    .create() // create the in-memory database 
    .asMap(); // wrap as `java.util.Map`

map.put(1, "Hello");
map.put(2, "World");
````

The typical steps are the following:

 - import the `io.github.whippetdb.db.api` and `io.github.whippetdb.db.api.*` packages

 - create the builder with the type arguments
 
 - perform some configuration on the builder
 
 - create/open the database
 
 - obtain a database reference (typically as a `java.util.Map`)
 
 - use the reference

These steps are described in detail in the next sections.


## The whippet-db interfaces and data types

There are two interfaces which can be used to operate on whippet-db.

The first is the whippet's proprietary one, `io.github.whippetdb.db.api.Db`. It is more capable and is slightly faster. It operates directly on the bytes in memory, through a number of interfaces described in the package 
`io.github.whippetdb.memory.api`. We will not focus on this interface right now.

Another, and much easier approach is to use the `java.util.Map` wrapper. 
As a `Map` interface operates on java objects, we need to supply converters that would convert between java objects and bytes in memory. The converters are the implementations of the interface `io.github.whippetdb.db.api.TypeIO`.
A number of ready-made converters exists in the package `io.github.whippetdb.db.api.types`.
If you need a converter that is missing from the `types` package, you should implement it on yourself,
using the existing converters as examples and/or superclasses.

Two converters are supplied as arguments to the builder's constructor `io.github.whippetdb.db.api.DbBuilder(TypeIO, TypeIO)`. The first argument is for keys and the second one is for values. The converters also define the generic type for the builder and subsequently for the map.
The `Map` reference can be obtained via `DbBuilder::asMap()` after the database was created or opened (see the next section).

````
DbBuilder<Integer,CharSequence> builder = new DbBuilder(new IntIO(), new CharsIO());
... // configure the builder
... // create the database
Map<Integer,CharSequence> db = builder.asMap();

db.put(1, "foo");
````


## The modes of operation

Whippet-db has three modes of operation:

- in-memory: the data is not persisted at all

- file-backed: the data is written directly to a memory-mapped file.
  In this mode the database must be explicitly closed before an application exits.
  Yet there are still a number of ways for the database to become corrupt.

- journaling mode: the transactions are written first to the write-ahead log, then sent to the main file,
  in a such way as to ensure the atomicity and integrity of the changes. The journal takes care of keeping the
  database file always consistent.
  
The file-backed and journaling modes are mutually compatible, that is, once a db was created in one mode, it may be later opened in another. This is because the main file format is the same.

The mode of operation is defined by the used method of the `DbBuilder`:

- `create()` - creates an in-memory database

- `journaling(boolean b)` - defines if the database will be journaling; should be called before the following methods

- `create(String path)` - creates a file-backed or journaling database

- `open(String path)` - opens an existing file-backed or journaling database

- `openOrCreate(String path)` - creates or opens existing file-backed or journaling database

In the methos above the missing directories in the path will be automatically created. 
If the supplied path is a directory, there will be created a temporary file in this directory, 
which will be automatically deleted when the database is closed or application terminates. 


## Other `DbBuilder` configuration

### Thread safety.

By default a database reference is not thread-safe. A simple coarse-grained synchronization may be enabled 
by calling `DbBuilder::synchronize(boolean)` during a configuration step. This is in practice equivalent to using `Collections::synchronizedMap(Map)`.

### Transactions

Whippet-db does support transactions, but only in the journaling mode. Unlike conventional RDBMSs, where transactions are atomic and durable, in whippet-db transactions by default are atomic and eventually durable.
The latter means that in the case of OS failure a number of last transactions may be lost. Note that in the event of process crash, while the OS remains intact, the last transactions remain safe as well. Typically,
it takes a matter of several seconds for a transaction to become durable. If the eventual durability is unsufficient for the task, whippet-bd may also provide the instant durability at the cost of much lower performance. 

Transactions may be automatic, where each insert/update/delete is automatically commited, or manual, where 
you define transaction boundaries by calling appropriate methods.

To enable automatic transactions, call `DbBuilder::autocommit(true)` during the configuration step. 
To make automatic transactions instantly durable, call `DbBuilder::autoflush(true)` as well.

To manage transactions manually, make sure that autocommit and autoflush are disabled (as they are by default), obtain a reference to `io.github.whippetdb.db.api.Db` using `DbBuilder::db()`, and use its methods:
 
- `commit()` - to commit the last changes (since the previous commit),

- `discard()` - to discard the last changes

- `flush()` - to make a commited transaction instantly durable


## Should I close the database on exit, and how?

The answer depends on the operation mode. 

If the mode is file-backed without journaling, then the database should be explicitly closed to become durable and consistent. To do it, obtain a reference to `io.github.whippetdb.db.api.Db` using `DbBuilder::db()`, and use its method `close()`.

In the journaling mode the database doesn't need to be explicitly closed on exit, as the journal would take care of keeping the data consistent and durable. On the other hand doing it may be useful if you are going to move the data file or reopen it in non-journaling mode. 

## Examples

````
import io.github.whippetdb.db.api.DbBuilder;
import io.github.whippetdb.db.api.types.LongIO;
import io.github.whippetdb.db.api.types.CharsIO;

...

// in-memory Long-Long map
static Map<Long,Long> inMemoryMap = new DbBuilder(new LongIO(), new LongIO()).create().asMap();

// file-backed synchronized non-journaling Long-Long map
static Map<Long,Long> onDiskSynchronizedMap = new DbBuilder(new LongIO(), new LongIO())
	.synchronize(true)
	.create("path/to/file").asMap();

// journaling synchronized Long-Long map
static Map<Long,Long> journalingSynchronizedLongLongMap = new DbBuilder(new LongIO(), new LongIO())
	.journaling(true)
	.synchronize(true)
	.create("path/to/file").asMap();

// journaling synchronized CharSequence-CharSequence map;
// for better performance provide expected average key and value sizes
static Map<CharSequence,CharSequence> journalingSynchronizedStrStrMap = new DbBuilder(new CharsIO(10,null), new CharsIO(40,null))
	.journaling(true)
	.synchronize(true)
	.create("path/to/file").asMap();
