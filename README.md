# Whippet-db

An embedded key-value store for Java.

## Overview

Whippet-db is a fast embedded local key-value store for Java, either in-memory or persistent - of your choice. The library is extremally low-footprint - less then 150 kb, with no dependencies, and is very simple to use. It is written in pure classic Java 1.8 with no use of Unsafe or native methods, so there are no problems with transition to newer Java versions.

## Applications

* as a general data index or a cache
* as a drop-in replacement for a java.util.Map which is off-heap, persistent and sometimes faster
* in journaling mode Whippet makes an ideal application configuration/properties store

## Features

* **Performance.** Despite its small size and pure-Java nature, Whippet is quite fast, rivaling the fastest known KV stores. In non-journaling mode its CRUD speed range reaches 5-7M op/sec for small objects, while in the journaling mode over an SSD the write speed is about 150-250K op/sec.

* **Journaling mode** makes Whippet to keep the updates atomic, so the integrity of a database doesn't suffer from a sudden power outage. By default the updates are eventually durable. This means that in the case of an application failure all updates remain intact, while in the case of a system or hardware failure a number of the very recent updates may be lost. You may also opt for instant durability, at the cost of much lower speed.

* **No preallocation.** Used RAM of file space grows dynamically.

* **The user API of Whippet is as easy as it gets.** The [DbBuilder](https://samokhodkin.github.io/whippet-db/api/io/github/whippetdb/db/api/DbBuilder.html) class is used to do some customisations and to open/create the store, while the database itself is represented as  `java.util.Map`. [Some data types](https://samokhodkin.github.io/whippet-db/api/io/github/whippetdb/db/api/types/package-summary.html) are supported out of the box, but for the less common types you need to implement the [TypeIO](https://samokhodkin.github.io/whippet-db/api/io/github/whippetdb/db/api/TypeIO.html) interface. There is also a more advanced [internal interface](https://samokhodkin.github.io/whippet-db/api/io/github/whippetdb/db/api/Db.html) to the database.

* **In-place value modification** is possible using the internal API, including value resizing

* **Thread safety** is currently implemented via an optional coarse-grained synchronization with one lock per whole database.

## What Whippet DB is not

* Not a distributed database. It may be used only inside a single process.

* Not highly concurrent: its performance doesn't scale with more threads because of a single lock per database.

* It doesn't provide response time guarantee, especially in journaling mode

* It's doesn't save space. The average overhead is 20-25 bytes/key for fixed-size data, and 80-100 bytes/key for variable-size data. This will be improved in next releses, but not much.

## Setup

Maven dependency
````
not yet available
````
or [download the jar](https://github.com/samokhodkin/whippet-db/releases)

## Documentation

See the [API doc](https://samokhodkin.github.io/whippet-db/api/)

## Code samples

````
import io.github.whippetdb.db.api.DbBuilder;
import io.github.whippetdb.db.api.types.LongIO;
import io.github.whippetdb.db.api.types.CharsIO;

...

// in-memory Long:Long map
static Map<Long,Long>  = new DbBuilder(new LongIO(), new LongIO()).create().asMap();

// on-disk, synchronized Long:Long map
static Map<Long,Long>  = new DbBuilder(new LongIO(), new LongIO())
	.synchronize(true)
	.create("path/to/file").asMap();

// journaling on-disk synchronized Long:Long map
static Map<Long,Long>  = new DbBuilder(new LongIO(), new LongIO())
	.journaling(true)
	.synchronize(true)
	.create("path/to/file").asMap();

// journaling on-disk synchronized CharSequence:CharSequence map;
// for better performance provide expected average key and value sizes
static Map<CharSequence,CharSequence>  = new DbBuilder(new CharsIO(20,null), new CharsIO(50,null))
	.journaling(true)
	.synchronize(true)
	.create("path/to/file").asMap();

````

## Performance

The important feature of Whipet, which may seem both as a problem and as a benefit, is that its performance is very sensitive to the randomness of the keys. 
In more exact terms, it's sensitive to inter- and intra-correlation of bits in the keys. The more correlated are the keys, the better is performance, both in the terms of used space and speed. 
So Whippet shines best when the keys are just a sequential numbers, and is modest when the keys are strongly random, the difference reaching 5-6 times in speed and 1.5-1.7 times in used space. 

The following pictures compare the typical insertion speed and used space of 3 implementations of `java.util.Map<Long,Long>` - the Whippet DB, the [Cronicle Map](https://github.com/OpenHFT/Chronicle-Map), and the `java.util.HashMap`.
The implementations were run against the three sets of 50M keys - serial, moderately random and strongly random. The HashMap wasn't able to insert more then 33M keys, it just got stuck in garbage collection.

Keys type | # of inserted keys vs time | Insertion speed vs time | Used space vs # of inserted keys
----------|----------|----------|----------
Sequential | [image](https://samokhodkin.github.io/whippet-db/images/keys-time-serial.png) | [image](https://samokhodkin.github.io/whippet-db/images/speed-keys-serial.png) | [image](https://samokhodkin.github.io/whippet-db/images/size-keys-serial.png)
Moderately random | [image](https://samokhodkin.github.io/whippet-db/images/keys-time-mod-random.png) | [image](https://samokhodkin.github.io/whippet-db/images/speed-keys-mod-random.png) | [image](https://samokhodkin.github.io/whippet-db/images/size-keys-mod-random.png)
Strongly random | [image](https://samokhodkin.github.io/whippet-db/images/keys-time-random.png) | [image](https://samokhodkin.github.io/whippet-db/images/speed-keys-random.png) | [image](https://samokhodkin.github.io/whippet-db/images/size-keys-random.png)

The table below compares the speed and space for a workload consisting of 50M fresh inserts, followed by 50M reads, then 50M deletes, then 50M secondary inserts. 
Whippet and Cronicle Map were compared against the three key sets described above. Both keys and values are 8-byte Longs.

Keys type | Inserts | Reads | Deletes | Inserts | Average
----------|----------|----------|----------
Whippet, sequential keys | 5921364.0 op/sec, 38 bytes/key | 9626492.0 op/sec | 9044862.0 op/sec | 7257947.5 op/sec, 38 bytes/key | 7676069.5 op/sec
Chronicle, sequential keys | 2606610.2 op/sec, 27 bytes/key total | 4689551.5 op/sec | 2506391.2 op/sec | 2713998.8 op/sec, 27 bytes/key total | 2931863.5 op/sec
Whippet, moderately random keys | 2526528.5 op/sec, 42 bytes/key | 3243383.5 op/sec | 3118373.5 op/sec | 2169009.2 op/sec, 42 bytes/key |  2692079.8 op/sec
Chronicle, moderately random keys | 2487562.2 op/sec, 27 bytes/key total | 4405674.5 op/sec | 2485830.8 op/sec | 2597267.5 op/sec, 27 bytes/key total | 2824300.2 op/sec
Whippet, strongly random keys | 1768581.1 op/sec, 52 bytes/key | 2182096.0 op/sec | 2204707.0 op/sec | 1633853.5 op/sec, 52 bytes/key | 1914493.9 op/sec
Chronicle, strongly random keys | 2469867.5 op/sec, 27 bytes/key | 4368338.0 op/sec | 2451340.8 op/sec | 2583445.2 op/sec , 27 bytes/key | 2799512.8 op/sec


