Whippet-db
==========

Fast embedded key-value database engine for Java with a Map interface.

[![Maven Central](https://img.shields.io/maven-central/v/io.github.samokhodkin/whippet-db.svg)](https://mvnrepository.com/artifact/io.github.samokhodkin/whippet-db/latest)
[![Javadoc](https://www.javadoc.io/badge/io.github.samokhodkin/whippet-db.svg)](https://www.javadoc.io/doc/io.github.samokhodkin/whippet-db)


## Overview

Whippet-db is a fast embedded local key-value store for Java, either in-memory or persistent - at your choice. The library is extremally low-footprint - less then 150 kb, with no dependencies, and is very simple to use. It is written in pure classic Java 1.8 with no use of Unsafe or native methods, so there are no problems with transition to newer Java versions.

## Applications

* as a general data index or a cache
* as a drop-in replacement for a java.util.Map which is off-heap, persistent and sometimes faster
* in journaling mode Whippet makes an ideal application configuration/properties store

## Features

* **Performance.** In non-journaling mode its CRUD speed range reaches 5-7M op/sec for small fixed-size objects, while in the journaling mode over an SSD the write speed is about 150-250K op/sec.

* **Journaling mode** makes Whippet to keep the updates atomic, so the integrity of a database doesn't suffer from a sudden system crash or power outage. By default the updates are eventually durable, which means that in the case of an application failure all updates remain intact, while in the case of a system or hardware failure a number of the very recent updates may be lost. You may also opt for instant durability, at the cost of much lower speed.

* **No preallocation.** Used RAM of file space grows dynamically.

* **The user API of Whippet is as easy as it gets.** The [DbBuilder](https://samokhodkin.github.io/whippet-db/api/io/github/whippetdb/db/api/DbBuilder.html) class is used to do some customisations and to open/create the store, while the database itself is represented as  `java.util.Map`. [Some data types](https://samokhodkin.github.io/whippet-db/api/io/github/whippetdb/db/api/types/package-summary.html) are supported out of the box, but for the less common types you need to implement the [TypeIO](https://samokhodkin.github.io/whippet-db/api/io/github/whippetdb/db/api/TypeIO.html) interface. There is also a more advanced [internal interface](https://samokhodkin.github.io/whippet-db/api/io/github/whippetdb/db/api/Db.html) to the database.

* **In-place value modification** is possible using the internal API, including value resizing

* **Thread safety** is currently implemented via an optional coarse-grained synchronization with one lock per whole database.

## What Whippet DB is not

* Not a distributed database. It may be used only in a single process.

* Not highly concurrent: its performance doesn't scale with more threads because of a single lock per database.

* It doesn't provide response time guarantee, especially in journaling mode

* It doesn't focus on space efficiency. The average overhead is 20-25 bytes/key for fixed-size data, and 80-100 bytes/key for variable-size data. This will be improved in next releses, but not much.

## Setup

Add latest Maven dependency:
[![Maven Central](https://img.shields.io/maven-central/v/io.github.samokhodkin/whippet-db.svg)](https://mvnrepository.com/artifact/io.github.samokhodkin/whippet-db/latest)

or [download the latest whippet-db-x.x.x jar](https://github.com/samokhodkin/whippet-db/releases)

## Documentation

[Brief tutorial with examples](https://github.com/samokhodkin/whippet-db/blob/main/docs/tutorial/index.md)

[Demo project](https://github.com/samokhodkin/whippet-db-demo)

The latest javadoc: [![Javadoc](https://www.javadoc.io/badge/io.github.samokhodkin/whippet-db.svg)](https://www.javadoc.io/doc/io.github.samokhodkin/whippet-db)


## Performance

The important feature of Whipet, which may seem both as a problem and as a benefit, is that its performance is very sensitive to the randomness of the keys. 
In more exact terms, it's sensitive to inter- and intra-correlation of bits in the keys. The more correlated are the keys, the better is performance, both in the terms of used space and speed. 
So Whippet shines when the keys are sequential numbers, and is modest when the keys are strongly random, the difference reaching 5-6 times in speed and 1.5-1.7 times in used space. 

The following pictures compare the typical insertion speed and used space of 3 implementations of `java.util.Map<Long,Long>` - the Whippet DB, the [Cronicle Map](https://github.com/OpenHFT/Chronicle-Map), and the `java.util.HashMap`.
The implementations were run against the three sets of 50M keys - serial, moderately random and strongly random. The HashMap was unable to insert more then 30-40M keys, it gets stuck in garbage collection.

Keys type | # of inserted keys vs time | Insertion speed vs time | Used space vs # of inserted keys
----------|----------|----------|----------
Sequential | [image](https://samokhodkin.github.io/whippet-db/images/keys-time-serial.png) | [image](https://samokhodkin.github.io/whippet-db/images/speed-keys-serial.png) | [image](https://samokhodkin.github.io/whippet-db/images/size-keys-serial.png)
Moderately random | [image](https://samokhodkin.github.io/whippet-db/images/keys-time-mod-random.png) | [image](https://samokhodkin.github.io/whippet-db/images/speed-keys-mod-random.png) | [image](https://samokhodkin.github.io/whippet-db/images/size-keys-mod-random.png)
Strongly random | [image](https://samokhodkin.github.io/whippet-db/images/keys-time-random.png) | [image](https://samokhodkin.github.io/whippet-db/images/speed-keys-random.png) | [image](https://samokhodkin.github.io/whippet-db/images/size-keys-random.png)

The table below shows the typical figures for a workload consisting of 50M fresh inserts, followed by 50M reads, then 50M deletes, then 50M secondary inserts, 
against the three key sets described above. Both keys and values are 8-byte Longs. For comparison there are also figures for Cronicle Map.

Keys type | Inserts, op/sec | Reads, op/sec | Deletes, op/sec | Inserts, op/sec | Average, op/sec
----------|----------|----------|----------|----------|----------
Whippet, sequential keys | 5,921,364 (38 bytes/key) | 9,626,492 | 9,044,862 | 7,257,947 (38 bytes/key) | 7,676,069
Whippet, moderately random keys | 2,526,528 (42 bytes/key) | 3,243,383 | 3,118,373 | 2,169,009 (42 bytes/key) |  2,692,079
Whippet, strongly random keys | 1,768,581 (52 bytes/key) | 2,182,096 | 2,204,707 | 1,633,853 (52 bytes/key) | 1,914,493
Chronicle, sequential keys | 2,606,610 (27 bytes/key) | 4,689,551 | 2,506,391 | 2,713,998 (27 bytes/key) | 2,931,863
Chronicle, moderately random keys | 2,487,562 (27 bytes/key) | 4,405,674 | 2,485,830 | 2,597,267 (27 bytes/key) | 2,824,300
Chronicle, strongly random keys | 2,469,867 (27 bytes/key) | 4,368,338 | 2,451,340 | 2,583,445 (27 bytes/key) | 2,799,512


