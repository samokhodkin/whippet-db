# Whippet-db

An embedded key-value store for Java.

## Overview

Whippet-db is a fast embedded local key-value store for Java, either in-memory or persistent - of your choice. The library is extremally low-footprint - less then 150 kb, with no dependencies, and is very simple to use. It is written in pure classic Java 1.8 with no use of Unsafe or native methods, so there are no problems with transition to newer Java versions.

## Applications

* as a general data index or a cache
* as a persistent and sometimes more capable drop-in replacement for a java.util.Map
* in journaling mode Whippet makes an ideal application configuration/properties store

## Features

* **Performance.** Despite its small size and pure-Java nature, Whippet is quite fast, rivaling the fastest known KV stores. In non-journaling mode its CRUD speed range reaches 5-7M op/sec for small objects, while in the journaling mode over an SSD the write speed is about 150-250K op/sec.

* **Journaling mode** makes Whippet to keep the updates atomic, so the integrity of a database doesn't suffer from a sudden power outage. By default the updates are eventually durable. This means that in the case of an application failure all updates remain intact, while in the case of a system or hardware failure a number of the very recent updates may be lost. You may also opt for instant durability, at the cost of much lower speed.

* **No preallocation.** Used RAM of file space grows dynamically.

* **The user API of Whippet is as easy as it gets.** The [DbBuilder](https://samokhodkin.github.io/whippet-db/api/io/github/whippetdb/db/api/DbBuilder.html) class is used to do some customisations and to open/create the store, while the database itself is represented as  `java.util.Map`. [Some data types](https://samokhodkin.github.io/whippet-db/api/io/github/whippetdb/db/api/types/package-summary.html) are supported out of the box, but for the less common types you need to implement the [TypeIO](https://samokhodkin.github.io/whippet-db/api/io/github/whippetdb/db/api/TypeIO.html) interface. There is also a more advanced [internal interface](https://samokhodkin.github.io/whippet-db/api/io/github/whippetdb/db/api/Db.html) to the database.

* **In-place value modification** is possible using the internal API, including value resizing

* **Thread safety** is currently implemented via an optional coarse-grained synchronization with one lock per whole database.

## Setup

Maven dependency
````
not yet available
````
or [download the jar](https://github.com/samokhodkin/whippet-db/releases)

## Documentation

See the [API doc](https://samokhodkin.github.io/whippet-db/api/)

## Code samples

