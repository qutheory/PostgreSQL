[![Swift](https://img.shields.io/badge/swift-3.1_--_4.0-brightgreen.svg)](https://swift.org)
[![Linux Build Status](https://img.shields.io/circleci/project/github/vapor-community/postgresql.svg?label=Linux)](https://circleci.com/gh/vapor-community/postgresql)
[![macOS Build Status](https://img.shields.io/travis/vapor-community/postgresql.svg?label=macOS)](https://travis-ci.org/vapor-community/postgresql)
[![codecov](https://codecov.io/gh/vapor-community/postgresql/branch/master/graph/badge.svg)](https://codecov.io/gh/vapor-community/postgresql)
[![GitHub license](https://img.shields.io/badge/license-MIT-brightgreen.svg)](LICENSE)

# PostgreSQL for Swift

## Prerequisites

The PostgreSQL C driver must be installed in order to use this package.  
Follow the [README of the cpostgresql repo](https://github.com/vapor-community/cpostgresql/blob/master/README.md) to get started.

## Using PostgreSQL

This section outlines how to import the PostgreSQL package both with or without a Vapor project.

### With Vapor

The easiest way to use PostgreSQL with Vapor is to include the PostgreSQL provider.

```swift
import PackageDescription

let package = Package(
    name: "Project",
    dependencies: [
        .Package(url: "https://github.com/vapor/vapor.git", majorVersion: 2),
        .Package(url: "https://github.com/vapor-community/postgresql-provider.git", majorVersion: 2)
    ],
    exclude: [ ... ]
)
```

The PostgreSQL provider package adds PostgreSQL to your project and adds some additional, Vapor-specific conveniences like `drop.postgresql()`.

Using `import PostgreSQLProvider` will import both Fluent and Fluent's Vapor-specific APIs.

### With Fluent

Fluent is a powerful, pure-Swift ORM that can be used with any Server-Side Swift framework. The PostgreSQL driver allows you to use a PostgreSQL database to power your models and queries.

```swift
import PackageDescription

let package = Package(
    name: "Project",
    dependencies: [
        ...
        .Package(url: "https://github.com/vapor/fluent.git", majorVersion: 2),
        .Package(url: "https://github.com/vapor-community/postgresql-driver.git", majorVersion: 2)
    ],
    exclude: [ ... ]
)
```

Use `import PostgreSQLDriver` to access the `PostgreSQLDriver` class which you can use to initialize a Fluent `Database`.

### Just PostgreSQL

At the core of the PostgreSQL provider and PostgreSQL driver is a Swift wrapper around the C PostgreSQL client. This package can be used by itself to send raw, parameterized queries to your PostgreSQL database.

```swift
import PackageDescription

let package = Package(
    name: "Project",
    dependencies: [
        ...
        .Package(url: "https://github.com/vapor/postgresql.git", majorVersion: 2)
    ],
    exclude: [ ... ]
)
```

Use `import PostgreSQL` to access the `PostgreSQL.Database` class.


# Examples

## Connecting to the Database

```swift
import PostgreSQL

let postgreSQL =  PostgreSQL.Database(
    hostname: "localhost",
    database: "test",
    user: "root",
    password: ""
)
```

## Select

```swift
let version = try postgreSQL.execute("SELECT version()")
```

## Prepared Statement

The second parameter to `execute()` is an array of `PostgreSQL.Value`s.

```swift
let results = try postgreSQL.execute("SELECT * FROM users WHERE age >= $1", [.int(21)])
```

## Listen and Notify

```swift
try postgreSQL.listen(to: "test_channel") { notification in
    print(notification.channel)
    print(notification.payload)
}

// Allow set up time for LISTEN
sleep(1)

try postgreSQL.notify(channel: "test_channel", payload: "test_payload")

```

## Connection

Each call to `execute()` creates a new connection to the PostgreSQL database. This ensures thread safety since a single connection cannot be used on more than one thread.

If you would like to re-use a connection between calls to execute, create a reusable connection and pass it as the third parameter to `execute()`.

```swift
let connection = try postgreSQL.makeConnection()
let result = try postgreSQL.execute("SELECT * FROM users WHERE age >= $1", [.int(21)]), connection)
```

## Contributors

Maintained by [Steven Roebert](https://github.com/sroebert), [Nate Bird](https://twitter.com/natesbird), [Prince Ugwuh](https://twitter.com/Prince2k3), and other members of the Vapor community.
