// swift-tools-version:4.0
import PackageDescription

let package = Package(
    name: "PostgreSQL",
    products: [
        .library(name: "PostgreSQL", targets: ["PostgreSQL"]),
    ],
    dependencies: [
        // Module map for `libpq`
        .package(url: "https://github.com/vapor-community/cpostgresql.git", .upToNextMajor(from: "2.1.0")),
        
        // Data structure for converting between multiple representations
        .package(url: "https://github.com/vapor/node.git", .upToNextMajor(from: "2.1.0")),

        // Core extensions, type-aliases, and functions that facilitate common tasks
        .package(url: "https://github.com/vapor/core.git", .upToNextMajor(from: "2.1.2"))
    ],
    targets: [
        .target(name: "PostgreSQL", dependencies: ["CPostgreSQL", "Node", "Core"]),
        .testTarget(name: "PostgreSQLTests", dependencies: ["PostgreSQL"]),
    ]
)
