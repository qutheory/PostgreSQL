import PackageDescription

let package = Package(
    name: "PostgreSQL",
    dependencies: [
        // Module map for `libpq`
        .Package(url: "https://github.com/vapor-community/cpostgresql.git", majorVersion: 2),

        // Data structure for converting between multiple representations
        .Package(url: "https://github.com/vapor/node.git", majorVersion: 2),

        // Core extensions, type-aliases, and functions that facilitate common tasks
        .Package(url: "https://github.com/vapor/core.git", majorVersion: 2)
    ]
)
