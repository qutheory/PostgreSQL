import PackageDescription

let beta = Version(2,0,0, prereleaseIdentifiers: ["beta"])

let package = Package(
    name: "PostgreSQL",
    dependencies: [
        // Module map for `libpq`
        .Package(url: "https://github.com/vapor-community/cpostgresql.git", beta),

        // Data structure for converting between multiple representations
        .Package(url: "https://github.com/vapor/node.git", Version(2,0,0)),

        // Core extensions, type-aliases, and functions that facilitate common tasks
        .Package(url: "https://github.com/vapor/core.git", Version(2,0,0)),
    ]
)
