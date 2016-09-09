import PackageDescription

let package = Package(
    name: "PostgreSQL",
    dependencies: [
   		 .Package(url: "https://github.com/qutheory/cpostgresql.git", majorVersion: 0),
   		 .Package(url: "https://github.com/vapor/node.git", majorVersion: 0, minor: 5)
    ]
)
