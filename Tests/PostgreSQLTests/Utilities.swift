import XCTest
import PostgreSQL

extension PostgreSQL.Database {
    static func makeTestConnection() -> PostgreSQL.Database {
        do {
            let postgreSQL = PostgreSQL.Database(
                host: "127.0.0.1",
                port: "5432",
                dbname: "test",
                user: "postgres",
                password: ""
                
            )
            try postgreSQL.execute("SELECT version()")
            return postgreSQL
        } catch {
            print()
            print()
            print("⚠️  PostgreSQL Not Configured ⚠️")
            print()
            print("Error: \(error)")
            print()
            print("You must configure PostgreSQL to run with the following configuration: ")
            print("    user: 'postgres'")
            print("    password: '' // (empty)")
            print("    host: '127.0.0.1'")
            print("    database: 'test'")
            print()

            print()

            XCTFail("Configure PostgreSQL")
            fatalError("Configure PostgreSQL")
        }
    }
}

