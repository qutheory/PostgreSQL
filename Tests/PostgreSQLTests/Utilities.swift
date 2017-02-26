import XCTest
import PostgreSQL
import Foundation

extension PostgreSQL.Database {
    static func makeTestConnection() -> PostgreSQL.Database {
        do {
            let postgreSQL = try PostgreSQL.Database(
                host: "127.0.0.1",
                port: 5432,
                database: "test",
                user: "postgres",
                password: ""
            )
            try postgreSQL.execute("SELECT version()")
            return postgreSQL
        } catch {
            print()
            print()
            print("⚠️ PostgreSQL Not Configured ⚠️")
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

extension String {
    var hexStringBytes: [Int8] {
        guard let characters = cString(using: .utf8) else {
            return []
        }
        
        var data: [Int8] = []
        data.reserveCapacity(characters.count / 2)
        
        var byteChars: [CChar] = [0, 0, 0]
        for i in stride(from: 0, to: characters.count - 1, by: 2) {
            byteChars[0] = characters[i]
            byteChars[1] = characters[i+1]
            let byteValue = UInt8(strtol(byteChars, nil, 16))
            
            guard byteValue != 0 || (byteChars[0] == 48 && byteChars[1] == 48) else {
                return []
            }
            
            data.append(Int8(bitPattern: byteValue))
        }
        
        return data
    }
    
    var postgreSQLParsedDate: Date {
        struct Formatter {
            static let `static`: DateFormatter = {
                let formatter = DateFormatter()
                formatter.timeZone = TimeZone(abbreviation: "UTC")
                formatter.dateFormat = "yyyy-MM-dd HH:mm:ss.SSS"
                return formatter
            }()
        }
        return Formatter.static.date(from: self)!
    }
}

extension Float32 {
    static let min = Float32(bitPattern: 0x00800000)
    static let max = Float32(bitPattern: 0x7f7fffff)
}

extension Float64 {
    static let min = Float64(bitPattern: 0x0010000000000000)
    static let max = Float64(bitPattern: 0x7fefffffffffffff)
}
