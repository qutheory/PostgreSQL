import XCTest
@testable import PostgreSQL

class PostgreSQLTests: XCTestCase {
    static let allTests = [
        ("testSelectVersion", testSelectVersion),
        ("testTables", testTables),
        ("testParameterization", testParameterization),
        ("testDataType", testDataType),
        ("testCustomType", testCustomType),
    ]

    var postgreSQL: PostgreSQL.Database!

    override func setUp() {
        postgreSQL = PostgreSQL.Database.makeTestConnection()
    }

    func testSelectVersion() {
        do {
            let results = try postgreSQL.execute("SELECT version(), version(), 1337, 3.14, 'what up', NULL")
            guard let version = results.first?["version"] else {
                XCTFail("Version not in results")
                return
            }

            guard let string = version.string else {
                XCTFail("Version not in results")
                return
            }

            XCTAssert(string.hasPrefix("PostgreSQL"))
        } catch {
            XCTFail("Could not select version: \(error)")
        }
    }

    func testTables() {
        do {
            try postgreSQL.execute("DROP TABLE IF EXISTS foo")
            try postgreSQL.execute("CREATE TABLE foo (bar INT, baz VARCHAR(16), bla BOOLEAN)")
            try postgreSQL.execute("INSERT INTO foo VALUES (42, 'Life', true)")
            try postgreSQL.execute("INSERT INTO foo VALUES (1337, 'Elite', false)")
            try postgreSQL.execute("INSERT INTO foo VALUES (9, NULL, true)")

            if let resultBar = try postgreSQL.execute("SELECT * FROM foo WHERE bar = 42").first {
                XCTAssertEqual(resultBar["bar"]?.int, 42)
                XCTAssertEqual(resultBar["baz"]?.string, "Life")
                XCTAssertEqual(resultBar["bla"]?.bool, true)
            } else {
                XCTFail("Could not get bar result")
            }

            if let resultBaz = try postgreSQL.execute("SELECT * FROM foo where baz = 'Elite'").first {
                XCTAssertEqual(resultBaz["bar"]?.int, 1337)
                XCTAssertEqual(resultBaz["baz"]?.string, "Elite")
                XCTAssertEqual(resultBaz["bla"]?.bool, false)
            } else {
                XCTFail("Could not get baz result")
            }

            if let resultBaz = try postgreSQL.execute("SELECT * FROM foo where bar = 9").first {
                XCTAssertEqual(resultBaz["bar"]?.int, 9)
                XCTAssertEqual(resultBaz["baz"]?.string, nil)
                XCTAssertEqual(resultBaz["bla"]?.bool, true)
            } else {
                XCTFail("Could not get null result")
            }
        } catch {
            XCTFail("Testing tables failed: \(error)")
        }
    }

    func testParameterization() {
        do {
            try postgreSQL.execute("DROP TABLE IF EXISTS parameterization")
            try postgreSQL.execute("CREATE TABLE parameterization (d FLOAT4, i INT, s VARCHAR(16), u INT)")
            
            try postgreSQL.execute("INSERT INTO parameterization VALUES ($1, $2, $3, $4)", [.null, .null, "life".makeNode(), .null], on: nil)

            try postgreSQL.execute("INSERT INTO parameterization VALUES (3.14, NULL, 'pi', NULL)")
            try postgreSQL.execute("INSERT INTO parameterization VALUES (NULL, NULL, 'life', 42)")
            try postgreSQL.execute("INSERT INTO parameterization VALUES (NULL, -1, 'test', NULL)")

            if let result = try postgreSQL.execute("SELECT * FROM parameterization WHERE d = $1", [3.14]).first {
                XCTAssertEqual(result["d"]?.double, 3.14)
                XCTAssertEqual(result["i"]?.int, nil)
                XCTAssertEqual(result["s"]?.string, "pi")
                XCTAssertEqual(result["u"]?.int, nil)
            } else {
                XCTFail("Could not get pi result")
            }

            if let result = try postgreSQL.execute("SELECT * FROM parameterization WHERE u = $1", [42]).first {
                XCTAssertEqual(result["d"]?.double, nil)
                XCTAssertEqual(result["i"]?.int, nil)
                XCTAssertEqual(result["s"]?.string, "life")
                XCTAssertEqual(result["u"]?.int, 42)
            } else {
                XCTFail("Could not get life result")
            }

            if let result = try postgreSQL.execute("SELECT * FROM parameterization WHERE i = $1", [-1]).first {
                XCTAssertEqual(result["d"]?.double, nil)
                XCTAssertEqual(result["i"]?.int, -1)
                XCTAssertEqual(result["s"]?.string, "test")
                XCTAssertEqual(result["u"]?.int, nil)
            } else {
                XCTFail("Could not get test by int result")
            }

            if let result = try postgreSQL.execute("SELECT * FROM parameterization WHERE s = $1", ["test"]).first {
                XCTAssertEqual(result["d"]?.double, nil)
                XCTAssertEqual(result["i"]?.int, -1)
                XCTAssertEqual(result["s"]?.string, "test")
                XCTAssertEqual(result["u"]?.int, nil)
            } else {
                XCTFail("Could not get test by string result")
            }
        } catch {
            XCTFail("Testing parameterization failed: \(error)")
        }
    }
    
    func testDataType() throws {
        let data: [UInt8] = [1, 2, 3, 4, 5, 0, 6, 7, 8, 9, 0]
        
        try postgreSQL.execute("DROP TABLE IF EXISTS foo")
        try postgreSQL.execute("CREATE TABLE foo (bar BYTEA)")
        try postgreSQL.execute("INSERT INTO foo VALUES ($1)", [.bytes(data)])
        
        let result = try postgreSQL.execute("SELECT * FROM foo").first
        XCTAssertNotNil(result)
        
        let resultBytesNode = result!["bar"]
        XCTAssertNotNil(resultBytesNode)
        
        XCTAssertEqual(resultBytesNode!, .bytes(data))
    }
    
    func testCustomType() throws {
        let uuidString = "7fe1743a-96a8-417c-b6c2-c8bb20d3017e"
        let dateString = "2016-10-24 23:04:19.223+00"
        
        try postgreSQL.execute("DROP TABLE IF EXISTS foo")
        try postgreSQL.execute("CREATE TABLE foo (uuid UUID, date TIMESTAMP WITH TIME ZONE)")
        try postgreSQL.execute("INSERT INTO foo VALUES ($1, $2)", [.string(uuidString), .string(dateString)])
        
        let result = try postgreSQL.execute("SELECT * FROM foo").first
        XCTAssertNotNil(result)
        XCTAssertEqual(result!["uuid"]?.string, uuidString)
        XCTAssertEqual(result!["date"]?.string, dateString)
    }
}
