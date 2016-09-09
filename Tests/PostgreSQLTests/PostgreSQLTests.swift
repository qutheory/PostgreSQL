import XCTest
@testable import PostgreSQL

class PostgreSQLTests: XCTestCase {
    static let allTests = [
        ("testSelectVersion", testSelectVersion),
        ("testTables", testTables),
        ("testParameterization", testParameterization),
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
            try postgreSQL.execute("CREATE TABLE foo (bar INT, baz VARCHAR(16))")
            try postgreSQL.execute("INSERT INTO foo VALUES (42, 'Life')")
            try postgreSQL.execute("INSERT INTO foo VALUES (1337, 'Elite')")
            try postgreSQL.execute("INSERT INTO foo VALUES (9, NULL)")
            
            
            
            if let resultBar = try postgreSQL.execute("SELECT * FROM foo WHERE bar = 42").first {
                XCTAssertEqual(resultBar["bar"]?.int, 42)
                XCTAssertEqual(resultBar["baz"]?.string, "Life")
            } else {
                XCTFail("Could not get bar result")
            }


            if let resultBaz = try postgreSQL.execute("SELECT * FROM foo where baz = 'Elite'").first {
                XCTAssertEqual(resultBaz["bar"]?.int, 1337)
                XCTAssertEqual(resultBaz["baz"]?.string, "Elite")
            } else {
                XCTFail("Could not get baz result")
            }

            if let resultBaz = try postgreSQL.execute("SELECT * FROM foo where bar = 9").first {
                XCTAssertEqual(resultBaz["bar"]?.int, 9)
                XCTAssertEqual(resultBaz["baz"]?.string, nil)
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
            XCTFail("Testing tables failed: \(error)")
        }
    }
}
