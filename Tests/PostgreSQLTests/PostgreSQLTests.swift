import XCTest
@testable import PostgreSQL
import Foundation

class PostgreSQLTests: XCTestCase {
    static let allTests = [
        ("testSelectVersion", testSelectVersion),
        ("testTables", testTables),
        ("testParameterization", testParameterization),
        ("testDataType", testDataType),
        ("testCustomType", testCustomType),
        ("testInts", testInts),
        ("testFloats", testFloats),
        ("testNumeric", testNumeric),
        ("testJSON", testJSON),
        ("testIntervals", testIntervals),
        ("testPoints", testPoints),
        ("testLineSegments", testLineSegments),
        ("testPaths", testPaths),
        ("testBoxes", testBoxes),
        ("testPolygons", testPolygons),
        ("testCircles", testCircles),
        ("testInets", testInets),
        ("testCidrs", testCidrs),
        ("testMacAddresses", testMacAddresses),
        ("testBitStrings", testBitStrings),
        ("testVarBitStrings", testVarBitStrings),
        ("testUnsupportedObject", testUnsupportedObject),
    ]

    var postgreSQL: PostgreSQL.Database!

    override func setUp() {
        postgreSQL = PostgreSQL.Database.makeTestConnection()
    }
    
    func testConnectionFailure() throws {
        let database = try PostgreSQL.Database(
            host: "127.0.0.1",
            port: 5432,
            database: "some_long_db_name_that_does_not_exist",
            user: "postgres",
            password: ""
        )
        
        try XCTAssertThrowsError(database.makeConnection()) { error in
            switch error {
            case DatabaseError.cannotEstablishConnection(_):
                break
            
            default:
                XCTFail("Invalid error")
            }
        }
    }
    
    func testConnection() throws {
        let connection = try postgreSQL.makeConnection()
        XCTAssertTrue(connection.connected)
        
        try connection.reset()
        try connection.close()
        XCTAssertFalse(connection.connected)
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
            try postgreSQL.execute("CREATE TABLE parameterization (d FLOAT8, i INT, s VARCHAR(16), u INT)")
            
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
        let dateString = "2016-10-24 23:04:19.223"
        
        try postgreSQL.execute("DROP TABLE IF EXISTS foo")
        try postgreSQL.execute("CREATE TABLE foo (uuid UUID, date TIMESTAMP WITHOUT TIME ZONE)")
        try postgreSQL.execute("INSERT INTO foo VALUES ($1, $2)", [.string(uuidString), .string(dateString)])
        
        let result = try postgreSQL.execute("SELECT * FROM foo").first
        XCTAssertNotNil(result)
        XCTAssertEqual(result!["uuid"]?.string, uuidString)
        XCTAssertEqual(result!["date"]?.string, dateString)
    }
    
    func testInts() throws {
        let rows: [(Int16, Int32, Int64)] = [
            (1, 2, 3),
            (-1, -2, -3),
            (Int16.min, Int32.min, Int64.min),
            (Int16.max, Int32.max, Int64.max),
        ]
        
        try postgreSQL.execute("DROP TABLE IF EXISTS foo")
        try postgreSQL.execute("CREATE TABLE foo (id serial, int2 int2, int4 int4, int8 int8)")
        for row in rows {
            try postgreSQL.execute("INSERT INTO foo VALUES (DEFAULT, $1, $2, $3)", [row.0.makeNode(), row.1.makeNode(), row.2.makeNode()])
        }
        
        let result = try postgreSQL.execute("SELECT * FROM foo ORDER BY id ASC")
        XCTAssertEqual(result.count, rows.count)
        for (i, resultRow) in result.enumerated() {
            let int2 = resultRow["int2"]
            XCTAssertNotNil(int2?.int)
            XCTAssertEqual(int2!.int!, Int(rows[i].0))
            
            let int4 = resultRow["int4"]
            XCTAssertNotNil(int4?.int)
            XCTAssertEqual(int4!.int!, Int(rows[i].1))
            
            let int8 = resultRow["int8"]
            XCTAssertNotNil(int8?.double)
            XCTAssertEqual(int8!.double!, Double(rows[i].2))
        }
    }
    
    func testFloats() throws {
        let rows: [(Float32, Float64)] = [
            (1, 2),
            (-1, -2),
            (1.23, 2.45),
            (-1.23, -2.45),
            (Float32.min, Float64.min),
            (Float32.max, Float64.max),
        ]
        
        try postgreSQL.execute("DROP TABLE IF EXISTS foo")
        try postgreSQL.execute("CREATE TABLE foo (id serial, float4 float4, float8 float8)")
        for row in rows {
            try postgreSQL.execute("INSERT INTO foo VALUES (DEFAULT, $1, $2)", [row.0.makeNode(), row.1.makeNode()])
        }
        
        let result = try postgreSQL.execute("SELECT * FROM foo ORDER BY id ASC")
        XCTAssertEqual(result.count, rows.count)
        for (i, resultRow) in result.enumerated() {
            let float4 = resultRow["float4"]
            XCTAssertNotNil(float4?.double)
            XCTAssertEqual(float4!.double!, Double(rows[i].0))
            
            let float8 = resultRow["float8"]
            XCTAssertNotNil(float8?.double)
            XCTAssertEqual(float8!.double!, Double(rows[i].1))
        }
    }
    
    func testNumeric() throws {
        let rows: [String] = [
            "0",
            "0.1",
            "-0.123",
            "123",
            "456.7891412341",
            "123143236449825.291401412",
            "-14982351014.1284121590511",
            "100000001000000000000.0000000001000000001",
            "NaN",
        ]
        
        try postgreSQL.execute("DROP TABLE IF EXISTS foo")
        try postgreSQL.execute("CREATE TABLE foo (id serial, numeric numeric)")
        for row in rows {
            try postgreSQL.execute("INSERT INTO foo VALUES (DEFAULT, $1)", [row.makeNode()])
        }
        
        let result = try postgreSQL.execute("SELECT * FROM foo ORDER BY id ASC")
        XCTAssertEqual(result.count, rows.count)
        for (i, resultRow) in result.enumerated() {
            let numeric = resultRow["numeric"]
            XCTAssertNotNil(numeric?.string)
            XCTAssertEqual(numeric!.string!, rows[i])
        }
    }
    
    func testJSON() throws {
        let rows: [String] = [
            "{}",
            "[]",
            "true",
            "123",
            "456.7891412341",
            "[1, 2, 3, 4, 5, 6]",
            "{\"foo\": \"bar\"}",
        ]
        
        try postgreSQL.execute("DROP TABLE IF EXISTS foo")
        try postgreSQL.execute("CREATE TABLE foo (id serial, json json, jsonb jsonb)")
        for row in rows {
            try postgreSQL.execute("INSERT INTO foo VALUES (DEFAULT, $1, $2)", [row.makeNode(), row.makeNode()])
        }
        
        let result = try postgreSQL.execute("SELECT * FROM foo ORDER BY id ASC")
        XCTAssertEqual(result.count, rows.count)
        for (i, resultRow) in result.enumerated() {
            let json = resultRow["json"]
            XCTAssertNotNil(json?.string)
            XCTAssertEqual(json!.string!, rows[i])
            
            let jsonb = resultRow["jsonb"]
            XCTAssertNotNil(jsonb?.string)
            XCTAssertEqual(jsonb!.string!, rows[i])
        }
    }
    
    func testIntervals() throws {
        let rows: [[String]] = [
            ["00:00:01","0:0:1"],
            ["00:00:00","0:0:0"],
            ["3 years 9 mons 2 days"],
            ["1 year 5 mons 1 day 00:00:12.134", "1 year 5 mons 1 day 0:0:12.134"],
            ["1 year"],
            ["2 years"],
            ["1 day"],
            ["2 days"],
            ["1 mon"],
            ["2 mons"],
            ["-00:00:01", "-0:0:1"],
            ["-1 days"],
            ["-11 mons +1 day"],
        ]
        
        try postgreSQL.execute("DROP TABLE IF EXISTS foo")
        try postgreSQL.execute("CREATE TABLE foo (id serial, interval interval)")
        for row in rows {
            try postgreSQL.execute("INSERT INTO foo VALUES (DEFAULT, $1)", [row[0].makeNode()])
        }
        
        let result = try postgreSQL.execute("SELECT * FROM foo ORDER BY id ASC")
        XCTAssertEqual(result.count, rows.count)
        for (i, resultRow) in result.enumerated() {
            let interval = resultRow["interval"]
            XCTAssertNotNil(interval?.string)
            XCTAssertTrue(rows[i].contains(interval!.string!))
        }
    }
    
    func testPoints() throws {
        let rows = [
            "(1.2,3.4)",
            "(-1.2,-3.4)",
            "(123.456,-298.135)",
        ]
        
        try postgreSQL.execute("DROP TABLE IF EXISTS foo")
        try postgreSQL.execute("CREATE TABLE foo (id serial, point point)")
        for row in rows {
            try postgreSQL.execute("INSERT INTO foo VALUES (DEFAULT, $1)", [row.makeNode()])
        }
        
        let result = try postgreSQL.execute("SELECT * FROM foo ORDER BY id ASC")
        XCTAssertEqual(result.count, rows.count)
        for (i, resultRow) in result.enumerated() {
            let point = resultRow["point"]
            XCTAssertNotNil(point?.string)
            XCTAssertEqual(point!.string!, rows[i])
        }
    }
    
    func testLineSegments() throws {
        let rows = [
            "[(1.2,3.4),(-1.2,-3.4)]",
            "[(-1.2,-3.4),(123.467,-298.135)]",
            "[(123.47,-238.123),(1.2,3.4)]",
        ]
        
        try postgreSQL.execute("DROP TABLE IF EXISTS foo")
        try postgreSQL.execute("CREATE TABLE foo (id serial, lseg lseg)")
        for row in rows {
            try postgreSQL.execute("INSERT INTO foo VALUES (DEFAULT, $1)", [row.makeNode()])
        }
        
        let result = try postgreSQL.execute("SELECT * FROM foo ORDER BY id ASC")
        XCTAssertEqual(result.count, rows.count)
        for (i, resultRow) in result.enumerated() {
            let lseg = resultRow["lseg"]
            XCTAssertNotNil(lseg?.string)
            XCTAssertEqual(lseg!.string!, rows[i])
        }
    }
    
    func testPaths() throws {
        let rows = [
            "[(1.2,3.4),(-1.2,-3.4),(123.67,-598.35)]",
            "((-1.2,-3.4),(12.4567,-298.35))",
            "((123.47,-235.35),(1.2,3.4))",
            "[(1.2,3.4)]",
        ]
        
        try postgreSQL.execute("DROP TABLE IF EXISTS foo")
        try postgreSQL.execute("CREATE TABLE foo (id serial, path path)")
        for row in rows {
            try postgreSQL.execute("INSERT INTO foo VALUES (DEFAULT, $1)", [row.makeNode()])
        }
        
        let result = try postgreSQL.execute("SELECT * FROM foo ORDER BY id ASC")
        XCTAssertEqual(result.count, rows.count)
        for (i, resultRow) in result.enumerated() {
            let path = resultRow["path"]
            XCTAssertNotNil(path?.string)
            XCTAssertEqual(path!.string!, rows[i])
        }
    }
    
    func testBoxes() throws {
        let rows = [
            "(1.2,3.4),(-1.2,-3.4)",
            "(13.467,-3.4),(-1.2,-598.35)",
            "(12.467,3.4),(1.2,-358.15)",
        ]
        
        try postgreSQL.execute("DROP TABLE IF EXISTS foo")
        try postgreSQL.execute("CREATE TABLE foo (id serial, box box)")
        for row in rows {
            try postgreSQL.execute("INSERT INTO foo VALUES (DEFAULT, $1)", [row.makeNode()])
        }
        
        let result = try postgreSQL.execute("SELECT * FROM foo ORDER BY id ASC")
        XCTAssertEqual(result.count, rows.count)
        for (i, resultRow) in result.enumerated() {
            let box = resultRow["box"]
            XCTAssertNotNil(box?.string)
            XCTAssertEqual(box!.string!, rows[i])
        }
    }
    
    func testPolygons() throws {
        let rows = [
            "((1.2,3.4),(-1.2,-3.4),(123.46,-358.25))",
            "((-1.2,-3.4),(3.4567,-28.235))",
            "((123.467,-98.123),(1.2,3.4))",
            "((1.2,3.4))",
        ]
        
        try postgreSQL.execute("DROP TABLE IF EXISTS foo")
        try postgreSQL.execute("CREATE TABLE foo (id serial, polygon polygon)")
        for row in rows {
            try postgreSQL.execute("INSERT INTO foo VALUES (DEFAULT, $1)", [row.makeNode()])
        }
        
        let result = try postgreSQL.execute("SELECT * FROM foo ORDER BY id ASC")
        XCTAssertEqual(result.count, rows.count)
        for (i, resultRow) in result.enumerated() {
            let polygon = resultRow["polygon"]
            XCTAssertNotNil(polygon?.string)
            XCTAssertEqual(polygon!.string!, rows[i])
        }
    }
    
    func testCircles() throws {
        let rows = [
            "<(1.2,3.4),456.7>",
            "<(-1.2,-3.4),98>",
            "<(123.67,-598.15),0.123>",
        ]
        
        try postgreSQL.execute("DROP TABLE IF EXISTS foo")
        try postgreSQL.execute("CREATE TABLE foo (id serial, circle circle)")
        for row in rows {
            try postgreSQL.execute("INSERT INTO foo VALUES (DEFAULT, $1)", [row.makeNode()])
        }
        
        let result = try postgreSQL.execute("SELECT * FROM foo ORDER BY id ASC")
        XCTAssertEqual(result.count, rows.count)
        for (i, resultRow) in result.enumerated() {
            let circle = resultRow["circle"]
            XCTAssertNotNil(circle?.string)
            XCTAssertEqual(circle!.string!, rows[i])
        }
    }
    
    func testInets() throws {
        let rows = [
            "192.168.100.128",
            "192.168.100.128/25",
            "2001:4f8:3:ba::/64",
            "2001:4f8:3:ba:2e0:81ff:fe22:d1f1",
            "80.60.123.255",
            "0.0.0.0",
            "127.0.0.1",
        ]
        
        try postgreSQL.execute("DROP TABLE IF EXISTS foo")
        try postgreSQL.execute("CREATE TABLE foo (id serial, inet inet)")
        for row in rows {
            try postgreSQL.execute("INSERT INTO foo VALUES (DEFAULT, $1)", [row.makeNode()])
        }
        
        let result = try postgreSQL.execute("SELECT * FROM foo ORDER BY id ASC")
        XCTAssertEqual(result.count, rows.count)
        for (i, resultRow) in result.enumerated() {
            let inet = resultRow["inet"]
            XCTAssertNotNil(inet?.string)
            XCTAssertEqual(inet!.string!, rows[i])
        }
    }
    
    func testCidrs() throws {
        let rows = [
            "192.168.100.128/32",
            "192.168.100.128/25",
            "2001:4f8:3:ba::/64",
            "2001:4f8:3:ba:2e0:81ff:fe22:d1f1/128",
            "80.60.123.255/32",
            "0.0.0.0/32",
            "127.0.0.1/32",
        ]
        
        try postgreSQL.execute("DROP TABLE IF EXISTS foo")
        try postgreSQL.execute("CREATE TABLE foo (id serial, cidr cidr)")
        for row in rows {
            try postgreSQL.execute("INSERT INTO foo VALUES (DEFAULT, $1)", [row.makeNode()])
        }
        
        let result = try postgreSQL.execute("SELECT * FROM foo ORDER BY id ASC")
        XCTAssertEqual(result.count, rows.count)
        for (i, resultRow) in result.enumerated() {
            let cidr = resultRow["cidr"]
            XCTAssertNotNil(cidr?.string)
            XCTAssertEqual(cidr!.string!, rows[i])
        }
    }
    
    func testMacAddresses() throws {
        let rows = [
            "5a:92:79:a1:ce:1a",
            "74:da:91:28:6a:a6",
            "c6:50:8d:dd:c9:dd",
            "fd:b8:e7:23:a4:56",
            "bb:ee:7f:8e:1e:39",
            "5d:0b:f4:f5:c9:24",
            "9e:b4:0c:b4:95:20",
            "b5:43:4c:f4:05:dd",
            "d8:39:78:9e:f6:fe",
            "58:ff:b8:e9:85:30",
        ]
        
        try postgreSQL.execute("DROP TABLE IF EXISTS foo")
        try postgreSQL.execute("CREATE TABLE foo (id serial, macaddr macaddr)")
        for row in rows {
            try postgreSQL.execute("INSERT INTO foo VALUES (DEFAULT, $1)", [row.makeNode()])
        }
        
        let result = try postgreSQL.execute("SELECT * FROM foo ORDER BY id ASC")
        XCTAssertEqual(result.count, rows.count)
        for (i, resultRow) in result.enumerated() {
            let macaddr = resultRow["macaddr"]
            XCTAssertNotNil(macaddr?.string)
            XCTAssertEqual(macaddr!.string!, rows[i])
        }
    }
    
    func testBitStrings() throws {
        let rows = [
            "01010",
            "00000",
            "11111",
            "10101",
            "11000",
            "00111",
            "00011",
            "00001",
            "10000",
        ]
        
        try postgreSQL.execute("DROP TABLE IF EXISTS foo")
        try postgreSQL.execute("CREATE TABLE foo (id serial, bits bit(5))")
        for row in rows {
            try postgreSQL.execute("INSERT INTO foo VALUES (DEFAULT, $1)", [row.makeNode()])
        }
        
        let result = try postgreSQL.execute("SELECT * FROM foo ORDER BY id ASC")
        XCTAssertEqual(result.count, rows.count)
        for (i, resultRow) in result.enumerated() {
            let bits = resultRow["bits"]
            XCTAssertNotNil(bits?.string)
            XCTAssertEqual(bits!.string!, rows[i])
        }
    }
    
    func testVarBitStrings() throws {
        let rows = [
            "0",
            "1",
            "01",
            "1011",
            "0011",
            "01100101",
            "11010010001110001010110010001101100010011110",
            "00000000",
            "11111111",
            "00000000000",
            "1111111111",
        ]
        
        try postgreSQL.execute("DROP TABLE IF EXISTS foo")
        try postgreSQL.execute("CREATE TABLE foo (id serial, bits bit varying)")
        for row in rows {
            try postgreSQL.execute("INSERT INTO foo VALUES (DEFAULT, $1)", [row.makeNode()])
        }
        
        let result = try postgreSQL.execute("SELECT * FROM foo ORDER BY id ASC")
        XCTAssertEqual(result.count, rows.count)
        for (i, resultRow) in result.enumerated() {
            let bits = resultRow["bits"]
            XCTAssertNotNil(bits?.string)
            XCTAssertEqual(bits!.string!, rows[i])
        }
    }
    
    func testUnsupportedObject() throws {
        let rows: [Node] = [
            .object(["1":1, "2":2]),
            .object(["1":1, "2":2, "3":3]),
            .object([:]),
            .object(["1":1]),
        ]
        
        try postgreSQL.execute("DROP TABLE IF EXISTS foo")
        try postgreSQL.execute("CREATE TABLE foo (id serial, text text)")
        for row in rows {
            try postgreSQL.execute("INSERT INTO foo VALUES (DEFAULT, $1)", [row])
        }
        
        let result = try postgreSQL.execute("SELECT * FROM foo ORDER BY id ASC")
        XCTAssertEqual(result.count, rows.count)
        for resultRow in result {
            let value = resultRow["text"]
            XCTAssertNotNil(value)
            XCTAssertEqual(value, Node.null)
        }
    }
    
    func testUnsupportedOID() throws {
        try postgreSQL.execute("DROP TABLE IF EXISTS foo")
        try postgreSQL.execute("CREATE TABLE foo (id serial, oid oid)")
        try postgreSQL.execute("INSERT INTO foo VALUES (DEFAULT, 1)", nil)
        try postgreSQL.execute("INSERT INTO foo VALUES (DEFAULT, 2)", nil)
        try postgreSQL.execute("INSERT INTO foo VALUES (DEFAULT, 123)", nil)
        try postgreSQL.execute("INSERT INTO foo VALUES (DEFAULT, 456)", nil)
        
        let result = try postgreSQL.execute("SELECT * FROM foo ORDER BY id ASC")
        XCTAssertEqual(result.count, 4)
        for resultRow in result {
            let value = resultRow["oid"]
            XCTAssertNotNil(value)
            
            guard case .bytes(_) = value! else {
                XCTFail("Result should be in bytes")
                return
            }
        }
    }
}
