import XCTest
@testable import PostgreSQL
import Foundation
import Dispatch

class PostgreSQLTests: XCTestCase {
    static let allTests = [
        ("testSelectVersion", testSelectVersion),
        ("testTables", testTables),
        ("testParameterization", testParameterization),
        ("testDataType", testDataType),
        ("testDates", testDates),
        ("testUUID", testUUID),
        ("testInts", testInts),
        ("testFloats", testFloats),
        ("testNumeric", testNumeric),
        ("testNumericAverage", testNumericAverage),
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
        ("testNotification", testNotification),
        ("testNotificationWithPayload", testNotificationWithPayload),
        ("testDispatchNotification", testDispatchNotification),
        ("testDispatchNotificationWithPayload", testDispatchNotificationWithPayload),
        ("testQueryToNode", testQueryToNode)
    ]

    var postgreSQL: PostgreSQL.Database!

    override func setUp() {
        postgreSQL = PostgreSQL.Database.makeTest()
    }

    func testSelectVersion() throws {
        let conn = try postgreSQL.makeConnection()
        
        do {
            let results = try conn.execute("SELECT version(), version(), 1337, 3.14, 'what up', NULL")
            guard let version = results[0, "version"] else {
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

    func testTables() throws {
        let conn = try postgreSQL.makeConnection()
        
        do {
            try conn.execute("DROP TABLE IF EXISTS foo")
            try conn.execute("CREATE TABLE foo (bar INT, baz VARCHAR(16), bla BOOLEAN)")
            try conn.execute("INSERT INTO foo VALUES (42, 'Life', true)")
            try conn.execute("INSERT INTO foo VALUES (1337, 'Elite', false)")
            try conn.execute("INSERT INTO foo VALUES (9, NULL, true)")

            if let resultBar = try conn.execute("SELECT * FROM foo WHERE bar = 42")[0] {
                XCTAssertEqual(resultBar["bar"]?.int, 42)
                XCTAssertEqual(resultBar["baz"]?.string, "Life")
                XCTAssertEqual(resultBar["bla"]?.bool, true)
            } else {
                XCTFail("Could not get bar result")
            }

            if let resultBaz = try conn.execute("SELECT * FROM foo where baz = 'Elite'")[0] {
                XCTAssertEqual(resultBaz["bar"]?.int, 1337)
                XCTAssertEqual(resultBaz["baz"]?.string, "Elite")
                XCTAssertEqual(resultBaz["bla"]?.bool, false)
            } else {
                XCTFail("Could not get baz result")
            }

            if let resultBaz = try conn.execute("SELECT * FROM foo where bar = 9")[0] {
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

    func testParameterization() throws {
        let conn = try postgreSQL.makeConnection()
        
        do {
            try conn.execute("DROP TABLE IF EXISTS parameterization")
            try conn.execute("CREATE TABLE parameterization (d FLOAT8, i INT, s VARCHAR(16), u INT)")

            try conn.execute("INSERT INTO parameterization VALUES ($1, $2, $3, $4)", [.null, .null, "life".makeNode(in: nil), .null])

            try conn.execute("INSERT INTO parameterization VALUES (3.14, NULL, 'pi', NULL)")
            try conn.execute("INSERT INTO parameterization VALUES (NULL, NULL, 'life', 42)")
            try conn.execute("INSERT INTO parameterization VALUES (NULL, -1, 'test', NULL)")

            if let result = try conn.execute("SELECT * FROM parameterization WHERE d = $1", [3.14])[0] {
                XCTAssertEqual(result["d"]?.double, 3.14)
                XCTAssertEqual(result["i"]?.int, nil)
                XCTAssertEqual(result["s"]?.string, "pi")
                XCTAssertEqual(result["u"]?.int, nil)
            } else {
                XCTFail("Could not get pi result")
            }

            if let result = try conn.execute("SELECT * FROM parameterization WHERE u = $1", [42])[0] {
                XCTAssertEqual(result["d"]?.double, nil)
                XCTAssertEqual(result["i"]?.int, nil)
                XCTAssertEqual(result["s"]?.string, "life")
                XCTAssertEqual(result["u"]?.int, 42)
            } else {
                XCTFail("Could not get life result")
            }

            if let result = try conn.execute("SELECT * FROM parameterization WHERE i = $1", [-1])[0] {
                XCTAssertEqual(result["d"]?.double, nil)
                XCTAssertEqual(result["i"]?.int, -1)
                XCTAssertEqual(result["s"]?.string, "test")
                XCTAssertEqual(result["u"]?.int, nil)
            } else {
                XCTFail("Could not get test by int result")
            }

            if let result = try conn.execute("SELECT * FROM parameterization WHERE s = $1", ["test"])[0] {
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
        let conn = try postgreSQL.makeConnection()
        
        let data: [UInt8] = [1, 2, 3, 4, 5, 0, 6, 7, 8, 9, 0]

        try conn.execute("DROP TABLE IF EXISTS foo")
        try conn.execute("CREATE TABLE foo (bar BYTEA)")
        try conn.execute("INSERT INTO foo VALUES ($1)", [.bytes(data)])

        let result = try conn.execute("SELECT * FROM foo")[0]
        XCTAssertNotNil(result)

        let resultBytesNode = result!["bar"]
        XCTAssertNotNil(resultBytesNode)

        XCTAssertEqual(resultBytesNode!, .bytes(data))
    }
    
    func testDates() throws {
        let conn = try postgreSQL.makeConnection()
        
        let rows: [Date] = [
            Date(timeIntervalSince1970: 0),
            Date(timeIntervalSince1970: 1),
            Date(timeIntervalSince1970: 60 * 60 * 24 * 365 * 31),
            Date(timeIntervalSince1970: 60 * 60 * 24 * 4),
            Date(timeIntervalSince1970: 1.52),
            Date(timeIntervalSince1970: 60 * 60 * 24 * 365 + 0.123),
            Date(timeIntervalSince1970: -60 * 60 * 24 * 365 + 0.123),
        ]

        try conn.execute("DROP TABLE IF EXISTS foo")
        try conn.execute("CREATE TABLE foo (id serial, date TIMESTAMP WITH TIME ZONE)")
        for row in rows {
            try conn.execute("INSERT INTO foo VALUES (DEFAULT, $1)", [row.makeNode(in: nil)])
        }

        let result = try conn.execute("SELECT * FROM foo ORDER BY id ASC").array ?? []
        XCTAssertEqual(result.count, rows.count)
        for (i, resultRow) in result.enumerated() {
            let node = resultRow["date"]
            XCTAssertNotNil(node?.date)
            XCTAssertEqual(node!.date!, rows[i])
        }
    }

    func testUUID() throws {
        let conn = try postgreSQL.makeConnection()
        
        let uuidString = "7fe1743a-96a8-417c-b6c2-c8bb20d3017e"

        try conn.execute("DROP TABLE IF EXISTS foo")
        try conn.execute("CREATE TABLE foo (uuid UUID)")
        try conn.execute("INSERT INTO foo VALUES ($1)", [.string(uuidString)])

        let result = try conn.execute("SELECT * FROM foo")[0]
        XCTAssertNotNil(result)
        XCTAssertEqual(result!["uuid"]?.string, uuidString)
    }

    func testInts() throws {
        let conn = try postgreSQL.makeConnection()
        
        let rows: [(Int16, Int32, Int64)] = [
            (1, 2, 3),
            (-1, -2, -3),
            (Int16.min, Int32.min, Int64.min),
            (Int16.max, Int32.max, Int64.max),
        ]

        try conn.execute("DROP TABLE IF EXISTS foo")
        try conn.execute("CREATE TABLE foo (id serial, int2 int2, int4 int4, int8 int8)")
        for row in rows {
            try conn.execute("INSERT INTO foo VALUES (DEFAULT, $1, $2, $3)", [row.0.makeNode(in: nil), row.1.makeNode(in: nil), row.2.makeNode(in: nil)])
        }

        let result = try conn.execute("SELECT * FROM foo ORDER BY id ASC").array ?? []
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
        let conn = try postgreSQL.makeConnection()
        
        let rows: [(Float32, Float64)] = [
            (1, 2),
            (-1, -2),
            (1.23, 2.45),
            (-1.23, -2.45),
            (Float32.min, Float64.min),
            (Float32.max, Float64.max),
        ]

        try conn.execute("DROP TABLE IF EXISTS foo")
        try conn.execute("CREATE TABLE foo (id serial, float4 float4, float8 float8)")
        for row in rows {
            try conn.execute("INSERT INTO foo VALUES (DEFAULT, $1, $2)", [row.0.makeNode(in: nil), row.1.makeNode(in: nil)])
        }

        let result = try conn.execute("SELECT * FROM foo ORDER BY id ASC").array ?? []
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
        let conn = try postgreSQL.makeConnection()
        
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

        try conn.execute("DROP TABLE IF EXISTS foo")
        try conn.execute("CREATE TABLE foo (id serial, numeric numeric)")
        for row in rows {
            try conn.execute("INSERT INTO foo VALUES (DEFAULT, $1)", [row.makeNode(in: nil)])
        }

        let result = try conn.execute("SELECT * FROM foo ORDER BY id ASC").array ?? []
        XCTAssertEqual(result.count, rows.count)
        for (i, resultRow) in result.enumerated() {
            let numeric = resultRow["numeric"]
            XCTAssertNotNil(numeric?.string)
            XCTAssertEqual(numeric!.string!, rows[i])
        }
    }

    func testNumericAverage() throws {
        let conn = try postgreSQL.makeConnection()

        try conn.execute("DROP TABLE IF EXISTS jobs")
        try conn.execute("CREATE TABLE jobs (id SERIAL PRIMARY KEY NOT NULL, title VARCHAR(255) NOT NULL, pay INT8 NOT NULL)")

        try conn.execute("INSERT INTO jobs (title, pay) VALUES ($1, $2)", ["A", 100])
        try conn.execute("INSERT INTO jobs (title, pay) VALUES ($1, $2)", ["A", 200])
        try conn.execute("INSERT INTO jobs (title, pay) VALUES ($1, $2)", ["A", 300])
        try conn.execute("INSERT INTO jobs (title, pay) VALUES ($1, $2)", ["A", 400])
        try conn.execute("INSERT INTO jobs (title, pay) VALUES ($1, $2)", ["A", 500])
        try conn.execute("INSERT INTO jobs (title, pay) VALUES ($1, $2)", ["B", 100])
        try conn.execute("INSERT INTO jobs (title, pay) VALUES ($1, $2)", ["B", 200])
        try conn.execute("INSERT INTO jobs (title, pay) VALUES ($1, $2)", ["B", 900])
        try conn.execute("INSERT INTO jobs (title, pay) VALUES ($1, $2)", ["C", 100])
        try conn.execute("INSERT INTO jobs (title, pay) VALUES ($1, $2)", ["C", 1100])
        try conn.execute("INSERT INTO jobs (title, pay) VALUES ($1, $2)", ["D", 100])
        try conn.execute("INSERT INTO jobs (title, pay) VALUES ($1, $2)", ["E", 500])

        defer {
            _ = try? conn.execute("DROP TABLE IF EXISTS jobs")
        }

        let result = try conn.execute("select title, avg(pay) as average, count(*) as cnt from jobs group by title order by average desc")

        guard let array = result.array else {
            XCTFail("Result was not an array")
            return
        }

        XCTAssertEqual(5, array.count)
        XCTAssertEqual("C", array[0]["title"]?.string)
        XCTAssertEqual("E", array[1]["title"]?.string)
        XCTAssertEqual("B", array[2]["title"]?.string)
        XCTAssertEqual("A", array[3]["title"]?.string)
        XCTAssertEqual("D", array[4]["title"]?.string)
        XCTAssertEqual(2, array[0]["cnt"]?.int)
        XCTAssertEqual(1, array[1]["cnt"]?.int)
        XCTAssertEqual(3, array[2]["cnt"]?.int)
        XCTAssertEqual(5, array[3]["cnt"]?.int)
        XCTAssertEqual(1, array[4]["cnt"]?.int)
        XCTAssertEqual(600, array[0]["average"]?.double)
        XCTAssertEqual(500, array[1]["average"]?.double)
        XCTAssertEqual(400, array[2]["average"]?.double)
        XCTAssertEqual(300, array[3]["average"]?.double)
        XCTAssertEqual(100, array[4]["average"]?.double)
    }

    func testJSON() throws {
        let conn = try postgreSQL.makeConnection()
        
        let rows: [String] = [
            "{}",
            "[]",
            "true",
            "123",
            "456.7891412341",
            "[1, 2, 3, 4, 5, 6]",
            "{\"foo\": \"bar\"}",
        ]

        try conn.execute("DROP TABLE IF EXISTS foo")
        try conn.execute("CREATE TABLE foo (id serial, json json, jsonb jsonb)")
        for row in rows {
            try conn.execute("INSERT INTO foo VALUES (DEFAULT, $1, $2)", [row.makeNode(in: nil), row.makeNode(in: nil)])
        }

        let result = try conn.execute("SELECT * FROM foo ORDER BY id ASC").array ?? []
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
        let conn = try postgreSQL.makeConnection()
        
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

        try conn.execute("DROP TABLE IF EXISTS foo")
        try conn.execute("CREATE TABLE foo (id serial, interval interval)")
        for row in rows {
            try conn.execute("INSERT INTO foo VALUES (DEFAULT, $1)", [row[0].makeNode(in: nil)])
        }

        let result = try conn.execute("SELECT * FROM foo ORDER BY id ASC").array ?? []
        XCTAssertEqual(result.count, rows.count)
        for (i, resultRow) in result.enumerated() {
            let interval = resultRow["interval"]
            XCTAssertNotNil(interval?.string)
            XCTAssertTrue(rows[i].contains(interval!.string!))
        }
    }

    func testPoints() throws {
        let conn = try postgreSQL.makeConnection()
        
        let rows = [
            "(1.2,3.4)",
            "(-1.2,-3.4)",
            "(123.456,-298.135)",
        ]

        try conn.execute("DROP TABLE IF EXISTS foo")
        try conn.execute("CREATE TABLE foo (id serial, point point)")
        for row in rows {
            try conn.execute("INSERT INTO foo VALUES (DEFAULT, $1)", [row.makeNode(in: nil)])
        }

        let result = try conn.execute("SELECT * FROM foo ORDER BY id ASC").array ?? []
        XCTAssertEqual(result.count, rows.count)
        for (i, resultRow) in result.enumerated() {
            let point = resultRow["point"]
            XCTAssertNotNil(point?.string)
            XCTAssertEqual(point!.string!, rows[i])
        }
    }

    func testLineSegments() throws {
        let conn = try postgreSQL.makeConnection()
        
        let rows = [
            "[(1.2,3.4),(-1.2,-3.4)]",
            "[(-1.2,-3.4),(123.467,-298.135)]",
            "[(123.47,-238.123),(1.2,3.4)]",
        ]

        try conn.execute("DROP TABLE IF EXISTS foo")
        try conn.execute("CREATE TABLE foo (id serial, lseg lseg)")
        for row in rows {
            try conn.execute("INSERT INTO foo VALUES (DEFAULT, $1)", [row.makeNode(in: nil)])
        }

        let result = try conn.execute("SELECT * FROM foo ORDER BY id ASC").array ?? []
        XCTAssertEqual(result.count, rows.count)
        for (i, resultRow) in result.enumerated() {
            let lseg = resultRow["lseg"]
            XCTAssertNotNil(lseg?.string)
            XCTAssertEqual(lseg!.string!, rows[i])
        }
    }

    func testPaths() throws {
        let conn = try postgreSQL.makeConnection()
        
        let rows = [
            "[(1.2,3.4),(-1.2,-3.4),(123.67,-598.35)]",
            "((-1.2,-3.4),(12.4567,-298.35))",
            "((123.47,-235.35),(1.2,3.4))",
            "[(1.2,3.4)]",
        ]

        try conn.execute("DROP TABLE IF EXISTS foo")
        try conn.execute("CREATE TABLE foo (id serial, path path)")
        for row in rows {
            try conn.execute("INSERT INTO foo VALUES (DEFAULT, $1)", [row.makeNode(in: nil)])
        }

        let result = try conn.execute("SELECT * FROM foo ORDER BY id ASC").array ?? []
        XCTAssertEqual(result.count, rows.count)
        for (i, resultRow) in result.enumerated() {
            let path = resultRow["path"]
            XCTAssertNotNil(path?.string)
            XCTAssertEqual(path!.string!, rows[i])
        }
    }

    func testBoxes() throws {
        let conn = try postgreSQL.makeConnection()
        
        let rows = [
            "(1.2,3.4),(-1.2,-3.4)",
            "(13.467,-3.4),(-1.2,-598.35)",
            "(12.467,3.4),(1.2,-358.15)",
        ]

        try conn.execute("DROP TABLE IF EXISTS foo")
        try conn.execute("CREATE TABLE foo (id serial, box box)")
        for row in rows {
            try conn.execute("INSERT INTO foo VALUES (DEFAULT, $1)", [row.makeNode(in: nil)])
        }

        let result = try conn.execute("SELECT * FROM foo ORDER BY id ASC").array ?? []
        XCTAssertEqual(result.count, rows.count)
        for (i, resultRow) in result.enumerated() {
            let box = resultRow["box"]
            XCTAssertNotNil(box?.string)
            XCTAssertEqual(box!.string!, rows[i])
        }
    }

    func testPolygons() throws {
        let conn = try postgreSQL.makeConnection()
        
        let rows = [
            "((1.2,3.4),(-1.2,-3.4),(123.46,-358.25))",
            "((-1.2,-3.4),(3.4567,-28.235))",
            "((123.467,-98.123),(1.2,3.4))",
            "((1.2,3.4))",
        ]

        try conn.execute("DROP TABLE IF EXISTS foo")
        try conn.execute("CREATE TABLE foo (id serial, polygon polygon)")
        for row in rows {
            try conn.execute("INSERT INTO foo VALUES (DEFAULT, $1)", [row.makeNode(in: nil)])
        }

        let result = try conn.execute("SELECT * FROM foo ORDER BY id ASC").array ?? []
        XCTAssertEqual(result.count, rows.count)
        for (i, resultRow) in result.enumerated() {
            let polygon = resultRow["polygon"]
            XCTAssertNotNil(polygon?.string)
            XCTAssertEqual(polygon!.string!, rows[i])
        }
    }

    func testCircles() throws {
        let conn = try postgreSQL.makeConnection()
        
        let rows = [
            "<(1.2,3.4),456.7>",
            "<(-1.2,-3.4),98>",
            "<(123.67,-598.15),0.123>",
        ]

        try conn.execute("DROP TABLE IF EXISTS foo")
        try conn.execute("CREATE TABLE foo (id serial, circle circle)")
        for row in rows {
            try conn.execute("INSERT INTO foo VALUES (DEFAULT, $1)", [row.makeNode(in: nil)])
        }

        let result = try conn.execute("SELECT * FROM foo ORDER BY id ASC").array ?? []
        XCTAssertEqual(result.count, rows.count)
        for (i, resultRow) in result.enumerated() {
            let circle = resultRow["circle"]
            XCTAssertNotNil(circle?.string)
            XCTAssertEqual(circle!.string!, rows[i])
        }
    }

    func testInets() throws {
        let conn = try postgreSQL.makeConnection()
        
        let rows = [
            "192.168.100.128",
            "192.168.100.128/25",
            "2001:4f8:3:ba::/64",
            "2001:4f8:3:ba:2e0:81ff:fe22:d1f1",
            "80.60.123.255",
            "0.0.0.0",
            "127.0.0.1",
        ]

        try conn.execute("DROP TABLE IF EXISTS foo")
        try conn.execute("CREATE TABLE foo (id serial, inet inet)")
        for row in rows {
            try conn.execute("INSERT INTO foo VALUES (DEFAULT, $1)", [row.makeNode(in: nil)])
        }

        let result = try conn.execute("SELECT * FROM foo ORDER BY id ASC").array ?? []
        XCTAssertEqual(result.count, rows.count)
        for (i, resultRow) in result.enumerated() {
            let inet = resultRow["inet"]
            XCTAssertNotNil(inet?.string)
            XCTAssertEqual(inet!.string!, rows[i])
        }
    }

    func testCidrs() throws {
        let conn = try postgreSQL.makeConnection()
        
        let rows = [
            "192.168.100.128/32",
            "192.168.100.128/25",
            "2001:4f8:3:ba::/64",
            "2001:4f8:3:ba:2e0:81ff:fe22:d1f1/128",
            "80.60.123.255/32",
            "0.0.0.0/32",
            "127.0.0.1/32",
        ]

        try conn.execute("DROP TABLE IF EXISTS foo")
        try conn.execute("CREATE TABLE foo (id serial, cidr cidr)")
        for row in rows {
            try conn.execute("INSERT INTO foo VALUES (DEFAULT, $1)", [row.makeNode(in: nil)])
        }

        let result = try conn.execute("SELECT * FROM foo ORDER BY id ASC").array ?? []
        XCTAssertEqual(result.count, rows.count)
        for (i, resultRow) in result.enumerated() {
            let cidr = resultRow["cidr"]
            XCTAssertNotNil(cidr?.string)
            XCTAssertEqual(cidr!.string!, rows[i])
        }
    }

    func testMacAddresses() throws {
        let conn = try postgreSQL.makeConnection()
        
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

        try conn.execute("DROP TABLE IF EXISTS foo")
        try conn.execute("CREATE TABLE foo (id serial, macaddr macaddr)")
        for row in rows {
            try conn.execute("INSERT INTO foo VALUES (DEFAULT, $1)", [row.makeNode(in: nil)])
        }

        let result = try conn.execute("SELECT * FROM foo ORDER BY id ASC").array ?? []
        XCTAssertEqual(result.count, rows.count)
        for (i, resultRow) in result.enumerated() {
            let macaddr = resultRow["macaddr"]
            XCTAssertNotNil(macaddr?.string)
            XCTAssertEqual(macaddr!.string!, rows[i])
        }
    }

    func testBitStrings() throws {
        let conn = try postgreSQL.makeConnection()
        
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

        try conn.execute("DROP TABLE IF EXISTS foo")
        try conn.execute("CREATE TABLE foo (id serial, bits bit(5))")
        for row in rows {
            try conn.execute("INSERT INTO foo VALUES (DEFAULT, $1)", [row.makeNode(in: nil)])
        }

        let result = try conn.execute("SELECT * FROM foo ORDER BY id ASC").array ?? []
        XCTAssertEqual(result.count, rows.count)
        for (i, resultRow) in result.enumerated() {
            let bits = resultRow["bits"]
            XCTAssertNotNil(bits?.string)
            XCTAssertEqual(bits!.string!, rows[i])
        }
    }

    func testVarBitStrings() throws {
        let conn = try postgreSQL.makeConnection()
        
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

        try conn.execute("DROP TABLE IF EXISTS foo")
        try conn.execute("CREATE TABLE foo (id serial, bits bit varying)")
        for row in rows {
            try conn.execute("INSERT INTO foo VALUES (DEFAULT, $1)", [row.makeNode(in: nil)])
        }

        let result = try conn.execute("SELECT * FROM foo ORDER BY id ASC").array ?? []
        XCTAssertEqual(result.count, rows.count)
        for (i, resultRow) in result.enumerated() {
            let bits = resultRow["bits"]
            XCTAssertNotNil(bits?.string)
            XCTAssertEqual(bits!.string!, rows[i])
        }
    }

    func testUnsupportedObject() throws {
        let conn = try postgreSQL.makeConnection()
        
        let rows: [Node] = [
            .object(["1":1, "2":2]),
            .object(["1":1, "2":2, "3":3]),
            .object([:]),
            .object(["1":1]),
        ]

        try conn.execute("DROP TABLE IF EXISTS foo")
        try conn.execute("CREATE TABLE foo (id serial, text text)")
        for row in rows {
            try conn.execute("INSERT INTO foo VALUES (DEFAULT, $1)", [row])
        }

        let result = try conn.execute("SELECT * FROM foo ORDER BY id ASC").array ?? []
        XCTAssertEqual(result.count, rows.count)
        for resultRow in result {
            let value = resultRow["text"]
            XCTAssertNotNil(value)
            XCTAssertEqual(value, Node.null)
        }
    }

    func testUnsupportedOID() throws {
        let conn = try postgreSQL.makeConnection()
        
        try conn.execute("DROP TABLE IF EXISTS foo")
        try conn.execute("CREATE TABLE foo (id serial, oid oid)")
        try conn.execute("INSERT INTO foo VALUES (DEFAULT, 1)")
        try conn.execute("INSERT INTO foo VALUES (DEFAULT, 2)")
        try conn.execute("INSERT INTO foo VALUES (DEFAULT, 123)")
        try conn.execute("INSERT INTO foo VALUES (DEFAULT, 456)")

        let result = try conn.execute("SELECT * FROM foo ORDER BY id ASC").array ?? []
        XCTAssertEqual(result.count, 4)
        for resultRow in result {
            let value = resultRow["oid"]
            XCTAssertNotNil(value)
        }
    }

    func testNotification() throws {
        let conn1 = try postgreSQL.makeConnection()
        let conn2 = try postgreSQL.makeConnection()
        
        let testExpectation = expectation(description: "Receive notification")

        conn1.listen(toChannel: "test_channel1") { (notification, error, stop) in
            XCTAssertEqual(notification?.channel, "test_channel1")
            XCTAssertNil(notification?.payload)
            XCTAssertNil(error)

            testExpectation.fulfill()
            stop = true
        }

        sleep(1)

        try conn2.notify(channel: "test_channel1", payload: nil)

        waitForExpectations(timeout: 5)
    }

	func testDispatchNotification() throws {
		let conn1 = try postgreSQL.makeConnection()
		let conn2 = try postgreSQL.makeConnection()
		
		let testExpectation = expectation(description: "Receive notification")
		
		let queue = DispatchQueue.global()
		var source: DispatchSourceRead?
		source = try! conn1.listen(toChannel: "test_channel1", queue: queue) { (notification, error) in
			XCTAssertEqual(notification?.channel, "test_channel1")
			XCTAssertNil(notification?.payload)
			XCTAssertNil(error)
			
			testExpectation.fulfill()
			source?.cancel()
		}
		source?.resume()
		
		sleep(1)
		
		try conn2.notify(channel: "test_channel1", payload: nil)
		
		waitForExpectations(timeout: 5)
	}

    func testNotificationWithPayload() throws {
        let conn1 = try postgreSQL.makeConnection()
        let conn2 = try postgreSQL.makeConnection()
        
        let testExpectation = expectation(description: "Receive notification with payload")

        conn1.listen(toChannel: "test_channel2") { (notification, error, stop) in
            XCTAssertEqual(notification?.channel, "test_channel2")
            XCTAssertEqual(notification?.payload, "test_payload")
            XCTAssertNil(error)

            testExpectation.fulfill()
            stop = true
        }

        sleep(1)

        try conn2.notify(channel: "test_channel2", payload: "test_payload")

        waitForExpectations(timeout: 5)
    }

	func testDispatchNotificationWithPayload() throws {
		let conn1 = try postgreSQL.makeConnection()
		let conn2 = try postgreSQL.makeConnection()
		
		let testExpectation = expectation(description: "Receive notification with payload")
		
		let queue = DispatchQueue.global()
		var source: DispatchSourceRead?
		source = try! conn1.listen(toChannel: "test_channel2", queue: queue) { (notification, error) in
			XCTAssertEqual(notification?.channel, "test_channel2")
			XCTAssertEqual(notification?.payload, "test_payload")
			XCTAssertNotNil(notification?.payload)
			XCTAssertNil(error)
			
			testExpectation.fulfill()
			source?.cancel()
		}
		source?.resume()
		
		sleep(1)
		
		try conn2.notify(channel: "test_channel2", payload: "test_payload")
		
		waitForExpectations(timeout: 5)
	}

    func testQueryToNode() throws {
        let conn = try postgreSQL.makeConnection()
        
        let results = try conn.execute("SELECT version()")
        XCTAssertNotNil(results.array?[0].object?["version"]?.string)
    }
    
    func testEmptyQuery() throws {
        let conn = try postgreSQL.makeConnection()
  
        do {
            try conn.execute("")
            XCTFail("This query should not succeed")
        }
        catch PostgresSQLStatusError.emptyQuery {
            // Should end up here
        }
        catch {
            throw error
        }
    }
    
    func testInvalidQuery() throws {
        let conn = try postgreSQL.makeConnection()
        
        do {
            try conn.execute("SELECT * FROM nothing")
            XCTFail("This query should not succeed")
        }
        catch let error as PostgreSQLError {
            XCTAssertEqual(error.code, PostgreSQLError.Code.undefinedTable)
        }
        catch {
            throw error
        }
    }
    
    func testTransactionSuccess() throws {
        let conn = try postgreSQL.makeConnection()
        
        let isolationLevels: [Connection.TransactionIsolationLevel] = [
            .readCommitted,
            .repeatableRead,
            .serializable,
        ]
        
        for isolationLevel in isolationLevels {
            try conn.execute("DROP TABLE IF EXISTS foo")
            try conn.execute("CREATE TABLE foo (bar INT, baz VARCHAR(16), bla BOOLEAN)")
            
            try conn.transaction(isolationLevel: isolationLevel) {
                try conn.execute("INSERT INTO foo VALUES (42, 'Life', true)")
                try conn.execute("INSERT INTO foo VALUES (1337, 'Elite', false)")
                try conn.execute("INSERT INTO foo VALUES (9, NULL, true)")
            }
            
            let resuls = try conn.execute("SELECT * FROM foo").array ?? []
            XCTAssertEqual(resuls.count, 3)
        }
    }
    
    func testTransactionFailure() throws {
        let conn = try postgreSQL.makeConnection()
        
        enum TestError : Error {
            case failure
        }
        
        let isolationLevels: [Connection.TransactionIsolationLevel] = [
            .readCommitted,
            .repeatableRead,
            .serializable,
        ]
        
        for isolationLevel in isolationLevels {
            try conn.execute("DROP TABLE IF EXISTS foo")
            try conn.execute("CREATE TABLE foo (bar INT, baz VARCHAR(16), bla BOOLEAN)")
            
            do {
                try conn.transaction(isolationLevel: isolationLevel) {
                    try conn.execute("INSERT INTO foo VALUES (42, 'Life', true)")
                    try conn.execute("INSERT INTO foo VALUES (1337, 'Elite', false)")
                    try conn.execute("INSERT INTO foo VALUES (9, NULL, true)")
                    
                    throw TestError.failure
                }
                
                XCTFail("transaction should throw error")
            }
            catch TestError.failure {
                
            }
            catch {
                XCTFail("Should not fail with unknown error")
            }
            
            let resuls = try conn.execute("SELECT * FROM foo").array ?? []
            XCTAssertEqual(resuls.count, 0)
        }
    }
}
