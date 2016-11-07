import XCTest
@testable import PostgreSQL
import Foundation

class BinaryUtilsTests: XCTestCase {
    static let allTests = [
        ("testConvert", testConvert),
        ("testParseBytes", testParseBytes),
        ("testParseString", testParseString),
        ("testParseInt16", testParseInt16),
        ("testParseInt32", testParseInt32),
        ("testParseInt64", testParseInt64),
        ("testParseFloat32", testParseFloat32),
        ("testParseFloat64", testParseFloat64),
        ("testParseNumeric", testParseNumeric),
        ("testParseIntegerTimestamp", testParseIntegerTimestamp),
        ("testParseFloatTimestamp", testParseFloatTimestamp),
        ("testParseIntegerInterval", testParseIntegerInterval),
        ("testParseFloatInterval", testParseFloatInterval),
        ("testParseUUID", testParseUUID),
        ("testParsePoint", testParsePoint),
        ("testParseLineSegment", testParseLineSegment),
        ("testParsePath", testParsePath),
        ("testParseBox", testParseBox),
        ("testParsePolygon", testParsePolygon),
        ("testParseCircle", testParseCircle),
        ("testParseIPAddress", testParseIPAddress),
        ("testParseMacAddress", testParseMacAddress),
        ("testParseBitString", testParseBitString),
    ]
    
    func testConvert() {
        var int16: Int16 = 123
        let convertedInt16: Int16 = withUnsafeMutablePointer(to: &int16) {
            $0.withMemoryRebound(to: Int8.self, capacity: MemoryLayout.size(ofValue: int16)) { value in
                return PostgresBinaryUtils.convert(value)
            }
        }
        XCTAssertEqual(int16, convertedInt16)
        
        var int32: Int32 = Int32.max
        let convertedInt32: Int32 = withUnsafeMutablePointer(to: &int32) {
            $0.withMemoryRebound(to: Int8.self, capacity: MemoryLayout.size(ofValue: int32)) { value in
                return PostgresBinaryUtils.convert(value)
            }
        }
        XCTAssertEqual(int32, convertedInt32)
        
        var uint64: UInt64 = UInt64.min
        let convertedUInt64: UInt64 = withUnsafeMutablePointer(to: &uint64) {
            $0.withMemoryRebound(to: Int8.self, capacity: MemoryLayout.size(ofValue: uint64)) { value in
                return PostgresBinaryUtils.convert(value)
            }
        }
        XCTAssertEqual(uint64, convertedUInt64)
        
        var double: Double = 123.456
        let convertedDouble: Double = withUnsafeMutablePointer(to: &double) {
            $0.withMemoryRebound(to: Int8.self, capacity: MemoryLayout.size(ofValue: double)) { value in
                return PostgresBinaryUtils.convert(value)
            }
        }
        XCTAssertEqual(double, convertedDouble)
        
        var uuid = UUID().uuid
        let convertedUUID: uuid_t = withUnsafeMutablePointer(to: &uuid) {
            $0.withMemoryRebound(to: Int8.self, capacity: MemoryLayout.size(ofValue: uuid)) { value in
                return PostgresBinaryUtils.convert(value)
            }
        }
        XCTAssertEqual(UUID(uuid: uuid), UUID(uuid: convertedUUID))
    }
    
    func testParseBytes() {
        let testByteArrays: [[UInt8]] = [
            [0xaa, 0xbb, 0x01, 0x00, 0x02, 0xFA],
            [0x12, 0x34, 0x56, 0x78],
            [],
        ]
        
        for bytes in testByteArrays {
            var int8Bytes = bytes.map { Int8(bitPattern: $0) }
            let parsedBytes = PostgresBinaryUtils.parseBytes(value: &int8Bytes, length: bytes.count)
            XCTAssertEqual(bytes, parsedBytes)
        }
    }
    
    func testParseString() {
        let testStrings: [String] = [
            "This a test string",
            "",
            "String with one\ntwo\nthree lines.",
            "A string with some emoticons 😎 👻 🌮",
        ]
        
        // Test null-terminated
        for string in testStrings {
            var stringData = Array(string.utf8CString)
            let parsedString = PostgresBinaryUtils.parseString(value: &stringData, length: stringData.count - 1)
            XCTAssertEqual(string, parsedString)
        }
        
        // Test non-null-terminated
        for string in testStrings {
            var stringData = Array(string.utf8CString)
            stringData.removeLast() // Remove NULL
            let parsedString = PostgresBinaryUtils.parseString(value: &stringData, length: stringData.count)
            XCTAssertEqual(string, parsedString)
        }
    }
    
    func testParseInt16() {
        let testInts: [Int16] = [0, 1, 1, 123, 999, Int16.min, Int16.max]
        
        for int in testInts {
            var bigEndian = int.bigEndian
            let convertedInt: Int16 = withUnsafeMutablePointer(to: &bigEndian) {
                $0.withMemoryRebound(to: Int8.self, capacity: MemoryLayout.size(ofValue: bigEndian)) { value in
                    return PostgresBinaryUtils.parseInt16(value: value)
                }
            }
            XCTAssertEqual(int, convertedInt)
        }
    }
    
    func testParseInt32() {
        let testInts: [Int32] = [0, 1, 1, 123, 999, Int32.min, Int32.max]
        
        for int in testInts {
            var bigEndian = int.bigEndian
            let convertedInt: Int32 = withUnsafeMutablePointer(to: &bigEndian) {
                $0.withMemoryRebound(to: Int8.self, capacity: MemoryLayout.size(ofValue: bigEndian)) { value in
                    return PostgresBinaryUtils.parseInt32(value: value)
                }
            }
            XCTAssertEqual(int, convertedInt)
        }
    }
    
    func testParseInt64() {
        let testInts: [Int64] = [0, 1, 1, 123, 999, Int64.min, Int64.max]
        
        for int in testInts {
            var bigEndian = int.bigEndian
            let convertedInt: Int64 = withUnsafeMutablePointer(to: &bigEndian) {
                $0.withMemoryRebound(to: Int8.self, capacity: MemoryLayout.size(ofValue: bigEndian)) { value in
                    return PostgresBinaryUtils.parseInt64(value: value)
                }
            }
            XCTAssertEqual(int, convertedInt)
        }
    }
    
    func testParseFloat32() {
        let testFloats: [Float32] = [0, 1, 1, 123, 999, 1.23, -456.789, Float32.min, Float32.max]
        
        for float in testFloats {
            var bigEndian = float.bigEndian
            let convertedFloat: Float32 = withUnsafeMutablePointer(to: &bigEndian) {
                $0.withMemoryRebound(to: Int8.self, capacity: MemoryLayout.size(ofValue: bigEndian)) { value in
                    return PostgresBinaryUtils.parseFloat32(value: value)
                }
            }
            XCTAssertEqual(float, convertedFloat)
        }
    }
    
    func testParseFloat64() {
        let testFloats: [Float64] = [0, 1, 1, 123, 999, 1.23, -456.789, Float64.min, Float64.max]
        
        for float in testFloats {
            var bigEndian = float.bigEndian
            let convertedFloat: Float64 = withUnsafeMutablePointer(to: &bigEndian) {
                $0.withMemoryRebound(to: Int8.self, capacity: MemoryLayout.size(ofValue: bigEndian)) { value in
                    return PostgresBinaryUtils.parseFloat64(value: value)
                }
            }
            XCTAssertEqual(float, convertedFloat)
        }
    }
    
    func testParseNumeric() {
        let numericTests = [
            ("0000000000000000", "0"),
            ("00010000000000000001", "1"),
            ("00010000400000000001", "-1"),
            ("00000000c0000000", "NaN"),
            ("0006000200000009000109291a8504d2162e2328", "123456789.123456789"),
            ("0006000340000006270f270f270f270f270f26ac", "-9999999999999999.999999"),
            ("0001000000000000007b", "123"),
            ("0002ffff0000000526941388", "0.98765"),
            ("0002ffff4000000526941388", "-0.98765"),
        ]
        
        for (hexString, numericString) in numericTests {
            var bytes = hexString.hexStringBytes
            let parsedString = PostgresBinaryUtils.parseNumeric(value: &bytes)
            XCTAssertEqual(numericString, parsedString)
        }
    }
    
    func testParseIntegerTimestamp() {
        let integerTimestampTests = [
            ("0001e329a49e5cf8", "2016-10-31 15:29:31.716856".postgreSQLParsedDate),
            ("0000000000000000", "2000-01-01 00:00:00.000".postgreSQLParsedDate),
            ("0380e6f773642000", "9999-12-31 00:00:00.000".postgreSQLParsedDate),
            ("ffaa2a39dbeb0a80", "1234-05-21 12:13:14.000".postgreSQLParsedDate),
        ]

        for (hexString, timestamp) in integerTimestampTests {
            var bytes = hexString.hexStringBytes
            let parsedDate = PostgresBinaryUtils.parseTimetamp(value: &bytes, isInteger: true)
            XCTAssertEqualWithAccuracy(timestamp.timeIntervalSince1970, parsedDate.timeIntervalSince1970, accuracy: 0.001)
        }
    }
    
    func testParseFloatTimestamp() {
        let floatTimestampTests = [
            ("41bfaa1fdbb783e0", "2016-10-31 15:29:31.716856".postgreSQLParsedDate),
            ("0000000000000000", "2000-01-01 00:00:00.000".postgreSQLParsedDate),
            ("424d63c2d6400000", "9999-12-31 00:00:00.000".postgreSQLParsedDate),
            ("c216804b02980000", "1234-05-21 12:13:14.000".postgreSQLParsedDate),
        ]

        for (hexString, timestamp) in floatTimestampTests {
            var bytes = hexString.hexStringBytes
            let parsedDate = PostgresBinaryUtils.parseTimetamp(value: &bytes, isInteger: false)
            XCTAssertEqualWithAccuracy(timestamp.timeIntervalSince1970, parsedDate.timeIntervalSince1970, accuracy: 0.001)
        }
    }
    
    func testParseIntegerInterval() {
        let intervalTests = [
            ("00000000000f42400000000000000000", ["00:00:01", "0:0:1"]),
            ("00000000000000000000000000000000", ["00:00:00", "0:0:0"]),
            ("0000000000000000000000020000002d", ["3 years 9 mons 2 days"]),
            ("0000000000b8fb960000000100000011", ["1 year 5 mons 1 day 00:00:12.12303", "1 year 5 mons 1 day 0:0:12.123"]),
            ("0000000000000000000000000000000c", ["1 year"]),
            ("00000000000000000000000000000018", ["2 years"]),
            ("00000000000000000000000100000000", ["1 day"]),
            ("00000000000000000000000200000000", ["2 days"]),
            ("00000000000000000000000000000001", ["1 mon"]),
            ("00000000000000000000000000000002", ["2 mons"]),
            ("fffffffffff0bdc00000000000000000", ["-00:00:01", "-0:0:1"]),
            ("0000000000000000ffffffff00000000", ["-1 days"]),
            ("000000000000000000000001fffffff5", ["-11 mons +1 day"]),
        ]

        for (hexString, intervals) in intervalTests {
            var bytes = hexString.hexStringBytes
            let parsedString = PostgresBinaryUtils.parseInterval(value: &bytes, timeIsInteger: true)
            XCTAssertTrue(intervals.contains(parsedString))
        }
    }
    
    func testParseFloatInterval() {
        let intervalTests = [
            ("3ff00000000000000000000000000000", ["00:00:01", "0:0:1"]),
            ("00000000000000000000000000000000", ["00:00:00", "0:0:0"]),
            ("0000000000000000000000020000002d", ["3 years 9 mons 2 days"]),
            ("40283efdc9c4da900000000100000011", ["1 year 5 mons 1 day 00:00:12.12303", "1 year 5 mons 1 day 0:0:12.123"]),
            ("0000000000000000000000000000000c", ["1 year"]),
            ("00000000000000000000000000000018", ["2 years"]),
            ("00000000000000000000000100000000", ["1 day"]),
            ("00000000000000000000000200000000", ["2 days"]),
            ("00000000000000000000000000000001", ["1 mon"]),
            ("00000000000000000000000000000002", ["2 mons"]),
            ("bff00000000000000000000000000000", ["-00:00:01", "-0:0:1"]),
            ("0000000000000000ffffffff00000000", ["-1 days"]),
            ("000000000000000000000001fffffff5", ["-11 mons +1 day"]),
        ]

        for (hexString, intervals) in intervalTests {
            var bytes = hexString.hexStringBytes
            let parsedString = PostgresBinaryUtils.parseInterval(value: &bytes, timeIsInteger: false)
            XCTAssertTrue(intervals.contains(parsedString))
        }
    }
    
    func testParseUUID() {
        let uuids = [
            "5a9279a1-ce1a-429c-8b3c-21c101e748a9",
            "74da9128-6aa6-43ec-84be-f564434ff4f1",
            "c6508ddd-c9dd-40ed-a378-f30c21022d36",
            "fdb8e723-a456-4ab3-b074-ea270a46322d",
            "bbee7f8e-1e39-4504-9565-e5df447b7a3e",
            "5d0bf4f5-c924-438e-9664-fcf302d9c793",
            "9eb40cb4-9520-4abd-ada6-4a2c9b9b06b9",
            "b5434cf4-05dd-4d62-bd52-bcc02e270ed0",
            "d839789e-f6fe-446f-93b1-5f9521c188fd",
            "58ffb8e9-8530-490a-afc1-d01b93b29264",
        ]
        
        for uuid in uuids {
            var bytes = uuid.replacingOccurrences(of: "-", with: "").hexStringBytes
            let parsedString = PostgresBinaryUtils.parseUUID(value: &bytes)
            XCTAssertEqual(uuid, parsedString)
        }
    }
    
    func testParsePoint() {
        let points = [
            ("3ff3333333333333400b333333333333", "(1.2,3.4)"),
            ("bff3333333333333c00b333333333333", "(-1.2,-3.4)"),
            ("405edd2f1a9fbe77c078e5999999999a", "(123.456,-398.35)"),
        ]
        
        for (hexString, point) in points {
            var bytes = hexString.hexStringBytes
            let parsedString = PostgresBinaryUtils.parsePoint(value: &bytes)
            XCTAssertEqual(point, parsedString)
        }
    }
    
    func testParseLineSegment() {
        let lineSegments = [
            ("3ff3333333333333400b333333333333bff3333333333333c00b333333333333", "[(1.2,3.4),(-1.2,-3.4)]"),
            ("bff3333333333333c00b333333333333405edd2f1a9fbe77c078e5999999999a", "[(-1.2,-3.4),(123.456,-398.35)]"),
            ("405edd2f1a9fbe77c078e5999999999a3ff3333333333333400b333333333333", "[(123.456,-398.35),(1.2,3.4)]"),
        ]
        
        for (hexString, lineSegment) in lineSegments {
            var bytes = hexString.hexStringBytes
            let parsedString = PostgresBinaryUtils.parseLineSegment(value: &bytes)
            XCTAssertEqual(lineSegment, parsedString)
        }
    }
    
    func testParsePath() {
        let paths = [
            ("00000000033ff3333333333333400b333333333333bff3333333333333c00b333333333333405edd2f1a9fbe77c078e5999999999a", "[(1.2,3.4),(-1.2,-3.4),(123.456,-398.35)]"),
            ("0100000002bff3333333333333c00b333333333333405edd2f1a9fbe77c078e5999999999a", "((-1.2,-3.4),(123.456,-398.35))"),
            ("0100000002405edd2f1a9fbe77c078e5999999999a3ff3333333333333400b333333333333", "((123.456,-398.35),(1.2,3.4))"),
            ("00000000013ff3333333333333400b333333333333", "[(1.2,3.4)]"),
            ("0000000000", "[]"),
            ("0100000000", "()"),
        ]
        
        for (hexString, path) in paths {
            var bytes = hexString.hexStringBytes
            let parsedString = PostgresBinaryUtils.parsePath(value: &bytes)
            XCTAssertEqual(path, parsedString)
        }
    }
    
    func testParseBox() {
        let boxes = [
            ("3ff3333333333333400b333333333333bff3333333333333c00b333333333333", "(1.2,3.4),(-1.2,-3.4)"),
            ("bff3333333333333c00b333333333333405edd2f1a9fbe77c078e5999999999a", "(-1.2,-3.4),(123.456,-398.35)"),
            ("405edd2f1a9fbe77c078e5999999999a3ff3333333333333400b333333333333", "(123.456,-398.35),(1.2,3.4)"),
        ]
        
        for (hexString, box) in boxes {
            var bytes = hexString.hexStringBytes
            let parsedString = PostgresBinaryUtils.parseBox(value: &bytes)
            XCTAssertEqual(box, parsedString)
        }
    }
    
    func testParsePolygon() {
        let polygons = [
            ("000000033ff3333333333333400b333333333333bff3333333333333c00b333333333333405edd2f1a9fbe77c078e5999999999a", "((1.2,3.4),(-1.2,-3.4),(123.456,-398.35))"),
            ("00000002bff3333333333333c00b333333333333405edd2f1a9fbe77c078e5999999999a", "((-1.2,-3.4),(123.456,-398.35))"),
            ("00000002405edd2f1a9fbe77c078e5999999999a3ff3333333333333400b333333333333", "((123.456,-398.35),(1.2,3.4))"),
            ("000000013ff3333333333333400b333333333333", "((1.2,3.4))"),
            ("00000000", "()"),
        ]
        
        for (hexString, polygon) in polygons {
            var bytes = hexString.hexStringBytes
            let parsedString = PostgresBinaryUtils.parsePolygon(value: &bytes)
            XCTAssertEqual(polygon, parsedString)
        }
    }
    
    func testParseCircle() {
        let points = [
            ("3ff3333333333333400b333333333333407c8b3333333333", "<(1.2,3.4),456.7>"),
            ("bff3333333333333c00b3333333333334058800000000000", "<(-1.2,-3.4),98>"),
            ("405edd2f1a9fbe77c078e5999999999a3fbf7ced916872b0", "<(123.456,-398.35),0.123>"),
        ]
        
        for (hexString, point) in points {
            var bytes = hexString.hexStringBytes
            let parsedString = PostgresBinaryUtils.parseCircle(value: &bytes)
            XCTAssertEqual(point, parsedString)
        }
    }
    
    func testParseIPAddress() {
        let ipAddressess = [
            ("02200004c0a86480", "192.168.100.128"),
            ("02190004c0a86480", "192.168.100.128/25"),
            ("03400010200104f8000300ba0000000000000000", "2001:4f8:3:ba::/64"),
            ("03800010200104f8000300ba02e081fffe22d1f1", "2001:4f8:3:ba:2e0:81ff:fe22:d1f1"),
            ("02200004503c7bff", "80.60.123.255"),
            ("0220000400000000", "0.0.0.0"),
            ("022000047f000001", "127.0.0.1"),
            ("02200104c0a86480", "192.168.100.128/32"),
            ("02190104c0a86480", "192.168.100.128/25"),
            ("03400110200104f8000300ba0000000000000000", "2001:4f8:3:ba::/64"),
            ("03800110200104f8000300ba02e081fffe22d1f1", "2001:4f8:3:ba:2e0:81ff:fe22:d1f1/128"),
            ("02200104503c7bff", "80.60.123.255/32"),
            ("0220010400000000", "0.0.0.0/32"),
            ("022001047f000001", "127.0.0.1/32"),
        ]
        
        for (hexString, ipAddress) in ipAddressess {
            var bytes = hexString.hexStringBytes
            let parsedString = PostgresBinaryUtils.parseIPAddress(value: &bytes)
            XCTAssertEqual(ipAddress, parsedString)
        }
    }
    
    func testParseMacAddress() {
        let macAddressess = [
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
        
        for macAddress in macAddressess {
            var bytes = macAddress.replacingOccurrences(of: ":", with: "").hexStringBytes
            let parsedString = PostgresBinaryUtils.parseMacAddress(value: &bytes)
            XCTAssertEqual(macAddress, parsedString)
        }
    }
    
    func testParseBitString() {
        let bitStrings = [
            ("0000000100", "0"),
            ("0000000180", "1"),
            ("0000000240", "01"),
            ("00000004b0", "1011"),
            ("0000000430", "0011"),
            ("0000000865", "01100101"),
            ("0000002cd238ac8d89e0", "11010010001110001010110010001101100010011110"),
            ("0000000800", "00000000"),
            ("00000008ff", "11111111"),
            ("0000000b0000", "00000000000"),
            ("0000000affc0", "1111111111"),
        ]
        
        for (hexString, bitString) in bitStrings {
            var bytes = hexString.hexStringBytes
            let parsedString = PostgresBinaryUtils.parseBitString(value: &bytes, length: bytes.count)
            XCTAssertEqual(bitString, parsedString)
        }
    }
}
