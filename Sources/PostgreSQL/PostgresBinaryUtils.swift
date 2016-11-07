import Foundation
import Core

extension UInt8 {
    var lowercaseHexPair: String {
        let hexString = String(self, radix: 16, uppercase: false)
        return String(repeating: "0", count: 2 - hexString.characters.count) + hexString
    }
    
    var lowercaseBinaryString: String {
        let bitString = String(self, radix: 2, uppercase: false)
        return String(repeating: "0", count: 8 - bitString.characters.count) + bitString
    }
}

extension Float32 {
    init(bigEndian: Float32) {
        let int = UInt32(bigEndian: bigEndian.bitPattern)
        self = Float32(bitPattern: int)
    }
    
    var bigEndian: Float32 {
        return Float32(bitPattern: bitPattern.bigEndian)
    }
    
    var littleEndian: Float32 {
        return Float32(bitPattern: bitPattern.littleEndian)
    }
    
    var byteSwapped: Float32 {
        return Float32(bitPattern: bitPattern.byteSwapped)
    }
}

extension Float64 {
    init(bigEndian: Float64) {
        let int = UInt64(bigEndian: bigEndian.bitPattern)
        self = Float64(bitPattern: int)
    }
    
    var bigEndian: Float64 {
        return Float64(bitPattern: bitPattern.bigEndian)
    }
    
    var littleEndian: Float64 {
        return Float64(bitPattern: bitPattern.littleEndian)
    }
    
    var byteSwapped: Float64 {
        return Float64(bitPattern: bitPattern.byteSwapped)
    }
}

/// Most information for parsing binary formats has been retrieved from the following links:
/// - https://www.postgresql.org/docs/9.6/static/datatype.html (Data types)
/// - https://github.com/postgres/postgres/tree/55c3391d1e6a201b5b891781d21fe682a8c64fe6/src/backend/utils/adt (Backend sending code)
struct PostgresBinaryUtils {
    
    // MARK: - Formatters
    
    struct Formatters {
        private static func formatter(format: String, forceUTC: Bool) -> DateFormatter {
            let formatter = DateFormatter()
            if forceUTC {
                formatter.timeZone = TimeZone(abbreviation: "UTC")
            }
            formatter.dateFormat = format
            return formatter
        }
        
        private static let timestamp: DateFormatter = formatter(format: "yyyy-MM-dd HH:mm:ss.SSS", forceUTC: true)
        private static let timestamptz: DateFormatter = formatter(format: "yyyy-MM-dd HH:mm:ss.SSSX", forceUTC: false)
        
        private static let date: DateFormatter = formatter(format: "yyyy-MM-dd", forceUTC: false)
        
        private static let time: DateFormatter = formatter(format: "HH:mm:ss.SSS", forceUTC: true)
        private static let timetz: DateFormatter = formatter(format: "HH:mm:ss.SSSX", forceUTC: false)
        
        static func dateFormatter(for oid: OID) -> DateFormatter {
            switch oid {
            case .date:
                return date
            case .time:
                return time
            case .timetz:
                return timetz
            case .timestamp:
                return timestamp
            case .timestamptz:
                return timestamptz
            default:
                return timestamptz
            }
        }
        
        static let interval: NumberFormatter = {
            let formatter = NumberFormatter()
            formatter.numberStyle = .decimal
            formatter.groupingSeparator = ""
            formatter.decimalSeparator = "."
            formatter.minimumIntegerDigits = 2
            formatter.maximumIntegerDigits = 2
            formatter.minimumFractionDigits = 0
            formatter.maximumFractionDigits = 6
            return formatter
        }()
        
        static let geometry: NumberFormatter = {
            let formatter = NumberFormatter()
            formatter.numberStyle = .decimal
            formatter.groupingSeparator = ""
            formatter.decimalSeparator = "."
            formatter.minimumFractionDigits = 0
            formatter.maximumFractionDigits = 14
            return formatter
        }()
    }
    
    // MARK: - Convert
    
    static func convert<T>(_ value: UnsafeMutablePointer<Int8>) -> T {
        return value.withMemoryRebound(to: T.self, capacity: 1) {
            $0.pointee
        }
    }
    
    static func parseBytes(value: UnsafeMutablePointer<Int8>, length: Int) -> [UInt8] {
        var uint8Bytes: [UInt8] = []
        uint8Bytes.reserveCapacity(length)
        for i in 0..<length {
            uint8Bytes.append(UInt8(bitPattern: value[i]))
        }
        return uint8Bytes
    }
    
    static func valueToByteArray<T>(_ value: inout T) -> [Int8] {
        let size = MemoryLayout.size(ofValue: value)
        return withUnsafePointer(to: &value) { valuePointer in
            return valuePointer.withMemoryRebound(to: Int8.self, capacity: size) { bytePointer in
                return UnsafeBufferPointer(start: bytePointer, count: size).array
            }
        }
    }
    
    // MARK: - String
    
    
    /// Parses a non-null terminated string with a given length from an Int8 pointer into a string
    ///
    /// - Parameters:
    ///   - value: The pointer to the string byte array.
    ///   - length: The length of the string (excluding null terminator if there is one).
    /// - Returns: The parsed string.
    static func parseString(value: UnsafeMutablePointer<Int8>, length: Int) -> String {
        // As strings might not be null terminated, use `init(bytes:encoding:)` with buffer pointer
        let bufferPointer = value.withMemoryRebound(to: UInt8.self, capacity: length) {
            UnsafeBufferPointer(start: $0, count: length)
        }
        guard let string = String(bytes: bufferPointer, encoding: .utf8) else {
            print("Could not parse string as UTF8, returning an empty string.")
            return ""
        }
        return string
    }
    
    // MARK: - Int
    
    static func parseInt16(value: UnsafeMutablePointer<Int8>) -> Int16 {
        return Int16(bigEndian: convert(value))
    }
    
    static func parseInt32(value: UnsafeMutablePointer<Int8>) -> Int32 {
        return Int32(bigEndian: convert(value))
    }
    
    static func parseInt64(value: UnsafeMutablePointer<Int8>) -> Int64 {
        return Int64(bigEndian: convert(value))
    }
    
    // MARK: - Float
    
    static func parseFloat32(value: UnsafeMutablePointer<Int8>) -> Float32 {
        return Float32(bigEndian: convert(value))
    }
    
    static func parseFloat64(value: UnsafeMutablePointer<Int8>) -> Float64 {
        return Float64(bigEndian: convert(value))
    }
    
    // MARK: - Numberic
    
    struct NumericConstants {
        static let signNaN: Int16 = -16384
        static let signNegative: Int16 = 16384
        static let decDigits = 4
    }
    
    static func parseNumeric(value: UnsafeMutablePointer<Int8>) -> String {
        let sign = parseInt16(value: value.advanced(by: 4))
        
        // Check for NaN
        guard sign != NumericConstants.signNaN else {
            return "NaN"
        }
        
        // Check that we actually have some digits
        let numberOfDigits = Int(parseInt16(value: value))
        guard numberOfDigits > 0 else {
            return "0"
        }
        
        let dscale = Int(parseInt16(value: value.advanced(by: 6)))
        
        // Add all digits to a string
        var number: String = ""
        for i in 0..<numberOfDigits {
            let int16 = parseInt16(value: value.advanced(by: 8 + i * 2))
            let stringDigits = String(int16)
            
            if i == 0 {
                number += stringDigits
            }
            else {
                // The number of digits should be 4 (DEC_DIGITS),
                // so pad if necessary.
                number += String(repeating: "0", count: 4 - stringDigits.characters.count) + stringDigits
            }
        }
        
        if dscale > 0 {
            // Make sure we have enough decimal digits by pre-padding with zeros
            if number.characters.count < dscale {
                number = String(repeating: "0", count: dscale - number.characters.count) + number
            }
            else {
                // Remove any trailing zeros
                while number.hasSuffix("0") {
                    number.remove(at: number.index(number.endIndex, offsetBy: -1))
                }
            }
            
            // Insert decimal point
            number.insert(".", at: number.index(number.endIndex, offsetBy: -dscale))
            
            // If we have at least a zero before the decimal point
            if dscale == number.characters.count - 1 {
                number = "0"+number
            }
        }
        
        // Make number negative if necessary
        if sign == NumericConstants.signNegative {
            number = "-"+number
        }
        
        return number
    }
    
    // MARK: - Date / Time
    
    struct TimestampConstants {
        static let referenceDate: Date = {
            let components = DateComponents(year: 2000, month: 1, day: 1)
            var calendar = Calendar(identifier: .gregorian)
            calendar.timeZone = TimeZone(abbreviation: "UTC")!
            return calendar.date(from: components)!
        }()
    }
    
    static func parseTimetamp(value: UnsafeMutablePointer<Int8>, isInteger: Bool) -> Date {
        let interval: TimeInterval
        if isInteger {
            let microseconds = parseInt64(value: (value))
            interval = TimeInterval(microseconds) / 1_000_000
        } else {
            let seconds = parseFloat64(value :value)
            interval = TimeInterval(seconds)
        }
        return Date(timeInterval: interval, since: TimestampConstants.referenceDate)
    }
    
    // MARK: - Interval
    
    private static func parseInt64TimeInterval(value: UnsafeMutablePointer<Int8>) -> (hours: Int64, minutes: Int64, seconds: Double, isNegative: Bool) {
        var totalMicroseconds = parseInt64(value: value)
        let isNegative = totalMicroseconds < 0
        if isNegative {
            totalMicroseconds *= -1
        }
        
        let hours = totalMicroseconds / (1_000_000 * 60 * 60)
        totalMicroseconds -= hours * (1_000_000 * 60 * 60)
        
        let minutes = totalMicroseconds / (1_000_000 * 60)
        totalMicroseconds -= minutes * (1_000_000 * 60)
        
        let seconds = Double(totalMicroseconds) / 1_000_000
        
        return (hours, minutes, seconds, isNegative)
    }
    
    private static func parseFloat64TimeInterval(value: UnsafeMutablePointer<Int8>) -> (hours: Int64, minutes: Int64, seconds: Double, isNegative: Bool) {
        var totalSeconds = parseFloat64(value :value)
        let isNegative = totalSeconds < 0
        if isNegative {
            totalSeconds *= -1
        }
        
        let hoursDouble = totalSeconds / (60 * 60)
        let hours = Int64(hoursDouble > Double(Int64.max) ? Int64.max : Int64(hoursDouble))
        totalSeconds -= Float64(hours) * (60 * 60)
        
        let minutesDouble = totalSeconds / 60
        let minutes = Int64(minutesDouble > Double(Int64.max) ? Int64.max : Int64(minutesDouble))
        totalSeconds -= Float64(minutes) * 60
        
        let seconds = Double(totalSeconds)
        
        return (hours, minutes, seconds, isNegative)
    }
    
    static func parseTimeInterval(value: UnsafeMutablePointer<Int8>, isInteger: Bool) -> String? {
        let hours: Int64
        let minutes: Int64
        let seconds: Double
        let isNegative: Bool
        
        if isInteger {
            (hours, minutes, seconds, isNegative) = parseInt64TimeInterval(value: value)
        } else {
            (hours, minutes, seconds, isNegative) = parseFloat64TimeInterval(value: value)
        }
        
        guard hours > 0 || minutes > 0 || seconds > 0 else {
            return nil
        }
        
        let timeString = [
            Formatters.interval.string(from: NSNumber(value: hours))!,
            Formatters.interval.string(from: NSNumber(value: minutes))!,
            Formatters.interval.string(from: NSNumber(value: seconds))!,
        ].joined(separator: ":")
        
        if isNegative {
            return "-\(timeString)"
        }
        return timeString
    }
    
    static func parseInterval(value: UnsafeMutablePointer<Int8>, timeIsInteger: Bool) -> String {
        let days = Int(parseInt32(value: value.advanced(by: 8)))
        var months = Int(parseInt32(value: value.advanced(by: 12)))
        let hasNegativeParts = days < 0 || months < 0
        
        let years = months / 12
        months -= years * 12
        
        var interval: [String] = []
        if years != 0 {
            let prefix = hasNegativeParts && years > 0 ? "+" : ""
            interval.append("\(prefix)\(years) \(years == 1 ? "year" : "years")")
        }
        if months != 0 {
            let prefix = hasNegativeParts && months > 0 ? "+" : ""
            interval.append("\(prefix)\(months) \(months == 1 ? "mon" : "mons")")
        }
        if days != 0 {
            let prefix = hasNegativeParts && days > 0 ? "+" : ""
            interval.append("\(prefix)\(days) \(days == 1 ? "day" : "days")")
        }
        if let timeString = parseTimeInterval(value: value, isInteger: timeIsInteger) {
            interval.append(timeString)
        }
        
        guard !interval.isEmpty else {
            // Fallback if all is zero
            return "00:00:00"
        }
        return interval.joined(separator: " ")
    }
    
    // MARK: - UUID
    
    static func parseUUID(value: UnsafeMutablePointer<Int8>) -> String {
        let uuid: uuid_t = convert(value)
        return UUID(uuid: uuid).uuidString.lowercased()
    }
    
    // MARK: - Geometric Types
    
    private static func parseGeometryFloat64(value: UnsafeMutablePointer<Int8>) -> String {
        let float = parseFloat64(value: value)
        return Formatters.geometry.string(from: NSNumber(value: float))!
    }
    
    private static func parsePoints(value: UnsafeMutablePointer<Int8>, count: Int) -> String {
        var points: [String] = []
        points.reserveCapacity(count)
        
        var value = value
        for _ in 0..<count {
            points.append(parsePoint(value: value))
            value = value.advanced(by: 16)
        }
        
        return points.joined(separator: ",")
    }
    
    static func parsePoint(value: UnsafeMutablePointer<Int8>) -> String {
        let x = parseGeometryFloat64(value: value)
        let y = parseGeometryFloat64(value: value.advanced(by: 8))
        return "(\(x),\(y))"
    }
    
    static func parseLineSegment(value: UnsafeMutablePointer<Int8>) -> String {
        let points = parsePoints(value: value, count: 2)
        return "[\(points)]"
    }
    
    static func parsePath(value: UnsafeMutablePointer<Int8>) -> String {
        let isOpen: Bool = convert(value)
        let numberOfPoints = parseInt32(value: value.advanced(by: 1))
        let points = parsePoints(value: value.advanced(by: 5), count: Int(numberOfPoints))
        
        if isOpen {
            return "(\(points))"
        }
        return "[\(points)]"
    }
    
    static func parseBox(value: UnsafeMutablePointer<Int8>) -> String {
        return parsePoints(value: value, count: 2)
    }
    
    static func parsePolygon(value: UnsafeMutablePointer<Int8>) -> String {
        let numberOfPoints = parseInt32(value: value)
        let points = parsePoints(value: value.advanced(by: 4), count: Int(numberOfPoints))
        return "(\(points))"
    }
    
    static func parseCircle(value: UnsafeMutablePointer<Int8>) -> String {
        let centerPoint = parsePoint(value: value)
        let radius = parseGeometryFloat64(value: value.advanced(by: 16))
        return "<\(centerPoint),\(radius)>"
    }
    
    // MARK: - Network Address Types
    
    // https://github.com/postgres/postgres/blob/6560407c7db2c7e32926a46f5fb52175ac10d9e5/src/port/inet_net_ntop.c#L44
    static let PGSQL_AF_INET6: Int32 = AF_INET + 1
    
    static func parseIPAddress(value: UnsafeMutablePointer<Int8>) -> String {
        let psqlFamily = Int32(value[0])
        let bits = UInt8(bitPattern: value[1])
        let isCidr: Bool = convert(value.advanced(by: 2))
        
        let family: Int32
        let length: Int
        let standardBits: UInt8
        if psqlFamily == PGSQL_AF_INET6 {
            family = AF_INET6
            length = Int(INET6_ADDRSTRLEN)
            standardBits = 128
        } else {
            family = AF_INET
            length = Int(INET_ADDRSTRLEN)
            standardBits = 32
        }
        
        var buffer = [CChar](repeating: 0, count: length)
        inet_ntop(family, value.advanced(by: 4), &buffer, socklen_t(length))
        let inetString = String(cString: buffer)
        
        if !isCidr && bits == standardBits {
            return inetString
        }
        return "\(inetString)/\(bits)"
    }
    
    static func parseMacAddress(value: UnsafeMutablePointer<Int8>) -> String {
        return (0..<6)
            .map { UInt8(bitPattern: value[$0]).lowercaseHexPair }
            .joined(separator: ":")
    }
    
    // MARK: - Bit String
    
    static func parseBitString(value: UnsafeMutablePointer<Int8>, length: Int) -> String {
        let bitLength = parseInt32(value: value)
        let bitString = (4..<length)
            .map { UInt8(bitPattern: value[$0]).lowercaseBinaryString }
            .joined()
        
        // Limit the bitString to the bitLength
        return bitString.substring(to: bitString.index(bitString.startIndex, offsetBy: Int(bitLength)))
    }
}
