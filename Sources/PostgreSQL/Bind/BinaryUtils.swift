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
}

extension Float64 {
    init(bigEndian: Float64) {
        let int = UInt64(bigEndian: bigEndian.bitPattern)
        self = Float64(bitPattern: int)
    }
    
    var bigEndian: Float64 {
        return Float64(bitPattern: bitPattern.bigEndian)
    }
}

/// Most information for parsing binary formats has been retrieved from the following links:
/// - https://www.postgresql.org/docs/9.6/static/datatype.html (Data types)
/// - https://github.com/postgres/postgres/tree/55c3391d1e6a201b5b891781d21fe682a8c64fe6/src/backend/utils/adt (Backend sending code)
struct BinaryUtils {
    
    // MARK: - Formatter
    
    struct Formatters {
        static let timestamptz: DateFormatter = {
            let formatter = DateFormatter()
            formatter.dateFormat = "yyyy-MM-dd HH:mm:ss.SSSX"
            return formatter
        }()
        
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
    
    static func valueToBytes<T>(_ value: inout T) -> (UnsafeMutablePointer<Int8>, Int) {
        let size = MemoryLayout.size(ofValue: value)
        return withUnsafePointer(to: &value) { valuePointer in
            return valuePointer.withMemoryRebound(to: Int8.self, capacity: size) { bytePointer in
                let bytes: UnsafeMutablePointer<Int8> = UnsafeMutablePointer.allocate(capacity: size)
                bytes.assign(from: bytePointer, count: size)
                return (bytes, size)
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
    
    // MARK: - Numeric
    
    struct Numeric {
        private static let signNaN: Int16 = -16384
        private static let signNegative: Int16 = 16384
        private static let decDigits = 4
        private static let NBASE: Int16 = 10000
        private static let halfNBASE: Int16 = 5000
        private static let roundPowers: [Int16] = [0, 1000, 100, 10]
        
        var sign: Int16
        var weight: Int
        var dscale: Int
        var numberOfDigits: Int
        var digits: [Int16]
        
        init(value: UnsafeMutablePointer<Int8>) {
            sign = BinaryUtils.parseInt16(value: value.advanced(by: 4))
            weight = Int(BinaryUtils.parseInt16(value: value.advanced(by: 2)))
            
            var dscale = Int(BinaryUtils.parseInt16(value: value.advanced(by: 6)))
            if dscale < 0 {
                dscale = 0
            }
            self.dscale = dscale
            
            numberOfDigits = Int(BinaryUtils.parseInt16(value: value))
            digits = (0..<numberOfDigits).map { BinaryUtils.parseInt16(value: value.advanced(by: 8 + $0 * 2)) }
        }
        
        private func getDigit(atIndex index: Int) -> String {
            let int16: Int16
            if index >= 0 && index < numberOfDigits {
                int16 = digits[index]
            } else {
                int16 = 0
            }
            let stringDigits = String(int16)
            
            guard index != 0 else {
                return stringDigits
            }
            
            // The number of digits should be 4 (DEC_DIGITS),
            // so pad if necessary.
            return String(repeating: "0", count: Numeric.decDigits - stringDigits.characters.count) + stringDigits
        }
        
        /// Function for rounding numeric values.
        /// The code is based on https://github.com/postgres/postgres/blob/3a0d473192b2045cbaf997df8437e7762d34f3ba/src/backend/utils/adt/numeric.c#L8594
        mutating func roundIfNeeded() {
            // Decimal digits wanted
            var totalDigits = (weight + 1) * Numeric.decDigits + dscale
            
            // If less than 0, result should be 0
            guard totalDigits >= 0 else {
                digits = []
                weight = 0
                sign = 0
                return
            }
            
            // NBASE digits wanted
            var nbaseDigits = (totalDigits + Numeric.decDigits - 1) / Numeric.decDigits
            
            // 0, or number of decimal digits to keep in last NBASE digit
            totalDigits = totalDigits % Numeric.decDigits
            
            guard nbaseDigits < numberOfDigits || (nbaseDigits == numberOfDigits && totalDigits > 0) else {
                return
            }
            
            numberOfDigits = nbaseDigits
            
            var carry: Int16
            if totalDigits == 0 {
                carry = digits[0] >= Numeric.halfNBASE ? 1 : 0
            } else {
                nbaseDigits -= 1
                
                // Must round within last NBASE digit
                var pow10 = Numeric.roundPowers[totalDigits]
                let extra = digits[nbaseDigits] % pow10
                digits[nbaseDigits] = digits[nbaseDigits] - extra
                
                carry = 0
                if extra >= pow10 / 2 {
                    pow10 += digits[nbaseDigits]
                    if pow10 >= Numeric.NBASE {
                        pow10 -= Numeric.NBASE
                        carry = 1
                    }
                    digits[nbaseDigits] = pow10
                }
            }
            
            // Propagate carry if needed
            while carry > 0 {
                nbaseDigits -= 1
                if nbaseDigits < 0 {
                    digits.insert(0, at: 0)
                    nbaseDigits = 0
                    
                    numberOfDigits += 1
                    weight += 1
                }
                
                carry += digits[nbaseDigits]
                
                if carry >= Numeric.NBASE {
                    digits[nbaseDigits] = carry - Numeric.NBASE
                    carry = 1
                } else {
                    digits[nbaseDigits] = carry
                    carry = 0
                }
            }
        }
        
        var string: String {
            // Check for NaN
            guard sign != Numeric.signNaN else {
                return "NaN"
            }
            
            guard !digits.isEmpty else {
                return "0"
            }
            
            var digitIndex = 0
            var string: String = ""
            
            // Make number negative if necessary
            if sign == Numeric.signNegative {
                string += "-"
            }
            
            // Add all digits before decimal point
            if weight < 0 {
                digitIndex = weight + 1
                string += "0"
            } else {
                while digitIndex <= weight {
                    string += getDigit(atIndex: digitIndex)
                    digitIndex += 1
                }
            }
            
            guard dscale > 0 else {
                return string
            }
            
            // Add digits after decimal point
            string += "."
            let decimalIndex = string.endIndex
            
            for _ in stride(from: 0, to: dscale, by: Numeric.decDigits) {
                string += getDigit(atIndex: digitIndex)
                digitIndex += 1
            }
            
            #if swift(>=3.2)
                let maxOffset = string.distance(from: decimalIndex, to: string.endIndex)
                let offset = min(maxOffset, dscale)
                let endIndex = string.index(decimalIndex, offsetBy: offset)
                
                return String(string[..<endIndex])
            #else
                let endIndex = string.index(decimalIndex, offsetBy: dscale + 1)
                return string.substring(to: endIndex)
            #endif
        }
    }
    
    static func parseNumeric(value: UnsafeMutablePointer<Int8>) -> String {
        var numeric = Numeric(value: value)
        numeric.roundIfNeeded()
        return numeric.string
    }
    
    // MARK: - Date / Time

    struct TimestampConstants {
      // Foundation referenceDate is 00:00:00 UTC on 1 January 2001,
      // the reference date we want is 00:00:00 UTC on 1 January 2000
      static let offsetTimeIntervalSinceFoundationReferenceDate: TimeInterval = -31_622_400
      static let referenceDate = Date(timeIntervalSinceReferenceDate: offsetTimeIntervalSinceFoundationReferenceDate)
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
        let toIndex = bitString.index(bitString.startIndex, offsetBy: Int(bitLength))
        #if swift(>=4.0)
            return String(bitString[..<toIndex])
        #else
            return bitString.substring(to: toIndex)
        #endif
    }
}
