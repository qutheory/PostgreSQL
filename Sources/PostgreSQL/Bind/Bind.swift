import Foundation
import Core

public final class Bind {
    private class SmartPointer {
        let bytes: UnsafeMutablePointer<Int8>
        let length: Int
        let ownsMemory: Bool
        init(bytes: UnsafeMutablePointer<Int8>, length: Int, ownsMemory: Bool) {
            self.bytes = bytes
            self.length = length
            self.ownsMemory = ownsMemory
        }

        deinit {
            if ownsMemory {
                bytes.deallocate(capacity: length)
            }
        }
    }

    // MARK: - Enums
    
    public enum Format : Int32 {
        case string = 0
        case binary = 1
    }
    
    // MARK: - Properties
    
    public var bytes: UnsafeMutablePointer<Int8>? {
        get {
            return buffer?.bytes
        }
    }
    public var length: Int {
        get {
            return buffer?.length ?? 0
        }
    }

    private let buffer: SmartPointer?
    
    public let type: FieldType
    public let format: Format
    
    public let configuration: Configuration
    public let result: Result?
    
    // MARK: - Init
    
    /**
     Creates a NULL input binding.
     
     PQexecParams converts nil pointer to NULL.
     see: https://www.postgresql.org/docs/9.1/static/libpq-exec.html
     */
    public init(configuration: Configuration) {
        self.configuration = configuration

        buffer = nil

        type = nil
        format = .string
        
        result = nil
    }
    
    /**
     Creates an input binding from a String.
     */
    public convenience init(string: String, configuration: Configuration) {
        let utf8CString = string.utf8CString
        let count = utf8CString.count
        
        let bytes = UnsafeMutablePointer<Int8>.allocate(capacity: count)
        for (i, char) in utf8CString.enumerated() {
            bytes[i] = char
        }
        
        self.init(bytes: bytes, length: count, ownsMemory: true, type: nil, format: .string, configuration: configuration)
    }
    
    /**
     Creates an input binding from a UInt.
     */
    public convenience init(bool: Bool, configuration: Configuration) {
        let bytes = UnsafeMutablePointer<Int8>.allocate(capacity: 1)
        bytes.initialize(to: bool ? 1 : 0)
        
        self.init(bytes: bytes, length: 1, ownsMemory: true, type: FieldType(.bool), format: .binary, configuration: configuration)
    }
    
    /**
     Creates an input binding from an Int.
     */
    public convenience init(int: Int, configuration: Configuration) {
        let count = MemoryLayout.size(ofValue: int)
        
        let type: FieldType
        switch count {
        case 2:
            type = FieldType(.int2)
        case 4:
            type = FieldType(.int4)
        case 8:
            type = FieldType(.int8)
        default:
            // Unsupported integer size, use string instead
            self.init(string: int.description, configuration: configuration)
            return
        }
        
        var value = int.bigEndian
        let (bytes, length) = BinaryUtils.valueToBytes(&value)
        self.init(bytes: bytes, length: length, ownsMemory: true, type: type, format: .binary, configuration: configuration)
    }
    
    /**
     Creates an input binding from a UInt.
     */
    public convenience init(uint: UInt, configuration: Configuration) {
        let int: Int
        if uint >= UInt(Int.max) {
            int = Int.max
        }
        else {
            int = Int(uint)
        }
        
        self.init(int: int, configuration: configuration)
    }
    
    /**
     Creates an input binding from an Double.
     */
    public convenience init(double: Double, configuration: Configuration) {
        let count = MemoryLayout.size(ofValue: double)
        
        let type: FieldType
        switch count {
        case 4:
            type = FieldType(.float4)
        case 8:
            type = FieldType(.float8)
        default:
            // Unsupported float size, use string instead
            self.init(string: double.description, configuration: configuration)
            return
        }
        
        var value = double.bigEndian
        let (bytes, length) = BinaryUtils.valueToBytes(&value)
        self.init(bytes: bytes, length: length, ownsMemory: true, type: type, format: .binary, configuration: configuration)
    }

    /**
     Creates an input binding from an array of bytes.
     */
    public convenience init(bytes: Bytes, configuration: Configuration) {
        let int8Bytes: UnsafeMutablePointer<Int8> = UnsafeMutablePointer.allocate(capacity: bytes.count)
        for (i, byte) in bytes.enumerated() {
            int8Bytes[i] = Int8(bitPattern: byte)
        }
        
        self.init(bytes: int8Bytes, length: bytes.count, ownsMemory: true, type: nil, format: .binary, configuration: configuration)
    }

    /**
     Creates an input binding from a Date.
     */
    public convenience init(date: Date, configuration: Configuration) {
        let interval = date.timeIntervalSince(BinaryUtils.TimestampConstants.referenceDate)		

        if configuration.hasIntegerDatetimes {
            let microseconds = Int64(interval * 1_000_000)
            var value = microseconds.bigEndian
            let (bytes, length) = BinaryUtils.valueToBytes(&value)
            self.init(bytes: bytes, length: length, ownsMemory: true, type: FieldType(.timestamptz), format: .binary, configuration: configuration)
        }
        else {
            let seconds = Float64(interval)
            var value = seconds.bigEndian
            let (bytes, length) = BinaryUtils.valueToBytes(&value)
            self.init(bytes: bytes, length: length, ownsMemory: true, type: FieldType(.timestamptz), format: .binary, configuration: configuration)
        }
    }
    
    /**
     Creates an input binding from an array.
     */
    public convenience init(array: [StructuredData], configuration: Configuration) {
        let elements = array.map { $0.postgresArrayElementString }
        let arrayString = "{\(elements.joined(separator: ","))}"
        self.init(string: arrayString, configuration: configuration)
    }

    public init(bytes: UnsafeMutablePointer<Int8>?, length: Int, ownsMemory: Bool, type: FieldType, format: Format, configuration: Configuration) {
        self.buffer = bytes.map { SmartPointer(bytes: $0, length: length, ownsMemory: ownsMemory) }

        self.type = type
        self.format = format
        
        self.configuration = configuration
        
        result = nil
    }
    
    public init(result: Result, bytes: UnsafeMutablePointer<Int8>, length: Int, ownsMemory: Bool, type: FieldType, format: Format, configuration: Configuration) {
        self.result = result

        self.buffer = SmartPointer(bytes: bytes, length: length, ownsMemory: ownsMemory)

        self.type = type
        self.format = format
        
        self.configuration = configuration
    }
}

extension Node {
    /**
        Creates in input binding from a PostgreSQL Value.
    */
    public func bind(with configuration: Configuration) -> Bind {
        switch wrapped {
        case .number(let number):
            switch number {
            case .int(let int):
                return Bind(int: int, configuration: configuration)
            case .double(let double):
                return Bind(double: double, configuration: configuration)
            case .uint(let uint):
                return Bind(uint: uint, configuration: configuration)
            }
        case .string(let string):
            return Bind(string: string, configuration: configuration)
        case .null:
            return Bind(configuration: configuration)
        case .array(let array):
            return Bind(array: array, configuration: configuration)
        case .bytes(let bytes):
            return Bind(bytes: bytes, configuration: configuration)
        case .object(_):
            print("Unsupported Node type for PostgreSQL binding, everything except for .object is supported.")
            return Bind(configuration: configuration)
        case .bool(let bool):
            return Bind(bool: bool, configuration: configuration)
        case .date(let date):
            return Bind(date: date, configuration: configuration)
        }
    }
}

extension StructuredData {
    var postgresArrayElementString: String {
        switch self {
        case .null:
            return "NULL"
            
        case .bytes(let bytes):
            let hexString = bytes.map { $0.lowercaseHexPair }.joined()
            return "\"\\\\x\(hexString)\""
            
        case .bool(let bool):
            return bool ? "t" : "f"
            
        case .number(let number):
            return number.description
        
        case .string(let string):
            let escapedString = string
                .replacingOccurrences(of: "\\", with: "\\\\")
                .replacingOccurrences(of: "\"", with: "\\\"")
            return "\"\(escapedString)\""
            
        case .array(let array):
            let elements = array.map { $0.postgresArrayElementString }
            return "{\(elements.joined(separator: ","))}"
            
        case .object(_):
            print("Unsupported Node array type for PostgreSQL binding, everything except for .object is supported.")
            return "NULL"

        case .date(let date):
            return BinaryUtils.Formatters.timestamptz.string(from: date)
        }
    }
}
