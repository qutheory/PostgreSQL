import Foundation
import Core

protocol Bindable {
    var postgresBindingData: ([Int8]?, OID?, DataFormat) { get }
}

extension Node: Bindable {
    var postgresBindingData: ([Int8]?, OID?, DataFormat) {
        switch self {
        case .null:
            // PQexecParams converts nil pointer to NULL.
            // see: https://www.postgresql.org/docs/9.1/static/libpq-exec.html
            return (nil, nil, .string)
            
        case .bytes(let bytes):
            let int8Bytes = bytes.map { Int8(bitPattern: $0) }
            return (int8Bytes, nil, .binary)
            
        case .bool(let bool):
            return bool.postgresBindingData
            
        case .number(let number):
            if case .double(let value) = number {
                return value.postgresBindingData
            } else {
                return number.int.postgresBindingData
            }
        
        case .string(let string):
            return string.postgresBindingData
            
        case .array(let array):
            let elements = array.map { $0.postgresArrayElementString }
            let arrayString = "{\(elements.joined(separator: ","))}"
            return (arrayString.utf8CString.array, .none, .string)
            
        case .object(_):
            print("Unsupported Node type for PostgreSQL binding, everything except for .object is supported.")
            return (nil, nil, .string)
        }
    }
    
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
        }
    }
}

extension Bool: Bindable {
    var postgresBindingData: ([Int8]?, OID?, DataFormat) {
        return ([self ? 1 : 0], .bool, .binary)
    }
}

extension Int: Bindable {
    var postgresBindingData: ([Int8]?, OID?, DataFormat) {
        let count = MemoryLayout.size(ofValue: self)
        
        let oid: OID
        switch count {
        case 2:
            oid = .int2
        case 4:
            oid = .int4
        case 8:
            oid = .int8
        default:
            // Unsupported integer size, use string instead
            return description.postgresBindingData
        }
        
        var value = bigEndian
        return (PostgresBinaryUtils.valueToByteArray(&value), oid, .binary)
    }
}

extension Double: Bindable {
    var postgresBindingData: ([Int8]?, OID?, DataFormat) {
        let count = MemoryLayout.size(ofValue: self)
        
        let oid: OID
        switch count {
        case 4:
            oid = .float4
        case 8:
            oid = .float8
        default:
            // Unsupported float size, use string instead
            return description.postgresBindingData
        }
        
        var value = bigEndian
        return (PostgresBinaryUtils.valueToByteArray(&value), oid, .binary)
    }
}

extension String: Bindable {
    var postgresBindingData: ([Int8]?, OID?, DataFormat) {
        return (utf8CString.array, .none, .string)
    }
}
