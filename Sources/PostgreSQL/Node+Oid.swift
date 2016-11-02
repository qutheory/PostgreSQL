#if os(Linux)
    import CPostgreSQLLinux
#else
    import CPostgreSQLMac
#endif

public enum OID: Oid {
    case text = 25
    case varchar  = 1043
    case int2 = 21
    case int4 = 23
    case int8 = 20
    case float4 = 700
    case float8 = 701
    case numeric = 1700
    case bool = 16
    case char = 18
    case bytea = 17
    case unknown = 705
}

extension Node {
    init(oid: Oid, value: String) {
        guard let type = OID(rawValue: oid) else {
            // Always fallback to string, allowing to use with custom types as strings
            self = .string(value)
            return
        }
        
        switch type {
        case .text, .varchar, .char:
            self = .string(value)
        case .int2, .int4, .int8:
            self = .number(.int(Int(value) ?? 0))
        case .float4, .float8, .numeric:
            self = .number(.double(Double(value) ?? 0))
        case .bool:
            self = .bool((value == "t") ? true : false)
        case .bytea:
            self = value.hexToBytes()
        case .unknown:
            self = .null
        }
    }
}

extension String {
    func hexToBytes() -> Node {
        guard hasPrefix("\\x") && utf8.count % 2 == 0 else {
            return .null
        }
        
        var bytes: [UInt8] = []
        bytes.reserveCapacity(utf8.count - 2)
        for i in stride(from: 2, to: utf8.count, by: 2) {
            let startIndex = utf8.index(utf8.startIndex, offsetBy: i)
            let byteString = utf8[startIndex...utf8.index(after: startIndex)]
            
            guard let byte = UInt8(byteString.description, radix: 16) else {
                return .null
            }
            
            bytes.append(byte)
        }
        
        return .bytes(bytes)
    }
}
