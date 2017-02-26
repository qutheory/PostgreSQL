#if os(Linux)
    import CPostgreSQLLinux
#else
    import CPostgreSQLMac
#endif
import Foundation

/// Oid values can be found in the following file:
/// https://github.com/postgres/postgres/blob/55c3391d1e6a201b5b891781d21fe682a8c64fe6/src/include/catalog/pg_type.h
enum OID: Oid {
    case bool = 16
    
    case int2 = 21
    case int4 = 23
    case int8 = 20
    
    case bytea = 17
    
    case char = 18
    case name = 19
    case text = 25
    case bpchar = 1042
    case varchar = 1043
    
    case json = 114
    case jsonb = 3802
    case xml = 142
    
    case float4 = 700
    case float8 = 701
    
    case numeric = 1700
    
    case date = 1082
    case time = 1083
    case timetz = 1266
    case timestamp = 1114
    case timestamptz = 1184
    case interval = 1186
    
    case uuid = 2950
    
    case point = 600
    case lseg = 601
    case path = 602
    case box = 603
    case polygon = 604
    case circle = 718
    
    case cidr = 650
    case inet = 869
    case macaddr = 829
    
    case bit = 1560
    case varbit = 1562
    
    static let supportedArrayOIDs: Set<Oid> = [
        1000, // bool
        
        1005, // int2
        1007, // int4
        1016, // int8
        
        1001, // bytea
        
        1002, // char
        1003, // name
        1009, // text
        1014, // bpchar
        1015, // varchar
        
        199, // json
        3807, // jsonb
        143, // xml
        
        1021, // float4
        1022, // float8
        
        1231, // numeric
        
        1182, // date
        1183, // time
        1270, // timetz
        1115, // timestamp
        1185, // timestamptz
        1187, // interval
        
        2951, // uuid
        
        1017, // point
        1018, // lseg
        1019, // path
        1020, // box
        1027, // polygon
        719, // circle
        
        651, // cidr
        1041, // inet
        1040, // macaddr
        
        1561, // bit
        1563, // varbit
    ]
}

extension Node {
    init(configuration: Configuration, oid: Oid, value: UnsafeMutablePointer<Int8>, length: Int) {
        // Check if we support the type
        guard let type = OID(rawValue: oid) else {
            // Check if we have an array type and try to convert
            if OID.supportedArrayOIDs.contains(oid), let node = Node(configuration: configuration, arrayValue: value) {
                self = node
            } else {
                // Otherwise fallback to simply passing on the bytes
                let bytes = PostgresBinaryUtils.parseBytes(value: value, length: length)
                self = .bytes(bytes)
            }
            return
        }
        
        self = Node(configuration: configuration, oid: type, value: value, length: length)
    }
    
    init(configuration: Configuration, oid: OID, value: UnsafeMutablePointer<Int8>, length: Int) {
        switch oid {
        case .bool:
            self = .bool(value[0] != 0)
            
        case .char, .name, .text, .json, .xml, .bpchar, .varchar:
            let string = PostgresBinaryUtils.parseString(value: value, length: length)
            self = .string(string)
            
        case .jsonb:
            // Ignore jsonb version number
            let jsonValue = value.advanced(by: 1)
            let string = PostgresBinaryUtils.parseString(value: jsonValue, length: length - 1)
            self = .string(string)
        
        case .int2:
            let integer = PostgresBinaryUtils.parseInt16(value: value)
            self = .number(.int(Int(integer)))
            
        case .int4:
            let integer = PostgresBinaryUtils.parseInt32(value: value)
            self = .number(.int(Int(integer)))
            
        case .int8:
            let integer = PostgresBinaryUtils.parseInt64(value: value)
            if let intValue = Int(exactly: integer) {
                self = .number(.int(intValue))
            } else {
                self = .number(.double(Double(integer)))
            }
            
        case .bytea:
            let bytes = PostgresBinaryUtils.parseBytes(value: value, length: length)
            self = .bytes(bytes)
        
        case .float4:
            let float = PostgresBinaryUtils.parseFloat32(value: value)
            self = .number(.double(Double(float)))
            
        case .float8:
            let float = PostgresBinaryUtils.parseFloat64(value: value)
            self = .number(.double(Double(float)))
            
        case .numeric:
            let number = PostgresBinaryUtils.parseNumeric(value: value)
            self = .string(number)
            
        case .uuid:
            let uuid = PostgresBinaryUtils.parseUUID(value: value)
            self = .string(uuid)
            
        case .timestamp, .timestamptz, .date, .time, .timetz:
            let date = PostgresBinaryUtils.parseTimetamp(value: value, isInteger: configuration.hasIntegerDatetimes)
            let formatter = PostgresBinaryUtils.Formatters.dateFormatter(for: oid)
            let timestamp = formatter.string(from: date)
            self = .string(timestamp)
            
        case .interval:
            let interval = PostgresBinaryUtils.parseInterval(value: value, timeIsInteger: configuration.hasIntegerDatetimes)
            self = .string(interval)
            
        case .point:
            let point = PostgresBinaryUtils.parsePoint(value: value)
            self = .string(point)
            
        case .lseg:
            let lseg = PostgresBinaryUtils.parseLineSegment(value: value)
            self = .string(lseg)
            
        case .path:
            let path = PostgresBinaryUtils.parsePath(value: value)
            self = .string(path)
            
        case .box:
            let box = PostgresBinaryUtils.parseBox(value: value)
            self = .string(box)
            
        case .polygon:
            let polygon = PostgresBinaryUtils.parsePolygon(value: value)
            self = .string(polygon)
            
        case .circle:
            let circle = PostgresBinaryUtils.parseCircle(value: value)
            self = .string(circle)
            
        case .inet, .cidr:
            let inet = PostgresBinaryUtils.parseIPAddress(value: value)
            self = .string(inet)
            
        case .macaddr:
            let macaddr = PostgresBinaryUtils.parseMacAddress(value: value)
            self = .string(macaddr)
            
        case .bit, .varbit:
            let bitString = PostgresBinaryUtils.parseBitString(value: value, length: length)
            self = .string(bitString)
        }
    }
    
    private init?(configuration: Configuration, arrayValue: UnsafeMutablePointer<Int8>) {
        let elementOid = Oid(bigEndian: PostgresBinaryUtils.convert(arrayValue.advanced(by: 8)))
        
        // Check if we support the type
        guard let type = OID(rawValue: elementOid) else {
            return nil
        }
        
        // Get the dimension of the array
        let arrayDimension = PostgresBinaryUtils.parseInt32(value: arrayValue)
        guard arrayDimension > 0 else {
            self = .array([])
            return
        }
        
        var pointer = arrayValue.advanced(by: 12)
        
        // Get all dimension lengths
        var dimensionLengths: [Int] = []
        for _ in 0..<arrayDimension {
            dimensionLengths.append(Int(PostgresBinaryUtils.parseInt32(value: pointer)))
            pointer = pointer.advanced(by: 8)
        }
        
        // Parse the array
        self = Node.parseArray(configuration: configuration, type: type, dimensionLengths: dimensionLengths, pointer: &pointer)
    }
    
    private static func parseArray(configuration: Configuration, type: OID, dimensionLengths: [Int], pointer: inout UnsafeMutablePointer<Int8>) -> Node {
        // Get the length of the array
        let arrayLength = dimensionLengths[0]
        
        // Create elements array
        var elements: [Node] = []
        elements.reserveCapacity(arrayLength)
        
        // Loop through array and convert each item
        for _ in 0..<arrayLength {
            
            // Check if we need to parse sub arrays
            if dimensionLengths.count > 1 {
                
                var subDimensionLengths = dimensionLengths
                subDimensionLengths.removeFirst()
                
                let array = parseArray(configuration: configuration, type: type, dimensionLengths: subDimensionLengths, pointer: &pointer)
                elements.append(array)
                
            } else {
                
                let elementLength = Int(PostgresBinaryUtils.parseInt32(value: pointer))
                pointer = pointer.advanced(by: 4)
                
                // Check if the element is null
                guard elementLength != -1 else {
                    elements.append(.null)
                    continue
                }
                
                // Parse to node
                let node = Node(configuration: configuration, oid: type, value: pointer, length: elementLength)
                elements.append(node)
                pointer = pointer.advanced(by: elementLength)
            }
        }
        
        return .array(elements)
    }
}
