import CPostgreSQL

extension Bind {
    /// Parses a PostgreSQL value from an output binding.
    public var value: StructuredData {
        // Check if we have data to parse
        guard let value = bytes else {
            return .null
        }
        
        // We only parse binary data, otherwise simply return the data as a string
        guard format == .binary else {
            let string = BinaryUtils.parseString(value: value, length: length)
            return .string(string)
        }
        
        // Parse based on the type of data
        switch type {
        case .null:
            return .null
            
        case .supported(let supportedType):
            return Bind.parse(type: supportedType, configuration: configuration, value: value, length: length)
            
        case .array(let supportedArrayType):
            return Bind.parse(type: supportedArrayType, configuration: configuration, value: value, length: length)
            
        case .unsupported(_):
            // Unsupported Oid type for PostgreSQL binding.
            
            // Fallback to simply passing on the bytes
            let bytes = BinaryUtils.parseBytes(value: value, length: length)
            return .bytes(bytes)
        }
    }
}

/**
 Parsing data
 */
extension Bind {
    fileprivate static func parse(type: FieldType.Supported, configuration: Configuration, value: UnsafeMutablePointer<Int8>, length: Int) -> StructuredData {
        switch type {
        case .bool:
            return .bool(value[0] != 0)
            
        case .char, .name, .text, .json, .xml, .bpchar, .varchar:
            let string = BinaryUtils.parseString(value: value, length: length)
            return .string(string)
            
        case .jsonb:
            // Ignore jsonb version number
            let jsonValue = value.advanced(by: 1)
            let string = BinaryUtils.parseString(value: jsonValue, length: length - 1)
            return .string(string)
        
        case .int2:
            let integer = BinaryUtils.parseInt16(value: value)
            return .number(.int(Int(integer)))
            
        case .int4:
            let integer = BinaryUtils.parseInt32(value: value)
            return .number(.int(Int(integer)))
            
        case .int8:
            let integer = BinaryUtils.parseInt64(value: value)
            if let intValue = Int(exactly: integer) {
                return .number(.int(intValue))
            } else {
                return .number(.double(Double(integer)))
            }
            
        case .bytea:
            let bytes = BinaryUtils.parseBytes(value: value, length: length)
            return .bytes(bytes)
        
        case .float4:
            let float = BinaryUtils.parseFloat32(value: value)
            return .number(.double(Double(float)))
            
        case .float8:
            let float = BinaryUtils.parseFloat64(value: value)
            return .number(.double(Double(float)))
            
        case .numeric:
            let number = BinaryUtils.parseNumeric(value: value)
            return .string(number)
            
        case .uuid:
            let uuid = BinaryUtils.parseUUID(value: value)
            return .string(uuid)
            
        case .timestamp, .timestamptz, .date, .time, .timetz:
            let date = BinaryUtils.parseTimetamp(value: value, isInteger: configuration.hasIntegerDatetimes)
            return .date(date)
            
        case .interval:
            let interval = BinaryUtils.parseInterval(value: value, timeIsInteger: configuration.hasIntegerDatetimes)
            return .string(interval)
            
        case .point:
            let point = BinaryUtils.parsePoint(value: value)
            return .string(point)
            
        case .lseg:
            let lseg = BinaryUtils.parseLineSegment(value: value)
            return .string(lseg)
            
        case .path:
            let path = BinaryUtils.parsePath(value: value)
            return .string(path)
            
        case .box:
            let box = BinaryUtils.parseBox(value: value)
            return .string(box)
            
        case .polygon:
            let polygon = BinaryUtils.parsePolygon(value: value)
            return .string(polygon)
            
        case .circle:
            let circle = BinaryUtils.parseCircle(value: value)
            return .string(circle)
            
        case .inet, .cidr:
            let inet = BinaryUtils.parseIPAddress(value: value)
            return .string(inet)
            
        case .macaddr:
            let macaddr = BinaryUtils.parseMacAddress(value: value)
            return .string(macaddr)
            
        case .bit, .varbit:
            let bitString = BinaryUtils.parseBitString(value: value, length: length)
            return .string(bitString)
        }
    }
    
    fileprivate static func parse(type: FieldType.ArraySupported, configuration: Configuration, value: UnsafeMutablePointer<Int8>, length: Int) -> StructuredData {
        // Get the dimension of the array
        let arrayDimension = BinaryUtils.parseInt32(value: value)
        guard arrayDimension > 0 else {
            return .array([])
        }
        
        var pointer = value.advanced(by: 12)
        
        // Get all dimension lengths
        var dimensionLengths: [Int] = []
        for _ in 0..<arrayDimension {
            dimensionLengths.append(Int(BinaryUtils.parseInt32(value: pointer)))
            pointer = pointer.advanced(by: 8)
        }
        
        // Parse the array
        return parse(type: type, configuration: configuration, dimensionLengths: dimensionLengths, pointer: &pointer)
    }
    
    private static func parse(type: FieldType.ArraySupported, configuration: Configuration, dimensionLengths: [Int], pointer: inout UnsafeMutablePointer<Int8>) -> StructuredData {
        // Get the length of the array
        let arrayLength = dimensionLengths[0]
        
        // Create elements array
        var values: [StructuredData] = []
        values.reserveCapacity(arrayLength)
        
        // Loop through array and convert each item
        let supportedType = type.supported
        for _ in 0..<arrayLength {
            
            // Check if we need to parse sub arrays
            if dimensionLengths.count > 1 {
                
                var subDimensionLengths = dimensionLengths
                subDimensionLengths.removeFirst()
                
                let array = parse(type: type, configuration: configuration, dimensionLengths: subDimensionLengths, pointer: &pointer)
                values.append(array)
                
            } else {
                
                let elementLength = Int(BinaryUtils.parseInt32(value: pointer))
                pointer = pointer.advanced(by: 4)
                
                // Check if the element is null
                guard elementLength != -1 else {
                    values.append(.null)
                    continue
                }
                
                // Parse to node
                let item = parse(type: supportedType, configuration: configuration, value: pointer, length: elementLength)
                values.append(item)
                pointer = pointer.advanced(by: elementLength)
            }
        }
        
        return .array(values)
    }
}


