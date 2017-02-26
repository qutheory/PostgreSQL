#if os(Linux)
    import CPostgreSQLLinux
#else
    import CPostgreSQLMac
#endif

class Result {
    typealias Pointer = OpaquePointer

    private let pointer: Pointer
    private let configuration: Configuration
    let parsed: [[String: Node]]

    init(configuration: Configuration, pointer: Pointer) {
        self.configuration = configuration
        self.pointer = pointer
        
        var parsed: [[String: Node]] = []
        
        let rowCount = PQntuples(pointer)
        let columnCount = PQnfields(pointer)
        
        if rowCount > 0 && columnCount > 0 {
            for row in 0..<rowCount {
                var item: [String: Node] = [:]
                
                for column in 0..<columnCount {
                    let name = String(cString: PQfname(pointer, Int32(column)))
                    
                    if PQgetisnull(pointer, row, column) == 1 {
                        item[name] = .null
                    } else if let value = PQgetvalue(pointer, row, column) {
                        let type = PQftype(pointer, column)
                        let length = Int(PQgetlength(pointer, row, column))
                        item[name] = Node(
                            configuration: configuration,
                            oid: type,
                            value: value,
                            length: length
                        )
                    } else {
                        item[name] = .null
                    }
                }
                
                parsed.append(item)
            }
        }
        
        self.parsed = parsed
    }
}
