import CPostgreSQL

public class Result {
    
    // MARK: - Pointer
    
    public typealias Pointer = OpaquePointer
    
    // MARK: - Status
    
    public enum Status {
        case commandOk
        case tuplesOk
        case copyOut
        case copyIn
        case copyBoth
        case badResponse
        case nonFatalError
        case fatalError
        case emptyQuery
        
        init(_ pointer: Pointer?) {
            guard let pointer = pointer else {
                self = .fatalError
                return
            }
            
            switch PQresultStatus(pointer) {
            case PGRES_COMMAND_OK:
                self = .commandOk
            case PGRES_TUPLES_OK:
                self = .tuplesOk
            case PGRES_COPY_OUT:
                self = .copyOut
            case PGRES_COPY_IN:
                self = .copyIn
            case PGRES_COPY_BOTH:
                self = .copyBoth
            case PGRES_BAD_RESPONSE:
                self = .badResponse
            case PGRES_NONFATAL_ERROR:
                self = .nonFatalError
            case PGRES_FATAL_ERROR:
                self = .fatalError
            case PGRES_EMPTY_QUERY:
                self = .emptyQuery
            default:
                self = .fatalError
            }
        }
    }
    
    // MARK: - Properties
    
    public let pointer: Pointer?
    public let connection: Connection
    public let status: Status
    
    // MARK: - Init
    
    public init(pointer: Pointer?, connection: Connection) {
        self.pointer = pointer
        self.connection = connection
        status = Status(pointer)
    }
    
    // MARK: - Deinit
    
    deinit {
        if let pointer = pointer {
            PQclear(pointer)
        }
    }
    
    // MARK: - Value
    
    public func parseData() throws -> Node {
        switch status {
        case .nonFatalError, .fatalError:
            throw PostgreSQLError(result: self)
            
        case .badResponse:
            throw PostgresSQLStatusError.badResponse
            
        case .emptyQuery:
            throw PostgresSQLStatusError.emptyQuery
            
        case .copyOut, .copyIn, .copyBoth, .commandOk:
            // No data to parse
            return Node(.null, in: PostgreSQLContext.shared)
            
        case .tuplesOk:
            break
        }
        
        var results: [StructuredData] = []
        
        // This single dictionary is reused for all rows in the result set
        // to avoid the runtime overhead of (de)allocating one per row.
        var parsed: [String: StructuredData] = [:]
        
        let rowCount = PQntuples(pointer)
        let columnCount = PQnfields(pointer)

        if rowCount > 0 && columnCount > 0 {
            for row in 0..<rowCount {
                
                for column in 0..<columnCount {
                    let name = String(cString: PQfname(pointer, Int32(column)))
                    
                    // First check if we have null
                    if PQgetisnull(pointer, row, column) == 1 {
                        parsed[name] = .null
                    }
                    // Try to retrieve the binary value
                    else if let value = PQgetvalue(pointer, row, column) {
                        let oid = PQftype(pointer, column)
                        let type = FieldType(oid)
                        let length = Int(PQgetlength(pointer, row, column))
                        
                        let bind = Bind(
                            result: self,
                            bytes: value,
                            length: length,
                            ownsMemory: false,
                            type: type,
                            format: .binary,
                            configuration: connection.configuration
                        )
                        
                        parsed[name] = bind.value
                    }
                    // Otherwise fallback to null
                    else {
                        parsed[name] = .null
                    }
                }
                
                results.append(.object(parsed))
            }
        }
        
        return Node(.array(results), in: PostgreSQLContext.shared)
    }
}
