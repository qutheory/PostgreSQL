import CPostgreSQL

public class Result {
    
    // MARK: - Pointer
    
    public typealias Pointer = OpaquePointer
    
    // MARK: - Properties
    
    public let pointer: Pointer?
    public let connection: Connection
    public let status: ResultStatus
    
    // MARK: - Init
    
    public init(pointer: Pointer?, connection: Connection) {
        self.pointer = pointer
        self.connection = connection
        status = ResultStatus(pointer)
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

        case .singleTuple:
            // Shouldn't receive single tuple in Result
            throw PostgresSQLStatusError.badResponse

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

/// ResultNodeSequence is a result that pulls the rows from the database on demand.
public class ResultNodeSequence: Sequence {
    public enum PostgreSQLRowResult {
        case error(Error)
        case node(Node)
    }

    public typealias Iterator = AnyIterator<PostgreSQLRowResult>

    // MARK: - Properties

    public let connection: Connection

    // MARK: - Init

    public init(connection: Connection) throws {
        self.connection = connection
    }

    deinit {
        do {
            try self.cancel()
        } catch {
            print("Error canceling: \(error)")
        }
        clearConnection(self.connection.cConnection)
    }

    public func close() throws {
        try self.cancel()
        clearConnection(self.connection.cConnection)
    }

    private func cancel() throws {
        let cancel = PQgetCancel(self.connection.cConnection)
        let errBufSize: Int32 = 256
        let ptr = UnsafeMutablePointer<Int8>.allocate(capacity: Int(errBufSize))
        ptr.initialize(to: 0, count: Int(errBufSize))
        if PQcancel(cancel, ptr, errBufSize) == 0 {
            let errMsg = ptr.withMemoryRebound(to: CChar.self, capacity: Int(errBufSize)) {
                return String(validatingUTF8: $0) ?? ""
            }
            throw PostgreSQLCancelError(reason: errMsg)
        }
    }

    public func makeIterator() -> ResultNodeSequence.Iterator {
        // This single dictionary is reused for all rows in the result set
        // to avoid the runtime overhead of (de)allocating one per row.
        var parsed: [String: StructuredData] = [:]

        return AnyIterator {
            guard let pointer = PQgetResult(self.connection.cConnection) else {
                return nil
            }
            defer { PQclear(pointer) }
            let status = ResultStatus(pointer)

            switch status {
            case .nonFatalError, .fatalError:
                let result = PostgreSQLRowResult.error(PostgreSQLError(pointer: pointer))
                clearConnection(self.connection.cConnection)
                return result

            case .badResponse:
                clearConnection(self.connection.cConnection)
                return .error(PostgresSQLStatusError.badResponse)

            case .emptyQuery:
                clearConnection(self.connection.cConnection)
                return .error(PostgresSQLStatusError.emptyQuery)

            case .copyOut, .copyIn, .copyBoth, .commandOk:
                // No data to parse
                return .node(Node(.null, in: PostgreSQLContext.shared))

            case .tuplesOk:
                // tuplesOk signals that all rows have been retrieved
                clearConnection(self.connection.cConnection)
                return nil

            case .singleTuple:
                break
            }

            let columnCount = PQnfields(pointer)

            for column in 0..<columnCount {
                let name = String(cString: PQfname(pointer, column))

                // First check if we have null
                if PQgetisnull(pointer, 0, column) == 1 {
                    parsed[name] = .null
                }
                    // Try to retrieve the binary value
                else if let value = PQgetvalue(pointer, 0, column) {
                    let oid = PQftype(pointer, column)
                    let type = FieldType(oid)
                    let length = Int(PQgetlength(pointer, 0, column))

                    let bind = Bind(
                        result: self,
                        bytes: value,
                        length: length,
                        ownsMemory: false,
                        type: type,
                        format: .binary,
                        configuration: self.connection.configuration
                    )

                    parsed[name] = bind.value
                }
                    // Otherwise fallback to null
                else {
                    parsed[name] = .null
                }
            }

            return .node(Node(.object(parsed), in: PostgreSQLContext.shared))
        }
    }
}

private func clearConnection(_ connection: Connection.CConnection) {
    repeat {
        let pointer = PQgetResult(connection)
        guard pointer != nil else { break }
        PQclear(pointer)
    } while true
}

public enum ResultStatus {
    case commandOk
    case tuplesOk
    case copyOut
    case copyIn
    case copyBoth
    case badResponse
    case nonFatalError
    case fatalError
    case emptyQuery
    case singleTuple

    init(_ pointer: OpaquePointer?) {
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
        case PGRES_SINGLE_TUPLE:
            self = .singleTuple
        default:
            self = .fatalError
        }
    }
}
