#if os(Linux)
    import CPostgreSQLLinux
#else
    import CPostgreSQLMac
#endif

public enum DatabaseError: Error {
    case cannotEstablishConnection
    case indexOutOfRange
    case columnNotFound
    case invalidSQL(message: String)
    case noQuery
    case noResults
}

public class Database {
    private let host: String 
    private let port: String
    private let dbname: String 
    private let user: String 
    private let password: String 

    public init(host: String = "localhost", port: String = "5432", dbname: String, user: String, password: String) {
        self.host = host 
        self.port = port 
        self.dbname = dbname
        self.user = user 
        self.password = password
    }
    
    @discardableResult
    public func execute(_ query: String, _ values: [Node]? = [], on connection: Connection? = nil) throws -> [[String: Node]] {
        let internalConnection: Connection 

        if let conn = connection {
            internalConnection = conn
        } else {
            internalConnection = try makeConnection()
        }
        
        guard !query.isEmpty else {
            throw DatabaseError.noQuery
        }
        
        let res: Result.ResultPointer
        
        if let values = values, values.count > 0 {
			let paramsValues = bind(values)
            res = PQexecParams(internalConnection.connection, query, Int32(values.count), nil, paramsValues, nil, nil, Int32(0))
            
            defer {
                paramsValues.deinitialize()
                paramsValues.deallocate(capacity: values.count)
            }
        } else {
            res = PQexec(internalConnection.connection, query)
        }
        
        defer { PQclear(res) }
        switch Status(result: res) {
        case .nonFatalError:
            throw DatabaseError.invalidSQL(message: String(cString: PQresultErrorMessage(res)) )
        case .fatalError:
            throw DatabaseError.invalidSQL(message: String(cString: PQresultErrorMessage(res)) )
        case .unknown:
            throw DatabaseError.invalidSQL(message: String(cString: PQresultErrorMessage(res)) )
        case .tuplesOk:
            return Result(resultPointer: res).dictionary
        default:
            break
        }
        return []
    }
    
    func bind(_ values: [Node]) -> UnsafeMutablePointer<UnsafePointer<Int8>?> {


        let paramsValues = UnsafeMutablePointer<UnsafePointer<Int8>?>.allocate(capacity: values.count)

        var v = [[UInt8]]()
        for i in 0..<values.count {
            var ch = [UInt8](values[i].utf8)
            ch.append(0)
            v.append(ch)
            paramsValues[i] = UnsafePointer<Int8>(OpaquePointer(v.last!))
        }
        return paramsValues
    }
    
    public func makeConnection() throws -> Connection {
        return try Connection(host: self.host, port: self.port, dbname: self.dbname, user: self.user, password: self.password)
    }
}
