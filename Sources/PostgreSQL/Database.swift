#if os(Linux)
    import CPostgreSQLLinux
#else
    import CPostgreSQLMac
#endif
import Core

public enum DatabaseError: Error {
    case cannotEstablishConnection(String)
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
        guard !query.isEmpty else {
            throw DatabaseError.noQuery
        }
        
        let connection = try connection ?? makeConnection()

        let res: Result.ResultPointer

        if let values = values, !values.isEmpty {
            var paramTypes: [Oid] = []
            var paramValues: [[Int8]?] = []
            var paramLengths: [Int32] = []
            var paramFormats: [Int32] = []
            
            for value in values {
                switch value {
                case .bytes(let bytes):
                    paramValues.append(bytes.map { Int8(bitPattern: $0) })
                    paramTypes.append(OID.bytea.rawValue)
                    paramLengths.append(Int32(bytes.count))
                    paramFormats.append(1)
                    
                case .null:
                    // PQexecParams converts nil pointer to NULL.
                    // see: https://www.postgresql.org/docs/9.1/static/libpq-exec.html
                    paramValues.append(nil)
                    paramTypes.append(0)
                    paramLengths.append(0)
                    paramFormats.append(0)
                    
                default:
                    if let string = value.string {
                        paramValues.append(Array(string.utf8CString))
                    }
                    else {
                        paramValues.append(nil)
                    }
                    paramTypes.append(0)
                    paramLengths.append(0)
                    paramFormats.append(0)
                }
            }
            
            res = PQexecParams(connection.connection, query, Int32(values.count), paramTypes, paramValues.map { UnsafePointer<Int8>($0) }, paramLengths, paramFormats, 0)
            
        } else {
            res = PQexec(connection.connection, query)
        }

        defer {
            PQclear(res)
        }

        switch Status(result: res) {
        case .nonFatalError, .fatalError, .unknown:
            throw DatabaseError.invalidSQL(message: String(cString: PQresultErrorMessage(res)) )
        case .tuplesOk:
            return Result(resultPointer: res).dictionary
        default:
            return []
        }
    }

    public func makeConnection() throws -> Connection {
        return try Connection(host: self.host, port: self.port, dbname: self.dbname, user: self.user, password: self.password)
    }
}
