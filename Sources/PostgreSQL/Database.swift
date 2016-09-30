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
        case .nonFatalError, .fatalError, .unknown:
            throw DatabaseError.invalidSQL(message: String(cString: PQresultErrorMessage(res)) )
        case .tuplesOk:
            return Result(resultPointer: res).dictionary
        default:
            break
        }
        return []
    }

    func bind(_ values: [Node]) -> UnsafeMutablePointer<UnsafePointer<Int8>?> {
        let paramsValues = UnsafeMutablePointer<UnsafePointer<Int8>?>
            .allocate(capacity: values.count)

        for i in 0..<values.count {
            var ch = values[i].string?.bytes ?? []
            ch.append(0)

            let p = UnsafeMutablePointer<Int8>.allocate(capacity: ch.count)
            for (i, c) in ch.enumerated() {
                p[i] = Int8(bitPattern: c)
            }

            paramsValues[i] = UnsafePointer(p)
        }
        return paramsValues
    }

    public func makeConnection() throws -> Connection {
        return try Connection(host: self.host, port: self.port, dbname: self.dbname, user: self.user, password: self.password)
    }
}
