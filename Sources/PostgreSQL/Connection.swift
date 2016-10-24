#if os(Linux)
    import CPostgreSQLLinux
#else
    import CPostgreSQLMac
#endif

public final class Connection {
    public typealias ConnectionPointer = OpaquePointer

    private(set) var connection: ConnectionPointer!

    public var connected: Bool {
        if let connection = connection, PQstatus(connection) == CONNECTION_OK {
            return true
        }
        return false
    }

    public init(params: String ...) throws {
        let keys = params.filter { params.index(of: $0)! % 2 == 0 }
        let values = params.filter { params.index(of: $0)! % 2 != 0 }
        
        var connectionComponents = [String]()
        
        for i in 0..<(params.count / 2) {
            connectionComponents.append("\(keys[i])='\(values[i])'")
        }
        
        self.connection = PQconnectdb(connectionComponents.joined())
        if !self.connected {
            throw DatabaseError.cannotEstablishConnection(self.error)
        }
    }
    
    public convenience init(host: String = "localhost", port: String = "5432", dbname: String, user: String, password: String) throws {
        try self.init(params: "host", host, "port", port, "dbname", dbname, "user", user, "password", password)
    }

    public func reset() throws {
        guard self.connected else {
            throw DatabaseError.cannotEstablishConnection(error)
        }

        PQreset(connection)
    }

    public func close() throws {
        guard self.connected else {
            throw DatabaseError.cannotEstablishConnection(error)
        }

        PQfinish(connection)
    }

    public var error: String {
        guard let s = PQerrorMessage(connection) else {
            return ""
        }
        return String(cString: s) 
    }

    deinit {
        try? close()
    }
}
