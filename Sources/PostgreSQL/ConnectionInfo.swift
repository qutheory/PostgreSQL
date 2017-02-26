#if os(Linux)
    import CPostgreSQLLinux
#else
    import CPostgreSQLMac
#endif

public enum ConnInfo {
    case raw(String)
    case params([String: String])
    case basic(host: String, port: Int, database: String, user: String, password: String)
}

public protocol ConnInfoInitializable {
    init(conninfo: ConnInfo) throws
}

extension ConnInfoInitializable {
    public init(params: [String: String]) throws {
        try self.init(conninfo: .params(params))
    }
    
    public init(host: String, port: Int, database: String, user: String, password: String) throws {
        try self.init(conninfo: .basic(host: host, port: port, database: database, user: user, password: password))
    }
    
    public init(conninfo: String) throws {
        try self.init(conninfo: .raw(conninfo))
    }
}
