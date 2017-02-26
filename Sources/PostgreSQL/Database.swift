#if os(Linux)
    import CPostgreSQLLinux
#else
    import CPostgreSQLMac
#endif

public enum DatabaseError: Error {
    case cannotEstablishConnection(String)
    case indexOutOfRange
    case columnNotFound
    case invalidSQL(message: String)
    case noQuery
    case noResults
}

enum DataFormat : Int32 {
    case string = 0
    case binary = 1
}

public final class Database: ConnInfoInitializable {
    // MARK: - Properties
    public let conninfo: ConnInfo
    
    // MARK: - Init
    public init(conninfo: ConnInfo) throws {
        self.conninfo = conninfo
    }
    
    // MARK: - Connection
    @discardableResult
    public func execute(_ query: String, _ values: [Node]? = [], on connection: Connection? = nil) throws -> [[String: Node]] {
        guard !query.isEmpty else {
            throw DatabaseError.noQuery
        }
        
        let connection = try connection ?? makeConnection()

        return try connection.execute(query, values)
    }

    public func makeConnection() throws -> Connection {
        return try Connection(conninfo: conninfo)
    }
}
