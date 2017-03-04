import Core

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
	
	public func listen(to channel: String, callback: @escaping (Notification) -> Void) throws {
		try background {
			do {
				let connection = try self.makeConnection()
				
				try self.execute("LISTEN \(channel)", on: connection)
				
				while true {
					if connection.connected != true {
						throw DatabaseError.cannotEstablishConnection(connection.error)
					}
					
					PQconsumeInput(connection.connection)
					
					while let pgNotify = PQnotifies(connection.connection) {
						let notification = Notification(relname: pgNotify.pointee.relname, extra: pgNotify.pointee.extra, be_pid: pgNotify.pointee.be_pid)
						
						callback(notification)
						
						PQfreemem(pgNotify)
					}
				}
			}
			catch {
				fatalError("\(error)")
			}
		}
	}

	public func notify(channel: String, payload: String?, on connection: Connection? = nil) throws {
		let connection = try connection ?? makeConnection()
		
		if let payload = payload {
			try execute("NOTIFY \(channel), '\(payload)'", on: connection)
		}
		else {
			try execute("NOTIFY \(channel)", on: connection)
		}
	}
	
    public func makeConnection() throws -> Connection {
        return try Connection(conninfo: conninfo)
    }
}
