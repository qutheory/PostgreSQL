#if os(Linux)
    import CPostgreSQLLinux
#else
    import CPostgreSQLMac
#endif



public final class Connection: ConnInfoInitializable {
    public typealias ConnectionPointer = OpaquePointer
    public var configuration: Configuration?
    private(set) var connection: ConnectionPointer!

    public var connected: Bool {
        if let connection = connection, PQstatus(connection) == CONNECTION_OK {
            return true
        }
        return false
    }

    public init(conninfo: ConnInfo) throws {
        let string: String
        
        switch conninfo {
        case .raw(let ci):
            string = ci
        case .params(let params):
            var ci = ""
            
            params.forEach { (key, value) in
                ci += "\(key)='\(value)'"
            }
        
            string = ci
        case .basic(host: let host, port: let port, database: let database, user: let user, password: let password):
            string = "host='\(host)' port='\(port)' dbname='\(database)' user='\(user)' password='\(password)' client_encoding='UTF8'"
        }
        
        self.connection = PQconnectdb(string)
        if !self.connected {
            throw DatabaseError.cannotEstablishConnection(error)
        }
    }
    
    @discardableResult
    public func execute(_ query: String, _ values: [Node]? = []) throws -> [[String: Node]] {
        guard !query.isEmpty else {
            throw DatabaseError.noQuery
        }
        
        let values = values ?? []
        
        var types: [Oid] = []
        types.reserveCapacity(values.count)
        
        var paramValues: [[Int8]?] = []
        paramValues.reserveCapacity(values.count)
        
        var lengths: [Int32] = []
        lengths.reserveCapacity(values.count)
        
        var formats: [Int32] = []
        formats.reserveCapacity(values.count)
        
        for value in values {
            let (bytes, oid, format) = value.postgresBindingData
            paramValues.append(bytes)
            types.append(oid?.rawValue ?? 0)
            lengths.append(Int32(bytes?.count ?? 0))
            formats.append(format.rawValue)
        }
        
        let res: Result.Pointer = PQexecParams(
            connection, query,
            Int32(values.count),
            types, paramValues.map {
                UnsafePointer<Int8>($0)
            },
            lengths,
            formats,
            DataFormat.binary.rawValue
        )
        
        defer {
            PQclear(res)
        }
        
        switch Database.Status(result: res) {
        case .nonFatalError, .fatalError, .unknown:
            throw DatabaseError.invalidSQL(message: String(cString: PQresultErrorMessage(res)))
        case .tuplesOk:
            let configuration = try getConfiguration()
            return Result(configuration: configuration, pointer: res).parsed
        default:
            return []
        }
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
    
    // MARK: - Load Configuration
    
    private func getConfiguration() throws -> Configuration {
        if let configuration = self.configuration {
            return configuration
        }
        
        let hasIntegerDatetimes = getBooleanParameterStatus(key: "integer_datetimes", default: true)
        
        let configuration = Configuration(hasIntegerDatetimes: hasIntegerDatetimes)
        self.configuration = configuration
        
        return configuration
    }
    
    private func getBooleanParameterStatus(key: String, `default` defaultValue: Bool = false) -> Bool {
        guard let value = PQparameterStatus(connection, "integer_datetimes") else {
            return defaultValue
        }
        return String(cString: value) == "on"
    }
}
