import Foundation

#if os(Linux)
    import CPostgreSQLLinux
#else
    import CPostgreSQLMac
#endif

public class Result {
    public typealias ResultPointer = OpaquePointer

    private(set) var resultPointer: ResultPointer?

    init(resultPointer: ResultPointer) {
        self.resultPointer = resultPointer
    }

    lazy var dictionary: [[String: Node]] = {
        let rowCount = PQntuples(self.resultPointer)
        let columnCount = PQnfields(self.resultPointer)

        guard rowCount > 0 && columnCount > 0 else {
            return []
        }

        var parsedData = [[String: Node]]()

        for row in 0..<rowCount {
            var item = [String: Node]()
            for column in 0..<columnCount {
                let name = String(cString: PQfname(self.resultPointer, Int32(column)))

                if PQgetisnull(self.resultPointer, row, column) == 1 {
                    item[name] = .null
                } else {
                    let value = String(cString: PQgetvalue(self.resultPointer, row, column))
                    let type = PQftype(self.resultPointer, column)
                    item[name] = Node(oid: type, value: value)
                }
            }
            parsedData.append(item)
        }

        return parsedData
    }()
}
