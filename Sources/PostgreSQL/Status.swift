
#if os(Linux)
    import CPostgreSQLLinux
#else
    import CPostgreSQLMac
#endif


extension Database {
    public enum Status {
        case commandOk
        case tuplesOk
        case copyOut
        case copyIn
        case copyBoth
        case singleTuple
        case badResponse
        case nonFatalError
        case fatalError
        case emptyQuery
        case unknown

        init(result: Result.Pointer) {
            switch PQresultStatus(result) {
            case PGRES_COMMAND_OK:
                self = .commandOk
            case PGRES_TUPLES_OK:
                self = .tuplesOk
            case PGRES_SINGLE_TUPLE:
                self = .singleTuple
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
            default:
                self = .unknown
            }
        }
    }
}
