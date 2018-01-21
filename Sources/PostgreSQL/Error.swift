import CPostgreSQL

/// A list of all Error messages that
/// can be thrown from calls to `Database`.
///
/// All Error objects contain a String which
/// contains PostgreSQL's last error message.
public struct PostgreSQLError: Error {
    public let code: Code
    public let reason: String
}

public enum PostgresSQLStatusError: Error {
    case emptyQuery
    case badResponse
}

public struct PostgreSQLCancelError: Error {
    public let reason: String
}

extension PostgreSQLError {
    public enum Code: String {
        // Class 01 — Warning
        case warning = "01000"
        case dynamicResultSetsReturned = "0100C"
        case implicitZeroBitPadding = "01008"
        case nullValueEliminatedInSetFunction = "01003"
        case privilegeNotGranted = "01007"
        case privilegeNotRevoked = "01006"
        case stringDataRightTruncationWarning = "01004"
        case deprecatedFeature = "01P01"
        // Class 02 — No Data (this is also a warning class per the SQL standard)
        case noData = "02000"
        case noAdditionalDynamicResultSetsReturned = "02001"
        // Class 03 — SQL Statement Not Yet Complete
        case sqlStatementNotYetComplete = "03000"
        // Class 08 — Connection Exception
        case connectionException = "08000"
        case connectionDoesNotExist = "08003"
        case connectionFailure = "08006"
        case sqlclientUnableToEstablishSqlconnection = "08001"
        case sqlserverRejectedEstablishmentOfSqlconnection = "08004"
        case transactionResolutionUnknown = "08007"
        case protocolViolation = "08P01"
        // Class 09 — Triggered Action Exception
        case triggeredActionException = "09000"
        // Class  0A — Feature Not Supported
        case featureNotSupported = "0A000"
        // Class  0B — Invalid Transaction Initiation
        case invalidTransactionInitiation = "0B000"
        // Class 0F — Locator Exception
        case locatorException = "0F000"
        case invalidLocatorSpecification = "0F001"
        // Class 0L — Invalid Grantor
        case invalidGrantor = "0L000"
        case invalidGrantOperation = "0LP01"
        // Class 0P — Invalid Role Specification
        case invalidRoleSpecification = "0P000"
        // Class 0Z — Diagnostics Exception
        case diagnosticsException = "0Z000"
        case stackedDiagnosticsAccessedWithoutActiveHandler = "0Z002"
        // Class 20 — Case Not Found
        case caseNotFound = "20000"
        // Class 21 — Cardinality Violation
        case cardinalityViolation = "21000"
        // Class 22 — Data Exception
        case dataException = "22000"
        case arraySubscriptError = "2202E"
        case characterNotInRepertoire = "22021"
        case datetimeFieldOverflow = "22008"
        case divisionByZero = "22012"
        case errorInAssignment = "22005"
        case escapeCharacterConflict = "2200B"
        case indicatorOverflow = "22022"
        case intervalFieldOverflow = "22015"
        case invalidArgumentForLogarithm = "2201E"
        case invalidArgumentForNtileFunction = "22014"
        case invalidArgumentForNthValueFunction = "22016"
        case invalidArgumentForPowerFunction = "2201F"
        case invalidArgumentForWidthBucketFunction = "2201G"
        case invalidCharacterValueForCast = "22018"
        case invalidDatetimeFormat = "22007"
        case invalidEscapeCharacter = "22019"
        case invalidEscapeOctet = "2200D"
        case invalidEscapeSequence = "22025"
        case nonstandardUseOfEscapeCharacter = "22P06"
        case invalidIndicatorParameterValue = "22010"
        case invalidParameterValue = "22023"
        case invalidRegularExpression = "2201B"
        case invalidRowCountInLimitClause = "2201W"
        case invalidRowCountInResultOffsetClause = "2201X"
        case invalidTablesampleArgument = "2202H"
        case invalidTablesampleRepeat = "2202G"
        case invalidTimeZoneDisplacementValue = "22009"
        case invalidUseOfEscapeCharacter = "2200C"
        case mostSpecificTypeMismatch = "2200G"
        case nullValueNotAllowed = "22004"
        case nullValueNoIndicatorParameter = "22002"
        case numericValueOutOfRange = "22003"
        case stringDataLengthMismatch = "22026"
        case stringDataRightTruncationException = "22001"
        case substringError = "22011"
        case trimError = "22027"
        case unterminatedCString = "22024"
        case zeroLengthCharacterString = "2200F"
        case floatingPointException = "22P01"
        case invalidTextRepresentation = "22P02"
        case invalidBinaryRepresentation = "22P03"
        case badCopyFileFormat = "22P04"
        case untranslatableCharacter = "22P05"
        case notAnXmlDocument = "2200L"
        case invalidXmlDocument = "2200M"
        case invalidXmlContent = "2200N"
        case invalidXmlComment = "2200S"
        case invalidXmlProcessingInstruction = "2200T"
        // Class 23 — Integrity Constraint Violation
        case integrityConstraintViolation = "23000"
        case restrictViolation = "23001"
        case notNullViolation = "23502"
        case foreignKeyViolation = "23503"
        case uniqueViolation = "23505"
        case checkViolation = "23514"
        case exclusionViolation = "23P01"
        // Class 24 — Invalid Cursor State
        case invalidCursorState = "24000"
        // Class 25 — Invalid Transaction State
        case invalidTransactionState = "25000"
        case activeSqlTransaction = "25001"
        case branchTransactionAlreadyActive = "25002"
        case heldCursorRequiresSameIsolationLevel = "25008"
        case inappropriateAccessModeForBranchTransaction = "25003"
        case inappropriateIsolationLevelForBranchTransaction = "25004"
        case noActiveSqlTransactionForBranchTransaction = "25005"
        case readOnlySqlTransaction = "25006"
        case schemaAndDataStatementMixingNotSupported = "25007"
        case noActiveSqlTransaction = "25P01"
        case inFailedSqlTransaction = "25P02"
        case idleInTransactionSessionTimeout = "25P03"
        // Class 26 — Invalid SQL Statement Name
        case invalidSqlStatementName = "26000"
        // Class 27 — Triggered Data Change Violation
        case triggeredDataChangeViolation = "27000"
        // Class 28 — Invalid Authorization Specification
        case invalidAuthorizationSpecification = "28000"
        case invalidPassword = "28P01"
        // Class 2B — Dependent Privilege Descriptors Still Exist
        case dependentPrivilegeDescriptorsStillExist = "2B000"
        case dependentObjectsStillExist = "2BP01"
        // Class 2D — Invalid Transaction Termination
        case invalidTransactionTermination = "2D000"
        // Class 2F — SQL Routine Exception
        case sqlRoutineException = "2F000"
        case functionExecutedNoReturnStatement = "2F005"
        case modifyingSqlDataNotPermittedSQL = "2F002"
        case prohibitedSqlStatementAttemptedSQL = "2F003"
        case readingSqlDataNotPermittedSQL = "2F004"
        // Class 34 — Invalid Cursor Name
        case invalidCursorName = "34000"
        // Class 38 — External Routine Exception
        case externalRoutineException = "38000"
        case containingSqlNotPermitted = "38001"
        case modifyingSqlDataNotPermittedExternal = "38002"
        case prohibitedSqlStatementAttemptedExternal = "38003"
        case readingSqlDataNotPermittedExternal = "38004"
        // Class 39 — External Routine Invocation Exception
        case externalRoutineInvocationException = "39000"
        case invalidSqlstateReturned = "39001"
        // case null_value_not_allowed = "39004"
        case triggerProtocolViolated = "39P01"
        case srfProtocolViolated = "39P02"
        case event_trigger_protocol_violated = "39P03"
        // Class 3B — Savepoint Exception
        case savepointException = "3B000"
        case invalidSavepointSpecification = "3B001"
        // Class 3D — Invalid Catalog Name
        case invalidCatalogName = "3D000"
        // Class 3F — Invalid Schema Name
        case invalidSchemaName = "3F000"
        // Class 40 — Transaction Rollback
        case transactionRollback = "40000"
        case transactionIntegrityConstraintViolation = "40002"
        case serializationFailure = "40001"
        case statementCompletionUnknown = "40003"
        case deadlockDetected = "40P01"
        // Class 42 — Syntax Error or Access Rule Violation
        case syntaxErrorOrAccessRuleViolation = "42000"
        case syntaxError = "42601"
        case insufficientPrivilege = "42501"
        case cannotCoerce = "42846"
        case groupingError = "42803"
        case windowingError = "42P20"
        case invalidRecursion = "42P19"
        case invalidForeignKey = "42830"
        case invalidName = "42602"
        case nameTooLong = "42622"
        case reservedName = "42939"
        case datatypeMismatch = "42804"
        case indeterminateDatatype = "42P18"
        case collationMismatch = "42P21"
        case indeterminateCollation = "42P22"
        case wrongObjectType = "42809"
        case undefinedColumn = "42703"
        case undefinedFunction = "42883"
        case undefinedTable = "42P01"
        case undefinedParameter = "42P02"
        case undefinedObject = "42704"
        case duplicateColumn = "42701"
        case duplicateCursor = "42P03"
        case duplicateDatabase = "42P04"
        case duplicateFunction = "42723"
        case duplicatePreparedStatement = "42P05"
        case duplicateSchema = "42P06"
        case duplicateTable = "42P07"
        case duplicateAlias = "42712"
        case duplicateObject = "42710"
        case ambiguousColumn = "42702"
        case ambiguousFunction = "42725"
        case ambiguousParameter = "42P08"
        case ambiguousAlias = "42P09"
        case invalidColumnReference = "42P10"
        case invalidColumnDefinition = "42611"
        case invalidCursorDefinition = "42P11"
        case invalidDatabaseDefinition = "42P12"
        case invalidFunctionDefinition = "42P13"
        case invalidPreparedStatementDefinition = "42P14"
        case invalidSchemaDefinition = "42P15"
        case invalidTableDefinition = "42P16"
        case invalidObjectDefinition = "42P17"
        // Class 44 — WITH CHECK OPTION Violation
        case withCheckOptionViolation = "44000"
        // Class 53 — Insufficient Resources
        case insufficientResources = "53000"
        case diskFull = "53100"
        case outOfMemory = "53200"
        case tooManyConnections = "53300"
        case configurationLimitExceeded = "53400"
        // Class 54 — Program Limit Exceeded
        case programLimitExceeded = "54000"
        case statementTooComplex = "54001"
        case tooManyColumns = "54011"
        case tooManyArguments = "54023"
        // Class 55 — Object Not In Prerequisite State
        case objectNotInPrerequisiteState = "55000"
        case objectInUse = "55006"
        case cantChangeRuntimeParam = "55P02"
        case lockNotAvailable = "55P03"
        // Class 57 — Operator Intervention
        case operatorIntervention = "57000"
        case queryCanceled = "57014"
        case adminShutdown = "57P01"
        case crashShutdown = "57P02"
        case cannotConnectNow = "57P03"
        case databaseDropped = "57P04"
        // Class 58 — System Error (errors external to PostgreSQL itself)
        case systemError = "58000"
        case ioError = "58030"
        case undefinedFile = "58P01"
        case duplicateFile = "58P02"
        // Class 72 — Snapshot Failure
        case snapshotTooOld = "72000"
        // Class F0 — Configuration File Error
        case configFileError = "F0000"
        case lockFileExists = "F0001"
        // Class HV — Foreign Data Wrapper Error (SQL/MED)
        case fdwError = "HV000"
        case fdwColumnNameNotFound = "HV005"
        case fdwDynamicParameterValueNeeded = "HV002"
        case fdwFunctionSequenceError = "HV010"
        case fdwInconsistentDescriptorInformation = "HV021"
        case fdwInvalidAttributeValue = "HV024"
        case fdwInvalidColumnName = "HV007"
        case fdwInvalidColumnNumber = "HV008"
        case fdwInvalidDataType = "HV004"
        case fdwInvalidDataTypeDescriptors = "HV006"
        case fdwInvalidDescriptorFieldIdentifier = "HV091"
        case fdwInvalidHandle = "HV00B"
        case fdwInvalidOptionIndex = "HV00C"
        case fdwInvalidOptionName = "HV00D"
        case fdwInvalidStringLengthOrBufferLength = "HV090"
        case fdwInvalidStringFormat = "HV00A"
        case fdwInvalidUseOfNullPointer = "HV009"
        case fdwTooManyHandles = "HV014"
        case fdwOutOfMemory = "HV001"
        case fdwNoSchemas = "HV00P"
        case fdwOptionNameNotFound = "HV00J"
        case fdwReplyHandle = "HV00K"
        case fdwSchemaNotFound = "HV00Q"
        case fdwTableNotFound = "HV00R"
        case fdwUnableToCreateExecution = "HV00L"
        case fdwUnableToCreateReply = "HV00M"
        case fdwUnableToEstablishConnection = "HV00N"
        case plpgsqlError = "P0000"
        case raiseException = "P0001"
        case noDataFound = "P0002"
        case tooManyRows = "P0003"
        case assertFailure = "P0004"
        // Class XX — Internal Error
        case internalError = "XX000"
        case dataCorrupted = "XX001"
        case indexCorrupted = "XX002"
        
        case unknown
     }
}

// MARK: Inits

extension PostgreSQLError {
    public init(code: Code, connection: Connection) {
        let reason: String
        if let error = PQerrorMessage(connection.cConnection) {
            reason = String(cString: error)
        }
        else {
            reason = "Unknown"
        }
        
        self.init(code: code, reason: reason)
    }
    
    public init(pointer: OpaquePointer?) {
        guard let pointer = pointer else {
            self.init(code: .unknown, reason: "Unknown")
            return
        }
        
        let code: Code
        if let rawCodePointer = PQresultErrorField(pointer, 67) { // 67 == 'C' == PG_DIAG_SQLSTATE
            let rawCode = String(cString: rawCodePointer)
            code = Code(rawValue: rawCode) ?? .unknown
        }
        else {
            code = .unknown
        }

        let reason: String
        if let messagePointer = PQresultErrorMessage(pointer) {
            reason = String(cString: messagePointer)
        }
        else {
            reason = "Unknown"
        }
        
        self.init(code: code, reason: reason)
    }

    public init(result: Result) {
        self.init(pointer: result.pointer)
    }
}

// MARK: Debuggable
import Debugging

extension PostgreSQLError: Debuggable {
    public static var readableName: String {
        return "PostgreSQL Error"
    }

    public var identifier: String {
        return "\(code.rawValue) (\(code))"
    }

    public var possibleCauses: [String] {
        switch code {
        case .connectionException, .connectionDoesNotExist, .connectionFailure:
            return [
                "The connection to the server degraded during the query",
                "The connection has been open for too long",
                "Too much data has been sent through the connection"
            ]
        default:
            return []
        }
    }

    public var suggestedFixes: [String] {
        switch code {
        case .syntaxError:
            return [
                "Fix the invalid syntax in your query",
                "If an ORM has generated this error, report the issue to its GitHub page"
            ]
        case .connectionException, .connectionFailure:
            return [
                "Make sure you have entered the correct username and password",
                "Make sure the database has been created"
            ]
        default:
            return []
        }
    }

    public var stackOverflowQuestions: [String] {
        return []
    }

    public var documentationLinks: [String] {
        return []
    }
}
