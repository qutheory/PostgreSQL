#if os(Linux)

import XCTest
@testable import PostgreSQLTests

XCTMain([
    testCase(PostgreSQLTests.allTests)
])

#endif
