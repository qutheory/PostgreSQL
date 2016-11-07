#if os(Linux)

import XCTest
@testable import PostgreSQLTests

XCTMain([
    testCase(ArrayTests.allTests),
    testCase(BinaryUtilsTests.allTests),
    testCase(PostgreSQLTests.allTests),
])

#endif
