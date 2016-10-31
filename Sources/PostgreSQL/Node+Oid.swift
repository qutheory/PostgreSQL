#if os(Linux)
	import CPostgreSQLLinux
#else
	import CPostgreSQLMac
#endif

public enum OID: Oid {
	case text = 25
	case varchar  = 1043
	case int2 = 21
	case int4 = 23
	case int8 = 20
	case float4 = 700
	case float8 = 701
	case numeric = 1700
	case bool = 16
	case char = 18
	case unknown = 705
}

extension Node {
	init(oid: Oid, value: String) {
		guard let type = OID(rawValue: oid) else {
			// Always fallback to string, allowing to use with custom types as strings
			self = .string(value)
			return
		}

		switch type {
		case .text, .varchar, .char:
			self = .string(value)
		case .int2, .int4, .int8:
			self = .number(.int(Int(value) ?? 0))
		case .float4, .float8, .numeric:
			self = .number(.double(Double(value) ?? 0))
		case .bool:
			self = .bool((value == "t") ? true : false)
		case .unknown:
			self = .null
		}
	}
}
