//
//  Value+UTF8.swift
//  PostgreSQL
//
//  Created by Prince Ugwuh on 7/13/16.
//
//

import Foundation


extension Node {
    var utf8: String.UTF8View {
        switch self {
        case .string(let str):
            return str.utf8
        case .bool(let bool):
            return bool ? "true".utf8 : "false".utf8
		case .number(.double(let double)):
			return String(double).utf8
		case .number(.int(let int)):
			return String(int).utf8
        default:
            return String(UInt8(0)).utf8
        }
    }
}
