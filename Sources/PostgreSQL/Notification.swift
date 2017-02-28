public struct Notification {
	let channel: String
	let payload: String?
	let pid: Int
}

extension Notification {
	init(relname: UnsafeMutablePointer<Int8>, extra: UnsafeMutablePointer<Int8>, be_pid: Int32) {
		self.channel = String(cString: relname)
		self.pid = Int(be_pid)
		
		if (extra.pointee != 0) {
			self.payload = String(cString: extra)
		}
		else {
			self.payload = nil
		}
	}
}
