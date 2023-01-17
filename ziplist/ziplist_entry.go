package ziplist

type zlentry struct {
	prevRawLenSize int   // 前一个元素长度编码字节
	prevRawLen     int   // 前一个元素长度
	lenSize        int   // 当前元素长度编码字节
	len            int   // 当前元素长度
	headerSize     int   // prevRawLenSize + lenSize
	encoding       uint8 // zipStr* 或者 zipInt* 的编码值，对于4bit 直接是数字的范围值，我们需要对此范围做校验
	p              []byte
}

func zipEntry(p []byte, e *zlentry) {
	zipDecodePrevLen(p, &e.prevRawLenSize, &e.prevRawLen)
	zipDecodeLength(p[e.prevRawLenSize:], &e.encoding, &e.lenSize, &e.len)
	e.headerSize = e.prevRawLenSize + e.lenSize
	e.p = p
}
