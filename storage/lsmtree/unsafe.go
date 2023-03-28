package lsmtree

import "unsafe"

// unsafeStringBytes returns a slice of bytes that is backed by the string s,
// without copying the underlying data. The returned slice must not be modified.
func unsafeBytes(s string) (b []byte) {
	if sd := unsafe.StringData(s); sd != nil {
		b = unsafe.Slice(sd, len(s))
	}

	return
}
