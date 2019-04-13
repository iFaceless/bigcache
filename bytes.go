// +build !appengine

package bigcache

import (
	"reflect"
	"unsafe"
)

// bytesToString 这个其实就是高性能的字节转字符串的做法
// 基本原理是 stringHeader 底层指向的就是 []byte
func bytesToString(b []byte) string {
	bytesHeader := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	strHeader := reflect.StringHeader{Data: bytesHeader.Data, Len: bytesHeader.Len}
	return *(*string)(unsafe.Pointer(&strHeader))
}
