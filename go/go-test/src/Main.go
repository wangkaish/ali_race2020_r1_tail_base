package main

import (
	"crypto/md5"
	"encoding/hex"
	"reflect"
	"time"
	"unsafe"
)

func main() {

	var test = make(map[int]int, 10)
	println(&test)
	test_struct_param(test)
	println(test[0])
	test_struct_size()

}

func test_struct_param(test map[int]int) {
	test[0] = 10
	println(&test)
}

func test_struct_size()  {
	var array = make([]byte, 32)
	var test1 = Test{}
	var test2 = Test{data2: &(array)}
	println(reflect.TypeOf(test1.data2))
	println(reflect.TypeOf(test2.data2))
	println(unsafe.Sizeof(test1))
	println(unsafe.Sizeof(test2))



}

func test_md5() {

	hash := md5.New()

	var str = "abc"
	var bytes = (*[]byte)(unsafe.Pointer(&str))
	hash.Write(*bytes)

	str = "123"
	bytes = (*[]byte)(unsafe.Pointer(&str))
	hash.Write(*bytes)

	var res = hex.EncodeToString(hash.Sum(nil))

	println(res)
}

func test_time() {
	nano1 := time.Now().UnixNano()
	nano2 := time.Now().UnixNano()
	println(nano1)
	println(nano2 - nano1)
}

type Test struct {
	data2 *[]byte
}
