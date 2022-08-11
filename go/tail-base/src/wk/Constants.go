package wk

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"time"
	"unsafe"
)

const H byte = 'h'
const T byte = 't'
const EQ byte = '='
const E byte = 'e'
const O byte = 'o'
const C0 byte = '0'
const C1 byte = '1'
const C2 byte = '2'
const SP byte = '|'
const N byte = '\n'
const AND byte = '&'
const TYPE_ERR_ID = 0
const TYPE_ERR_DATA = 1
const TYPE_FINISH = 2
const TYPE_SET_PORT = 4
const TYPE_ADD_WAIT = 5
const TYPE_PROCESSOR_READY = 6
const ID_SKIP = 11
const C_PORT = 8003
const MAX_TRACE_LEN = 450
const DEBUG = false
const HOST = "localhost"

var HEX_NUM [127]int64
var L_PORT int

func Init() {
	fmt.Println("ONLINE: " + strconv.FormatBool(ONLINE))
	for i := '0'; i <= '9'; i++ {
		HEX_NUM[i] = int64(i - '0')
	}
	for i := 'A'; i <= 'F'; i++ {
		HEX_NUM[i] = int64(i - 'A' + 10)
	}
	for i := 'a'; i <= 'f'; i++ {
		HEX_NUM[i] = int64(i - 'a' + 10)
	}
	fmt.Println("arg: " + os.Args[1])
	l_port, err := strconv.ParseInt(os.Args[1], 10, 32)
	if err != nil {
		log_and_exit(err)
	}
	L_PORT = int(l_port)
}

func log_and_exit(msg interface{}) {
	fmt.Println(msg)
	os.Exit(-1)
}

func update_type(len int, raw_type int) uint32 {
	return uint32(uint(len) | (uint(raw_type) << 24))
}

func get_type_from_type_len(raw_len uint32) int {
	return int(raw_len >> 24)
}

func get_len_from_type_len(raw_len uint32) int {
	return int(raw_len & 0xffffff)
}

func connect(port int64) *net.Conn {
	for ; ; {
		var timeout = 100 * time.Second
		var address = HOST + ":" + strconv.FormatInt(port, 10)
		conn, err := net.DialTimeout("tcp", address, timeout)
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}
		return &conn
	}
}

func bytes_to_string(data []byte) *string {
	return (*string)(unsafe.Pointer(&data))
}

func indexOf(buf []byte, n byte, from int, to int) int {
	var size = to - from
	if size < 16 {
		return unroll_index_of(buf, n, from, size)
	} else {
		return index_of_16(buf, n, from, size)
	}
}

func index_of_16(m []byte, b byte, from int, size int) int {
	var group = 16
	var count = (size >> 4) << 4
	var s1_to = from + count
	for i := from; i < s1_to; i += group {
		if m[i] == b {
			return i
		}
		if m[i+1] == b {
			return i + 1
		}
		if m[i+2] == b {
			return i + 2
		}
		if m[i+3] == b {
			return i + 3
		}
		if m[i+4] == b {
			return i + 4
		}
		if m[i+5] == b {
			return i + 5
		}
		if m[i+6] == b {
			return i + 6
		}
		if m[i+7] == b {
			return i + 7
		}
		if m[i+8] == b {
			return i + 8
		}
		if m[i+9] == b {
			return i + 9
		}
		if m[i+10] == b {
			return i + 10
		}
		if m[i+11] == b {
			return i + 11
		}
		if m[i+12] == b {
			return i + 12
		}
		if m[i+13] == b {
			return i + 13
		}
		if m[i+14] == b {
			return i + 14
		}
		if m[i+15] == b {
			return i + 15
		}
	}
	return unroll_index_of(m, b, s1_to, size&(group-1))
}

func unroll_index_of(m []byte, b byte, from int, size int) int {
	switch size {
	case 1:
		if m[from] == b {
			return from
		}
		break
	case 2:
		if m[from] == b {
			return from
		}
		if m[from+1] == b {
			return from + 1
		}
		break
	case 3:
		if m[from] == b {
			return from
		}
		if m[from+1] == b {
			return from + 1
		}
		if m[from+2] == b {
			return from + 2
		}
		break
	case 4:
		if m[from] == b {
			return from
		}
		if m[from+1] == b {
			return from + 1
		}
		if m[from+2] == b {
			return from + 2
		}
		if m[from+3] == b {
			return from + 3
		}
		break
	case 5:
		if m[from] == b {
			return from
		}
		if m[from+1] == b {
			return from + 1
		}
		if m[from+2] == b {
			return from + 2
		}
		if m[from+3] == b {
			return from + 3
		}
		if m[from+4] == b {
			return from + 4
		}
		break
	case 6:
		if m[from] == b {
			return from
		}
		if m[from+1] == b {
			return from + 1
		}
		if m[from+2] == b {
			return from + 2
		}
		if m[from+3] == b {
			return from + 3
		}
		if m[from+4] == b {
			return from + 4
		}
		if m[from+5] == b {
			return from + 5
		}
		break
	case 7:
		if m[from] == b {
			return from
		}
		if m[from+1] == b {
			return from + 1
		}
		if m[from+2] == b {
			return from + 2
		}
		if m[from+3] == b {
			return from + 3
		}
		if m[from+4] == b {
			return from + 4
		}
		if m[from+5] == b {
			return from + 5
		}
		if m[from+6] == b {
			return from + 6
		}
		break
	case 8:
		if m[from] == b {
			return from
		}
		if m[from+1] == b {
			return from + 1
		}
		if m[from+2] == b {
			return from + 2
		}
		if m[from+3] == b {
			return from + 3
		}
		if m[from+4] == b {
			return from + 4
		}
		if m[from+5] == b {
			return from + 5
		}
		if m[from+6] == b {
			return from + 6
		}
		if m[from+7] == b {
			return from + 7
		}
		break
	case 9:
		if m[from] == b {
			return from
		}
		if m[from+1] == b {
			return from + 1
		}
		if m[from+2] == b {
			return from + 2
		}
		if m[from+3] == b {
			return from + 3
		}
		if m[from+4] == b {
			return from + 4
		}
		if m[from+5] == b {
			return from + 5
		}
		if m[from+6] == b {
			return from + 6
		}
		if m[from+7] == b {
			return from + 7
		}
		if m[from+8] == b {
			return from + 8
		}
		break
	case 10:
		if m[from] == b {
			return from
		}
		if m[from+1] == b {
			return from + 1
		}
		if m[from+2] == b {
			return from + 2
		}
		if m[from+3] == b {
			return from + 3
		}
		if m[from+4] == b {
			return from + 4
		}
		if m[from+5] == b {
			return from + 5
		}
		if m[from+6] == b {
			return from + 6
		}
		if m[from+7] == b {
			return from + 7
		}
		if m[from+8] == b {
			return from + 8
		}
		if m[from+9] == b {
			return from + 9
		}
		break
	case 11:
		if m[from] == b {
			return from
		}
		if m[from+1] == b {
			return from + 1
		}
		if m[from+2] == b {
			return from + 2
		}
		if m[from+3] == b {
			return from + 3
		}
		if m[from+4] == b {
			return from + 4
		}
		if m[from+5] == b {
			return from + 5
		}
		if m[from+6] == b {
			return from + 6
		}
		if m[from+7] == b {
			return from + 7
		}
		if m[from+8] == b {
			return from + 8
		}
		if m[from+9] == b {
			return from + 9
		}
		if m[from+10] == b {
			return from + 10
		}
		break
	case 12:
		if m[from] == b {
			return from
		}
		if m[from+1] == b {
			return from + 1
		}
		if m[from+2] == b {
			return from + 2
		}
		if m[from+3] == b {
			return from + 3
		}
		if m[from+4] == b {
			return from + 4
		}
		if m[from+5] == b {
			return from + 5
		}
		if m[from+6] == b {
			return from + 6
		}
		if m[from+7] == b {
			return from + 7
		}
		if m[from+8] == b {
			return from + 8
		}
		if m[from+9] == b {
			return from + 9
		}
		if m[from+10] == b {
			return from + 10
		}
		if m[from+11] == b {
			return from + 11
		}
		break
	case 13:
		if m[from] == b {
			return from
		}
		if m[from+1] == b {
			return from + 1
		}
		if m[from+2] == b {
			return from + 2
		}
		if m[from+3] == b {
			return from + 3
		}
		if m[from+4] == b {
			return from + 4
		}
		if m[from+5] == b {
			return from + 5
		}
		if m[from+6] == b {
			return from + 6
		}
		if m[from+7] == b {
			return from + 7
		}
		if m[from+8] == b {
			return from + 8
		}
		if m[from+9] == b {
			return from + 9
		}
		if m[from+10] == b {
			return from + 10
		}
		if m[from+11] == b {
			return from + 11
		}
		if m[from+12] == b {
			return from + 12
		}
		break
	case 14:
		if m[from] == b {
			return from
		}
		if m[from+1] == b {
			return from + 1
		}
		if m[from+2] == b {
			return from + 2
		}
		if m[from+3] == b {
			return from + 3
		}
		if m[from+4] == b {
			return from + 4
		}
		if m[from+5] == b {
			return from + 5
		}
		if m[from+6] == b {
			return from + 6
		}
		if m[from+7] == b {
			return from + 7
		}
		if m[from+8] == b {
			return from + 8
		}
		if m[from+9] == b {
			return from + 9
		}
		if m[from+10] == b {
			return from + 10
		}
		if m[from+11] == b {
			return from + 11
		}
		if m[from+12] == b {
			return from + 12
		}
		if m[from+13] == b {
			return from + 13
		}
		break
	case 15:
		if m[from] == b {
			return from
		}
		if m[from+1] == b {
			return from + 1
		}
		if m[from+2] == b {
			return from + 2
		}
		if m[from+3] == b {
			return from + 3
		}
		if m[from+4] == b {
			return from + 4
		}
		if m[from+5] == b {
			return from + 5
		}
		if m[from+6] == b {
			return from + 6
		}
		if m[from+7] == b {
			return from + 7
		}
		if m[from+8] == b {
			return from + 8
		}
		if m[from+9] == b {
			return from + 9
		}
		if m[from+10] == b {
			return from + 10
		}
		if m[from+11] == b {
			return from + 11
		}
		if m[from+12] == b {
			return from + 12
		}
		if m[from+13] == b {
			return from + 13
		}
		if m[from+14] == b {
			return from + 14
		}
		break
	}
	return -1
}

func lastIndexOf(data []byte, n byte, from int, to int) int {
	for p := from; p > to; p-- {
		if data[p] == n {
			return p
		}
	}
	return -1
}
