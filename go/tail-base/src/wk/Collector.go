package wk

import (
	"../intintmap"
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"hash"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"unsafe"
)

const TRACE_COUNT = 5000
const MAX_ERR_TRACE = 256

var TRACE_POS_ARRAY [MAX_ERR_TRACE * TRACE_COUNT]uint32
var TRACE_TIME_ARRAY [MAX_ERR_TRACE * TRACE_COUNT]int64
var TRACE_DATA [MAX_TRACE_LEN * MAX_ERR_TRACE * TRACE_COUNT]byte
var TRACE_MAP = make(map[int64]*Trace, 1024*16)
var RESULT [1024 * 1024]byte
var result_write_index = 0
var TIME_SET = intintmap.New(MAX_ERR_TRACE*2*TRACE_COUNT, 0.5)
var score_header [1024]byte
var score_header_write_index = 0
var processor_connected = 0
var finish_1 = 0
var finish_2 = 0
var trace_index = 0
var ch_score net.Conn
var ch_8000_0 net.Conn
var ch_8000_1 net.Conn
var ch_8001_0 net.Conn
var ch_8001_1 net.Conn

var hexes = []byte{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'}

func Run_collector(port int) {
	collector_init(port)
	go listener_collector_http(port)
	go listener_8003()
}

func collector_init(port int) {
	append_result('r')
	append_result('e')
	append_result('s')
	append_result('u')
	append_result('l')
	append_result('t')
	append_result('=')
	append_left_brace()

	var req = "POST /api/finished HTTP/1.1\r\n" +
		"Host: localhost\r\n" +
		"Content-Type: application/x-www-form-urlencoded\r\n" +
		"Content-Length: "
	copy(score_header[0:], req)
	score_header_write_index += len(req)
}

func listener_collector_http(port int) {
	http.HandleFunc("/ready", Handle_collector_http_ready)
	http.HandleFunc("/setParameter", Handle_collector_http_set_parameter)
	var addr = HOST + ":" + strconv.FormatInt(int64(port), 10)
	err := http.ListenAndServe(addr, nil)
	if err != nil {
		fmt.Println("服务器错误")
	}
}

func Handle_collector_http_ready(res http.ResponseWriter, req *http.Request) {
	if processor_connected == 5 {
		res.WriteHeader(200)
	} else {
		res.WriteHeader(503)
	}
}

func Handle_collector_http_set_parameter(res http.ResponseWriter, req *http.Request) {
	if ONLINE {
		port, err := strconv.ParseInt(req.URL.Query().Get("port"), 10, 32)
		if err != nil {
			log_and_exit(err)
		}
		res.WriteHeader(200)
		ch_score = *connect(port)
	}
}

func listener_8003() {
	var addr = HOST + ":" + strconv.FormatInt(int64(C_PORT), 10)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log_and_exit(err)
		return
	}
	if OFFLINE {
		fmt.Println("服务器等待客户端建立连接...")
	}
	for true {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("accept err:", err)
			return
		}
		go handle_8003(&conn)
	}
}

func handle_8003(_conn *net.Conn) {
	var _MD5 = md5.New()
	var MD5 = &(_MD5)
	var conn = *_conn
	if OFFLINE {
		fmt.Printf("客户端与服务器连接建立成功...r: %s, l: %s \n", conn.RemoteAddr(), conn.LocalAddr())
	}
	var buf_len = 1024 * 512
	var read_buf = make([]byte, buf_len)
	var write_buf = make([]byte, buf_len)
	var write_index = 0
	var read_index = 0
	for true {
		n, err := conn.Read(read_buf[write_index:])
		if err != nil {
			fmt.Println("read err:", err)
			return
		}
		write_index += n
		for true {
			var raw_len = binary.LittleEndian.Uint32(read_buf[read_index:])
			var data_len = get_len_from_type_len(raw_len)
			if write_index-read_index < data_len {
				if read_index > 0 {
					var remain = write_index - read_index
					copy(read_buf[0:], read_buf[read_index:write_index])
					read_index = 0
					write_index = remain
				}
				break
			}
			read_index += 4
			var _type = get_type_from_type_len(raw_len)
			if _type == TYPE_ERR_ID {
				var data = read_buf[read_index-4 : read_index+data_len]
				read_index += data_len
				if conn == ch_8000_0 {
					ch_8001_0.Write(data)
				} else if conn == ch_8000_1 {
					ch_8001_1.Write(data)
				} else if conn == ch_8001_0 {
					ch_8000_0.Write(data)
				} else if conn == ch_8001_1 {
					ch_8000_1.Write(data)
				}
			} else if _type == TYPE_ADD_WAIT {
				var data = read_buf[read_index-4 : read_index]
				if conn == ch_8000_0 {
					ch_8001_0.Write(data)
				} else if conn == ch_8000_1 {
					ch_8001_1.Write(data)
				} else if conn == ch_8001_0 {
					ch_8000_0.Write(data)
				} else if conn == ch_8001_1 {
					ch_8000_1.Write(data)
				}
			} else if _type == TYPE_ERR_DATA {
				var trace_id = int64(binary.LittleEndian.Uint64(read_buf[read_index:]))
				var trace = getTrace(trace_id)
				var fromIndex = read_index + 8
				var toIndex = read_index + data_len
				read_index = toIndex
				trace.write(read_buf[fromIndex:toIndex], MD5)
			} else if _type == TYPE_FINISH {
				var s_type = binary.LittleEndian.Uint32(read_buf[read_index:])
				read_index += 4
				if s_type == 1 {
					finish_1++
					if finish_1 == 4 {
						var data = write_buf[0:4]
						binary.LittleEndian.PutUint32(data, update_type(0, TYPE_FINISH))
						ch_8000_0.Write(data)
						ch_8000_1.Write(data)
						ch_8001_0.Write(data)
						ch_8001_1.Write(data)
					}
				} else {
					finish_2++
					if finish_2 == 4 {
						for _, trace := range TRACE_MAP {
							append_trace(trace)
						}
						result_write_index -= 3
						append_right_brace()
						var len_str = strconv.FormatInt(int64(result_write_index), 10)
						var len_bytes = (*[]byte)(unsafe.Pointer(&len_str))
						copy(score_header[score_header_write_index:], *len_bytes)
						score_header_write_index += len(*len_bytes)
						score_header[score_header_write_index] = '\r'
						score_header_write_index++
						score_header[score_header_write_index] = '\n'
						score_header_write_index++
						score_header[score_header_write_index] = '\r'
						score_header_write_index++
						score_header[score_header_write_index] = '\n'
						score_header_write_index++
						if OFFLINE {
							var str = bytes_to_string(RESULT[0:result_write_index])
							res, err := url.PathUnescape(*str)
							if err != nil {
								log_and_exit(err)
							}
							fmt.Println(res)
							str = bytes_to_string(score_header[0:score_header_write_index])
							fmt.Println(*str)
						}
						if ONLINE {
							ch_score.Write(score_header[0:score_header_write_index])
							ch_score.Write(RESULT[0:result_write_index])
						}
					}
				}
			} else if _type == TYPE_SET_PORT {
				var port = binary.LittleEndian.Uint32(read_buf[read_index:])
				read_index += 4
				if port == 80000 {
					ch_8000_0 = conn
					if OFFLINE {
						fmt.Println("set_port_8000_0")
					}
				} else if port == 80001 {
					ch_8000_1 = conn
					if OFFLINE {
						fmt.Println("set_port_8000_1")
					}
				} else if port == 80010 {
					ch_8001_0 = conn
					if OFFLINE {
						fmt.Println("set_port_8001_0")
					}
				} else if port == 80011 {
					ch_8001_1 = conn
					if OFFLINE {
						fmt.Println("set_port_8001_1")
					}
				}
				processor_connected++
				if processor_connected == 4 {
					if OFFLINE {
						fmt.Println("ready...")
					}
					var _temp = write_buf[0:4]
					binary.LittleEndian.PutUint32(_temp, update_type(0, TYPE_PROCESSOR_READY))
					ch_8000_0.Write(_temp)
					ch_8001_0.Write(_temp)
					processor_connected++
				}
			} else {
				log_and_exit("error read type: " + strconv.FormatInt(int64(raw_len), 10))
			}
			if read_index == write_index {
				read_index = 0
				write_index = 0
				break
			}
		}
	}
}

func append_trace(_trace *Trace) {
	//        =  %3D
	//        {  %7B
	//        "  %22
	//        :  %3A
	//        }  %7D
	//        ,  %2C
	var trace = *_trace
	var data_off = trace.data_off
	var end = data_off + trace.id_len
	var md5 = trace.md5_array
	append_double_q()
	for i := data_off; i < end; i++ {
		append_result(TRACE_DATA[i])
	}
	append_double_q()
	append_colon()
	append_double_q()
	for i := 0; i < 16; i++ {
		var b = (md5[i]) & 0xff
		append_result(hexes[b>>4])
		append_result(hexes[b&0xf])
	}
	append_double_q()
	append_comma()
}

func getTrace(trace_id int64) *Trace {
	var trace = TRACE_MAP[trace_id]
	if trace == nil {
		trace = &(Trace{})
		trace.init(trace_id)
		TRACE_MAP[trace_id] = trace
	}
	return trace
}

type Trace struct {
	p         int
	data_off  int
	pt_off    int
	id_len    int
	size      int
	data_size int
	md5_array [16]byte
}

func (t *Trace) init(trace_id int64) {
	if t.id_len == 0 {
		t.p = trace_index
		t.pt_off = t.p * MAX_ERR_TRACE
		t.data_off = t.p * MAX_TRACE_LEN * MAX_ERR_TRACE
		t.id_len = get_id_len(trace_id)
		trace_index++
	}
}

func (t *Trace) md5(_MD5 *hash.Hash) {
	var MD5 = *_MD5
	MD5.Reset()
	var pt_off = t.pt_off
	var pt_end = pt_off + t.size
	var array Int64s = TRACE_TIME_ARRAY[pt_off:pt_end]
	sort.Sort(array)
	for i := pt_off; i < pt_end; i++ {
		var raw_time = TRACE_TIME_ARRAY[i]
		var index = get_trace_index(raw_time)
		var raw_pos = TRACE_POS_ARRAY[pt_off+index]
		var pos = get_trace_pos(raw_pos)
		var len = get_trace_len(raw_pos)
		var td_slice = TRACE_DATA[t.data_off+pos : t.data_off+pos+len]
		MD5.Write(td_slice)
	}
	var res = (MD5.Sum(nil))
	copy(t.md5_array[0:16], res)
}

func (t *Trace) write(buf []byte, MD5 *hash.Hash) {
	var pt_off = t.pt_off
	var data_off = t.data_off
	var data_size = t.data_size
	var i = t.size

	var buf_len = len(buf)
	var buf_off = 0

	for ; buf_off < buf_len; {
		var len = int(binary.LittleEndian.Uint16(buf[buf_off : buf_off+2]))
		buf_off += 2
		copy(TRACE_DATA[data_off+data_size:data_off+data_size+len], buf[buf_off:buf_off+len])
		buf_off += len
		var time_off = data_off + data_size + t.id_len + 1
		var time = get_trace_time_from_trace_line(TRACE_DATA[time_off : time_off+16])
		_, exists := TIME_SET.Get(time)
		if exists {
			continue
		} else {
			TIME_SET.Put(time, 1)
		}
		TRACE_TIME_ARRAY[pt_off+i] = to_trace_time(time, i)
		TRACE_POS_ARRAY[pt_off+i] = to_trace_pos(data_size, len)
		data_size += len
		i++
	}
	t.size = i
	t.data_size = data_size
	t.md5(MD5)
	if DEBUG {
		if t.size > MAX_ERR_TRACE {
			log_and_exit("err_trace_overflow")
		}
	}
}

func append_eq() {
	append_result('%')
	append_result('3')
	append_result('D')
}

func append_left_brace() {
	append_result('%')
	append_result('7')
	append_result('B')
}

func append_double_q() {
	append_result('%')
	append_result('2')
	append_result('2')
}

func append_colon() {
	append_result('%')
	append_result('3')
	append_result('A')
}

func append_right_brace() {
	append_result('%')
	append_result('7')
	append_result('D')
}

func append_comma() {
	append_result('%')
	append_result('2')
	append_result('C')
}

func append_result(b byte) {
	RESULT[result_write_index] = b
	result_write_index++
}

func get_trace_time_from_trace_line(data []byte) int64 {
	var end = len(data)
	var time int64 = 0
	for i := 0; i < end; i++ {
		time = time*10 + int64(data[i]-'0')
	}
	return time
}

type Int64s []int64

func (a Int64s) Len() int {
	return len(a)
}

func (a Int64s) Less(i, j int) bool {
	return a[i] < a[j]
}

func (a Int64s) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func get_id_len(trace_id int64) int {
	var len = ID_SKIP
	trace_id = trace_id >> uint64(len*4)
	for true {
		if trace_id > 0 {
			len++
		} else {
			return len
		}
		trace_id >>= 4
	}
	return -1
}

func to_trace_pos(pos int, len int) uint32 {
	return uint32((pos << 16) | len)
}

func get_trace_pos(raw_pos uint32) int {
	return int(raw_pos >> 16)
}

func get_trace_len(raw_pos uint32) int {
	return int(raw_pos & 0xffff)
}

func to_trace_time(time int64, index int) int64 {
	return (time << 7) | int64(index)
}

func get_trace_time_from_raw_time(raw_time uint64) uint64 {
	return raw_time >> 7
}

func get_trace_index(raw_time int64) int {
	return int(raw_time & (0xff >> 1))
}
