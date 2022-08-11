package wk

import (
	"../intintmap"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"
)

const STABLE_CHECK = true

var CONTENT_LENGTH_MATCH = "Content-Length:"
var T_START_OFF = 128
var WINDOW_SIZE = 1024 * 256
var READ_BUF_SIZE = 1024 * 1024
var SEND_WAIT_BLOCK_RATE = 1024 * 4
var WAIT_BLOCK_SIZE = WINDOW_SIZE/SEND_WAIT_BLOCK_RATE - 8
var POS_ARRAY_SIZE = WINDOW_SIZE / 16
var WINDOW_BUF_SIZE = (WINDOW_SIZE * MAX_TRACE_LEN / READ_BUF_SIZE)
var HTTP_CODE = "http.status_code="
var ERROR_CODE = "error=1"
var KMP_HTTP_CODE_LEN = len("http.status_code=")
var PW_ARRAY [2]ProcessorWindow
var file_path string
var processor_ready = false

var r_port = 18080

func Run_processor(port int) {
	processor_init(port)
	go listener_processor_http(port)
}

func processor_init(port int) {
	if port == 8000 {
		file_path = "/trace1.data"
	} else {
		file_path = "/trace2.data"
	}
	fmt.Println("download: " + file_path)

	PW_ARRAY[0] = *NewProcessorWindow(0)
	PW_ARRAY[1] = *NewProcessorWindow(1)

	go connect_to_collector(&PW_ARRAY[0])
	go connect_to_collector(&PW_ARRAY[1])
}

func listener_processor_http(port int) {
	http.HandleFunc("/ready", Handle_processor_http_ready)
	http.HandleFunc("/setParameter", Handle_processor_http_set_parameter)
	var addr = HOST + ":" + strconv.FormatInt(int64(port), 10)
	err := http.ListenAndServe(addr, nil)
	if err != nil {
		fmt.Println("服务器错误")
	}
}

func Handle_processor_http_ready(res http.ResponseWriter, req *http.Request) {
	if processor_ready {
		res.WriteHeader(200)
	} else {
		res.WriteHeader(503)
	}
}

func Handle_processor_http_set_parameter(res http.ResponseWriter, req *http.Request) {
	port, err := strconv.ParseInt(req.URL.Query().Get("port"), 10, 32)
	if err != nil {
		log_and_exit(err)
	}
	if OFFLINE {
		port = int64(r_port)
	}
	connect_to_get_length(port)
	res.WriteHeader(200)
}

func connect_to_get_length(port int64) {
	var durl = "http://localhost:" + strconv.FormatInt(port, 10) + file_path
	client := http.DefaultClient
	client.Timeout = time.Second * 60 //设置超时时间
	res, err := client.Head(durl)
	if err != nil {
		log_and_exit(err)
	}
	if ONLINE {
		if L_PORT == 8000 {
			time.Sleep(100 * time.Millisecond)
		}
	}
	var length = res.ContentLength
	var redundancy int64 = 1024 * 1024 * 16
	var half = length / 2
	if OFFLINE {
		fmt.Println("content length: ", length)
	}
	go connect_to_download(&PW_ARRAY[0], &durl, 0, half+redundancy)
	go connect_to_download(&PW_ARRAY[1], &durl, half-redundancy, length-1)
}

func connect_to_download(pw *ProcessorWindow, durl *string, start int64, end int64) {
	var client = http.Client{}
	request, err := http.NewRequest("GET", *durl, nil)
	if err != nil {
		log_and_exit(err)
	}
	client.Timeout = 3000 * time.Second
	request.Header.Add("Range", "bytes="+strconv.FormatInt(start, 10)+"-"+strconv.FormatInt(end, 10))
	res, err := client.Do(request)
	if err != nil {
		log_and_exit(err)
	}
	var content_length = res.ContentLength
	var body = res.Body
	var write_data_len = 1024 * 512
	var write_data = make([]byte, write_data_len)
	if OFFLINE {
		fmt.Printf("url, content-length: \n", durl, content_length)
	}

	var read_index = 0
	var write_index = 0
	if pw.index == 1 {
		for ; ; {
			var read_buf = pw.get_read_buf()
			n, err := body.Read(read_buf[write_index:])
			if err != nil {
				log_and_exit(err)
			}
			write_index += n
			var n_pos = indexOf(read_buf, N, 0, write_index)
			if n_pos == -1 {
				continue
			}
			read_index = n_pos + 1
			break
		}
	}

	for ; ; {
		var read_buf = pw.get_read_buf()
		n, err := body.Read(read_buf[write_index:])
		if err != nil {
			if err != io.EOF {
				log_and_exit(err)
			}
		}
		write_index += n
		if pw.download_length+int64(write_index) == content_length {
			pw.download_length = content_length
			if pw.index == 0 {
				var n_pos = lastIndexOf(read_buf, N, write_index, read_index)
				write_index = n_pos + 1
			}
			handle_buf(pw, read_buf, read_index, write_index, write_data)
			call_finish(pw, write_data)
			if OFFLINE {
				fmt.Println("read buf complete")
			}
			break
		} else {
			if write_index < READ_BUF_SIZE {
				continue
			}
			var new_read_buf = pw.next_read_buf()
			var n_pos = lastIndexOf(read_buf, N, write_index-1, 0) + 1
			var len = write_index - n_pos
			if len < 20 {
				n_pos = lastIndexOf(read_buf, N, n_pos-2, 0) + 1
				len = write_index - n_pos
			}
			copy(new_read_buf[0:], read_buf[n_pos:write_index])

			var buf_read_index = read_index
			var buf_write_index = n_pos

			read_index = 0
			write_index = len

			pw.download_length += int64(n_pos)
			handle_buf(pw, read_buf, buf_read_index, buf_write_index, write_data)
		}
	}
}

func handle_buf(pw *ProcessorWindow, buf []byte, read_index int, write_index int, write_data []byte) {
	var startTime int64 = 0
	if OFFLINE {
		startTime = time.Now().UnixNano()
	}
	var err_set = pw.err_set
	var err_list = pw.err_list
	var trace_map = pw.trace_map
	var window_array = pw.window_array
	var pos_array = pw.pos_array
	var win_index = pw.win_index
	var buf_index = pw.buf_index
	var r_index = read_index
	var w_index = write_index
	var send_block_count = pw.send_block_count
	pw.fill_err_set()
	err_list.clear()
	for ; ; {
		var rm_trace_id = window_array[win_index]
		if rm_trace_id != 0 {
			handle_rm_trace_id(pw, write_data, rm_trace_id)
		}
		var r_cursor = indexOf(buf, SP, r_index+ID_SKIP, w_index)

		var trace_id = get_long_from_hex(buf, r_index, r_cursor)

		r_cursor = indexOf(buf, SP, r_cursor+1+16, w_index)      //startTime
		r_cursor = indexOf(buf, SP, r_cursor+1+ID_SKIP, w_index) //spanId
		if buf[r_cursor+2] == SP {
			r_cursor += 2
		} else {
			r_cursor = indexOf(buf, SP, r_cursor+1+ID_SKIP, w_index) //parentSpanId
		}
		r_cursor = indexOf(buf, SP, r_cursor+1, w_index)   //duration
		r_cursor = indexOf(buf, SP, r_cursor+1, w_index)   //serviceName
		r_cursor = indexOf(buf, SP, r_cursor+1, w_index)   //spanName
		r_cursor = indexOf(buf, SP, r_cursor+1+8, w_index) //host

		var ns = r_cursor + 1

		var t_start, exists = trace_map.Get(trace_id)
		if !exists {
			t_start = pw.acquire_pos()
			trace_map.Put(trace_id, t_start)
			if DEBUG {
				var t_off = int(t_start) * T_START_OFF
				var t_size = pos_array[t_off]
				if t_size != 0 {
					log_and_exit("t_size_error")
				}
			}
		}
		var t_off = int(t_start) * T_START_OFF
		var t_size = pos_array[t_off]
		pos_array[t_off] = t_size + 1
		if DEBUG {
			if int(t_size+2) > T_START_OFF {
				log_and_exit("t_start_off")
			}
		}

		_, exists = err_set.Get(trace_id)

		var ne = 0
		if STABLE_CHECK {
			ne = indexOf(buf, N, ns, w_index) + 1
			if check_error(buf, ns, ne-1) {
				err_list.Push(trace_id)
				err_set.Put(trace_id, 1)
			}
		} else {
			if !exists {
				_ne, is_err := check_error_2(buf, ns, w_index)
				if is_err {
					err_list.Push(trace_id)
					err_set.Put(trace_id, 1)
				}
				ne = _ne + 1
			} else {
				ne = indexOf(buf, N, ns, w_index) + 1
			}
		}

		if DEBUG {
			if buf[ne-1] != N {
				log_and_exit("find_n_error")
			}
		}

		var t_size_value = get_t_size_value(buf_index, r_index, ne)
		pos_array[(t_off + int(t_size) + 1)] = t_size_value

		window_array[win_index] = trace_id
		send_block_count++
		if send_block_count == SEND_WAIT_BLOCK_RATE {
			send_block_count = 0
			binary.LittleEndian.PutUint32(write_data, update_type(0, TYPE_ADD_WAIT))
			(*(pw.ch_collector)).Write(write_data[0:4])
			pw.wait_block()
		}
		if ne == w_index {
			break
		}
		win_index = inc_win_index(win_index)
		r_index = ne
	}
	var error_size = err_list.size()
	if error_size > 0 {
		var write_index = 4
		binary.LittleEndian.PutUint32(write_data, update_type(error_size*8, TYPE_ERR_ID))
		for i := 0; i < error_size; i++ {
			var err_id = err_list.get(i)
			binary.LittleEndian.PutUint64(write_data[write_index:], uint64(err_id))
			write_index += 8
		}
		(*(pw.ch_collector)).Write(write_data[0:write_index])
	}
	pw.win_index = inc_win_index(win_index)
	pw.buf_index = inc_buf_index(buf_index)
	pw.send_block_count = send_block_count
	pw.handled++

	if OFFLINE {
		var cost = (time.Now().UnixNano() - startTime) / 1000
		fmt.Printf("handled buf: pw: %d, buf: %d, cost: %d \n", pw.index, pw.handled, cost)
	}
}

func check_error(buf []byte, off int, end int) bool {
	var end1 = end - 19
	var i = off
	if buf[i] == '&' {
		i++
	}
	for ; i < end1; {
		var c = buf[i]
		if c == H {
			if buf[i+16] == EQ {
				return buf[i+17] != C2
			} else {
				i = indexOf(buf, AND, i+1, end) + 1
				if i == 0 {
					return false
				}
			}
		} else if c == E {
			if buf[i+5] == EQ {
				if buf[i+6] == C1 {
					return true
				} else {
					i += 8
				}
			} else {
				i = indexOf(buf, AND, i+1, end) + 1
				if i == 0 {
					return false
				}
			}
		} else if c == AND {
			i++
		} else {
			i = indexOf(buf, AND, i+1, end) + 1
			if i == 0 {
				return false
			}
		}
	}
	for ; i < end; {
		var c = buf[i]
		if c == E {
			if (i+6 < end) && (buf[i+5] == EQ) {
				if buf[i+6] == C1 {
					return true
				} else {
					i += 8
				}
			} else {
				i = indexOf(buf, AND, i+1, end)
				if i == -1 {
					return false
				}
			}
		} else if c == AND {
			i++
		} else {
			i = indexOf(buf, AND, i+1, end)
			if i == -1 {
				return false
			}
		}
	}
	return false
}

func check_error_2(buf []byte, off int, end int) (int, bool) {
	var i = off
	if buf[i] != AND {
		var c = buf[i]
		if c == H {
			if buf[i+16] == EQ {
				return indexOf(buf, N, i+20, end), buf[i+17] != C2
			} else {
				i++
			}
		} else if c == E {
			if buf[i+5] == EQ {
				if buf[i+6] == C1 {
					return indexOf(buf, N, i+8, end), true
				} else {
					i += 7
				}
			} else {
				i++
			}
		} else if c == N {
			return i, false
		} else {
			i++
		}
	}
	for ; i < end; {
		var c = buf[i]
		if c == AND {
			i++
			c = buf[i]
			if c == AND {
				i++
				c = buf[i]
			}
			if c == H {
				if buf[i+16] == EQ {
					return indexOf(buf, N, i+20, end), buf[i+17] != C2
				} else {
					i++
				}
			} else if c == E {
				if buf[i+5] == EQ {
					if buf[i+6] == C1 {
						return indexOf(buf, N, i+8, end), true
					} else {
						i += 7
					}
				} else {
					i++
				}
			} else if c == N {
				return i, false
			} else {
				i++
			}
		} else if c == N {
			return i, false
		} else {
			i++
		}
	}
	log_and_exit("find_err_trace_error")
	return 0, false
}

func get_long_from_hex(buf []byte, off int, end int) int64 {
	var v int64 = 0
	var size = end - off
	switch size {
	case 11:
		v = (v << 4) | (HEX_NUM[buf[off]])
		v = (v << 4) | (HEX_NUM[buf[off+1]])
		v = (v << 4) | (HEX_NUM[buf[off+2]])
		v = (v << 4) | (HEX_NUM[buf[off+3]])
		v = (v << 4) | (HEX_NUM[buf[off+4]])
		v = (v << 4) | (HEX_NUM[buf[off+5]])
		v = (v << 4) | (HEX_NUM[buf[off+6]])
		v = (v << 4) | (HEX_NUM[buf[off+7]])
		v = (v << 4) | (HEX_NUM[buf[off+8]])
		v = (v << 4) | (HEX_NUM[buf[off+9]])
		v = (v << 4) | (HEX_NUM[buf[off+10]])
		return v
	case 12:
		v = (v << 4) | (HEX_NUM[buf[off]])
		v = (v << 4) | (HEX_NUM[buf[off+1]])
		v = (v << 4) | (HEX_NUM[buf[off+2]])
		v = (v << 4) | (HEX_NUM[buf[off+3]])
		v = (v << 4) | (HEX_NUM[buf[off+4]])
		v = (v << 4) | (HEX_NUM[buf[off+5]])
		v = (v << 4) | (HEX_NUM[buf[off+6]])
		v = (v << 4) | (HEX_NUM[buf[off+7]])
		v = (v << 4) | (HEX_NUM[buf[off+8]])
		v = (v << 4) | (HEX_NUM[buf[off+9]])
		v = (v << 4) | (HEX_NUM[buf[off+10]])
		v = (v << 4) | (HEX_NUM[buf[off+11]])
		return v
	case 13:
		v = (v << 4) | (HEX_NUM[buf[off]])
		v = (v << 4) | (HEX_NUM[buf[off+1]])
		v = (v << 4) | (HEX_NUM[buf[off+2]])
		v = (v << 4) | (HEX_NUM[buf[off+3]])
		v = (v << 4) | (HEX_NUM[buf[off+4]])
		v = (v << 4) | (HEX_NUM[buf[off+5]])
		v = (v << 4) | (HEX_NUM[buf[off+6]])
		v = (v << 4) | (HEX_NUM[buf[off+7]])
		v = (v << 4) | (HEX_NUM[buf[off+8]])
		v = (v << 4) | (HEX_NUM[buf[off+9]])
		v = (v << 4) | (HEX_NUM[buf[off+10]])
		v = (v << 4) | (HEX_NUM[buf[off+11]])
		v = (v << 4) | (HEX_NUM[buf[off+12]])
		return v
	case 14:
		v = (v << 4) | (HEX_NUM[buf[off]])
		v = (v << 4) | (HEX_NUM[buf[off+1]])
		v = (v << 4) | (HEX_NUM[buf[off+2]])
		v = (v << 4) | (HEX_NUM[buf[off+3]])
		v = (v << 4) | (HEX_NUM[buf[off+4]])
		v = (v << 4) | (HEX_NUM[buf[off+5]])
		v = (v << 4) | (HEX_NUM[buf[off+6]])
		v = (v << 4) | (HEX_NUM[buf[off+7]])
		v = (v << 4) | (HEX_NUM[buf[off+8]])
		v = (v << 4) | (HEX_NUM[buf[off+9]])
		v = (v << 4) | (HEX_NUM[buf[off+10]])
		v = (v << 4) | (HEX_NUM[buf[off+11]])
		v = (v << 4) | (HEX_NUM[buf[off+12]])
		v = (v << 4) | (HEX_NUM[buf[off+13]])
		return v
	case 15:
		v = (v << 4) | (HEX_NUM[buf[off]])
		v = (v << 4) | (HEX_NUM[buf[off+1]])
		v = (v << 4) | (HEX_NUM[buf[off+2]])
		v = (v << 4) | (HEX_NUM[buf[off+3]])
		v = (v << 4) | (HEX_NUM[buf[off+4]])
		v = (v << 4) | (HEX_NUM[buf[off+5]])
		v = (v << 4) | (HEX_NUM[buf[off+6]])
		v = (v << 4) | (HEX_NUM[buf[off+7]])
		v = (v << 4) | (HEX_NUM[buf[off+8]])
		v = (v << 4) | (HEX_NUM[buf[off+9]])
		v = (v << 4) | (HEX_NUM[buf[off+10]])
		v = (v << 4) | (HEX_NUM[buf[off+11]])
		v = (v << 4) | (HEX_NUM[buf[off+12]])
		v = (v << 4) | (HEX_NUM[buf[off+13]])
		v = (v << 4) | (HEX_NUM[buf[off+14]])
		return v
	case 16:
		v = (v << 4) | (HEX_NUM[buf[off]])
		v = (v << 4) | (HEX_NUM[buf[off+1]])
		v = (v << 4) | (HEX_NUM[buf[off+2]])
		v = (v << 4) | (HEX_NUM[buf[off+3]])
		v = (v << 4) | (HEX_NUM[buf[off+4]])
		v = (v << 4) | (HEX_NUM[buf[off+5]])
		v = (v << 4) | (HEX_NUM[buf[off+6]])
		v = (v << 4) | (HEX_NUM[buf[off+7]])
		v = (v << 4) | (HEX_NUM[buf[off+8]])
		v = (v << 4) | (HEX_NUM[buf[off+9]])
		v = (v << 4) | (HEX_NUM[buf[off+10]])
		v = (v << 4) | (HEX_NUM[buf[off+11]])
		v = (v << 4) | (HEX_NUM[buf[off+12]])
		v = (v << 4) | (HEX_NUM[buf[off+13]])
		v = (v << 4) | (HEX_NUM[buf[off+14]])
		v = (v << 4) | (HEX_NUM[buf[off+15]])
		return v
	}
	log_and_exit("trace_id_parse_error")
	return 0
}

func call_finish(pw *ProcessorWindow, write_data []byte) {
	binary.LittleEndian.PutUint32(write_data[0:], update_type(4, TYPE_FINISH))
	binary.LittleEndian.PutUint32(write_data[4:], 1)
	(*pw.ch_collector).Write(write_data[0:8])
}

func connect_to_collector(pw *ProcessorWindow) {
	var conn = *connect(C_PORT)
	var buf_len = 1024 * 512
	pw.ch_collector = &conn
	var read_buf = make([]byte, buf_len)
	var write_buf = make([]byte, buf_len)
	var read_index = 0
	var write_index = 0
	binary.LittleEndian.PutUint32(write_buf, update_type(4, TYPE_SET_PORT))
	binary.LittleEndian.PutUint32(write_buf[4:], uint32(L_PORT*10+pw.index))
	conn.Write(write_buf[0:8])
	for true {
		n, err := conn.Read(read_buf[write_index:])
		if err != nil {
			log_and_exit(err)
		}
		write_index += n
		for true {
			var raw_len = binary.LittleEndian.Uint32(read_buf[read_index:])
			var len = get_len_from_type_len(raw_len)
			if write_index-read_index < len {
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
				err_chan := *pw.err_chan
				var end = len + 4
				for i := 4; i < end; i += 8 {
					err_id := binary.LittleEndian.Uint64(read_buf[read_index:])
					read_index += 8
					err_chan <- int64(err_id)
				}
			} else if _type == TYPE_ADD_WAIT {
				*(pw.wait_block_chan) <- 1
			} else if _type == TYPE_FINISH {
				handle_finish(pw, write_buf)
			} else if _type == TYPE_PROCESSOR_READY {
				processor_ready = true
				if OFFLINE {
					fmt.Println("ready...")
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

func handle_finish(pw *ProcessorWindow, write_data []byte) {
	if OFFLINE {
		fmt.Println("finish ...")
	}
	pw.fill_err_set()
	handle_finish_rm_trace_id(pw, write_data)
	binary.LittleEndian.PutUint32(write_data, update_type(4, TYPE_FINISH))
	binary.LittleEndian.PutUint32(write_data[4:], 2)
	(*pw.ch_collector).Write(write_data[0:8])
}

func handle_finish_rm_trace_id(pw *ProcessorWindow, write_data []byte) {
	var window_array = pw.window_array
	for i := 0; i < WINDOW_SIZE; i++ {
		handle_rm_trace_id(pw, write_data, window_array[i])
	}
}

func handle_rm_trace_id(pw *ProcessorWindow, write_data []byte, rm_trace_id int64) {
	var trace_map = pw.trace_map
	var rm_t_start, exists = trace_map.Del(rm_trace_id)
	if exists {
		var err_set = pw.err_set
		var rm_t_off = int(rm_t_start) * T_START_OFF
		var _, is_err = err_set.Del(rm_trace_id)
		var pos_array = pw.pos_array
		if is_err {
			var raw_buf_array = pw.raw_buf_array
			var rm_t_size = pos_array[rm_t_off]
			if DEBUG {
				if rm_t_size < 1 {
					log_and_exit("rm_t_size")
				}
			}
			binary.LittleEndian.PutUint64(write_data[4:], uint64(rm_trace_id))
			var _start = rm_t_off + 1
			var _end = _start + int(rm_t_size)
			var all_len = 12
			for i := _start; i < _end; i++ {
				var rm_pos = pos_array[i]
				var rm_buf_index = get_pos_buf_index(rm_pos)
				var rm_len = get_pos_len(rm_pos)
				var rm_r_index = get_pos_r_index(rm_pos)
				binary.LittleEndian.PutUint16(write_data[all_len:], uint16(rm_len))
				all_len += 2
				var raw_buf = *raw_buf_array[rm_buf_index]
				if OFFLINE {
					if raw_buf[rm_r_index+rm_len-1] != '\n' {
						log_and_exit("not end with n")
					}
				}
				copy(write_data[all_len:], raw_buf[rm_r_index:rm_r_index+rm_len])
				all_len += rm_len
			}
			binary.LittleEndian.PutUint32(write_data, update_type(all_len-4, TYPE_ERR_DATA))
			(*pw.ch_collector).Write(write_data[0:all_len])
		}
		pos_array[rm_t_off] = 0
		pw.release_pos(rm_t_start)
	}
}

func get_t_size_value(buf_index int, r_index int, ne int) uint64 {
	return uint64((buf_index << 48) | ((ne - r_index) << 32) | r_index)
}

func get_pos_buf_index(raw_pos uint64) int {
	return int(raw_pos >> 48)
}

func get_pos_r_index(raw_pos uint64) int {
	return int(raw_pos & 0xffffffff)
}

func get_pos_len(raw_pos uint64) int {
	return int((raw_pos >> 32) & 0xffff)
}

func inc_win_index(index int) int {
	return (index + 1) & (WINDOW_SIZE - 1)
}

func inc_buf_index(index int) int {
	index++
	if index == WINDOW_BUF_SIZE {
		return 0
	} else {
		return index
	}
}

type ProcessorWindow struct {
	send_block_count int
	download_length  int64
	header_decoded   bool
	wait_block_chan  *chan int32
	index            int
	ch_collector     *net.Conn
	ch_download      *net.Conn
	handled          int
	win_index        int
	buf_index        int
	window_array     []int64
	pos_array        []uint64
	raw_buf_array    []*[]byte
	pos_stack        *Stack
	err_list         *Stack
	err_set          *intintmap.Map
	err_chan         *chan int64
	trace_map        *intintmap.Map
}

func (w *ProcessorWindow) release_pos(start int64) {
	w.pos_stack.Push(start)
}

func (w *ProcessorWindow) fill_err_set() {
	var err_chan = *w.err_chan
	var err_set = w.err_set
	for ; ; {
		select {
		case err_id := <-err_chan:
			err_set.Put(err_id, 1)
		default:
			return
		}
	}
}

func (w *ProcessorWindow) wait_block() {
	_ = <-*(w.wait_block_chan)
}

func (w *ProcessorWindow) next_read_buf() []byte {
	return *((w.raw_buf_array)[inc_buf_index(w.buf_index)])
}

func (w *ProcessorWindow) acquire_pos() int64 {
	return (*(w.pos_stack)).Pop()
}

func (w *ProcessorWindow) get_read_buf() []byte {
	return *((w.raw_buf_array)[w.buf_index])
}

func NewProcessorWindow(index int) *ProcessorWindow {
	var pw = ProcessorWindow{}

	var window_array = make([]int64, WINDOW_SIZE)
	var pos_array = make([]uint64, POS_ARRAY_SIZE*T_START_OFF)
	var raw_buf_array = make([]*[]byte, WINDOW_BUF_SIZE)
	var pos_stack = NewStack(POS_ARRAY_SIZE)
	var err_list = NewStack(64)
	var err_set = intintmap.New(1024*16, 0.5)
	var trace_map = intintmap.New(WINDOW_SIZE, 0.5)
	var wait_block_chan = make(chan int32, WAIT_BLOCK_SIZE*4)
	var err_chan = make(chan int64, 1024)

	for i := 0; i < WAIT_BLOCK_SIZE; i++ {
		wait_block_chan <- 1
	}
	for i := POS_ARRAY_SIZE - 1; i >= 0; i-- {
		pos_stack.Push(int64(i))
	}
	for i := 0; i < WINDOW_BUF_SIZE; i++ {
		var buf = make([]byte, READ_BUF_SIZE)
		raw_buf_array[i] = &buf
	}

	pw.download_length = 0
	pw.header_decoded = false
	pw.wait_block_chan = &wait_block_chan
	pw.index = index
	pw.handled = 0
	pw.win_index = 0
	pw.buf_index = 0
	pw.window_array = window_array
	pw.pos_array = pos_array
	pw.raw_buf_array = raw_buf_array
	pw.pos_stack = pos_stack
	pw.err_list = err_list
	pw.err_set = err_set
	pw.trace_map = trace_map
	pw.err_chan = &err_chan

	return &pw
}

type Stack struct {
	i    int
	data *[]int64
}

func NewStack(cap int) *Stack {
	var stack = Stack{}
	var data = make([]int64, cap)
	stack.data = &data
	return &stack
}

func (s *Stack) Push(v int64) {
	(*(s.data))[s.i] = v
	s.i++
}

func (s *Stack) Pop() int64 {
	s.i--
	return (*(s.data))[s.i]
}

func (s *Stack) clear() {
	s.i = 0
}

func (s *Stack) size() int {
	return s.i
}

func (s *Stack) get(i int) int64 {
	return (*(s.data))[i]
}
