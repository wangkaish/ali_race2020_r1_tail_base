package my;

import com.carrotsearch.hppc.IntStack;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongIntHashMap;
import com.carrotsearch.hppc.LongIntMap;
import com.carrotsearch.hppc.LongIntScatterMap;
import com.carrotsearch.hppc.LongScatterSet;
import com.firenio.buffer.ByteBuf;
import com.firenio.codec.http11.HttpCodec;
import com.firenio.codec.http11.HttpConnection;
import com.firenio.codec.http11.HttpFrame;
import com.firenio.codec.http11.HttpStatus;
import com.firenio.common.ByteUtil;
import com.firenio.common.Util;
import com.firenio.component.Channel;
import com.firenio.component.ChannelAcceptor;
import com.firenio.component.ChannelConnector;
import com.firenio.component.Frame;
import com.firenio.component.IoEventHandle;
import com.firenio.component.NioEventLoop;
import com.firenio.component.NioEventLoopGroup;
import com.firenio.component.ProtocolCodec;
import com.firenio.concurrent.Callback;
import com.firenio.log.DebugUtil;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import static my.Main.*;

/**
 * @author: wangkai
 **/
public class Processor {

    static final byte[]            CONTENT_LENGTH_MATCH = "Content-Length:".getBytes();
    static final boolean           FAST_CHECK           = true;
    static final int               CORE_SIZE            = 2;
    static final int               T_START_OFF          = 64 + 64;
    static final int               WINDOW_SIZE          = 1024 * 256;
    static final int               READ_BUF_UNIT_SIZE   = 1024 * 1024;
    static final int               SEND_WAIT_BLOCK_RATE = 1024 * 4;
    static final int               WAIT_BLOCK_SIZE      = WINDOW_SIZE / SEND_WAIT_BLOCK_RATE - 8;
    static final int               POS_ARRAY_SIZE       = WINDOW_SIZE / 8;
    static final int               WINDOW_BUF_SIZE      = (WINDOW_SIZE * MAX_TRACE_LEN / READ_BUF_UNIT_SIZE);
    static final NioEventLoopGroup EL_GROUP             = new NioEventLoopGroup(true, CORE_SIZE);
    static final int               KMP_HTTP_CODE_LEN    = "http.status_code=".length();
    static final ProcessorWindow[] PW_ARRAY             = new ProcessorWindow[2];

    static AtomicInteger clean_count = new AtomicInteger();
    static int           r_port      = 18080;
    static ByteBuf       req_get_length_buf;
    static String        file_path;

    static void init(int port) throws Exception {
        if (port == 8000) {
            file_path = "/trace1.data";
        } else {
            file_path = "/trace2.data";
        }
        if (OFFLINE) {
            debug("download: " + file_path);
        }
        req_get_length_buf = get_get_length_req_buf();
        EL_GROUP.setIdleTime(Long.MAX_VALUE);
        EL_GROUP.setEnableMemoryPool(true);
        EL_GROUP.setEventLoopName("e-nio-processor");

        EL_GROUP.setMemoryUnit(1024 * 32);
        EL_GROUP.setMemoryCapacity(1024 * 1024 * 64);

        Util.start(EL_GROUP);
        PW_ARRAY[0] = new ProcessorWindow(EL_GROUP.getEventLoop(0));
        PW_ARRAY[1] = new ProcessorWindow(EL_GROUP.getEventLoop(1));

        connect_to_collector(PW_ARRAY[0]);
        connect_to_collector(PW_ARRAY[1]);

    }

    static void runProcessor(int port) throws Exception {
        init(port);

        ChannelAcceptor context = new ChannelAcceptor(EL_GROUP, HOST, port);
        context.addChannelEventListener(NO_DELAY_LISTENER);
        context.addProtocolCodec(new HttpCodec("s", 0, true));
        context.setPrintConfig(false);
        context.setIoEventHandle(new IoEventHandle() {

            @Override
            public void accept(Channel ch, Frame frame) throws Exception {
                HttpFrame f   = (HttpFrame) frame;
                String    url = f.getRequestURL();
                if ("/setParameter".equals(url)) {
                    if (ONLINE) {
                        r_port = Integer.parseInt(f.getRequestParam("port"));
                        if (DEBUG) {
                            DebugUtil.info("set port: " + r_port);
                        }
                    }
                    connect_to_download(PW_ARRAY[0]);
                    connect_to_download(PW_ARRAY[1]);
                    connect_to_get_length(ch.getEventLoop());
                }
                if (OFFLINE) {
                    DebugUtil.info("is_ready: " + (clean_count.get() >= 2));
                }
                if (clean_count.get() < 2) {
                    f.setStatus(HttpStatus.C503);
                } else {
                    f.setConnection(HttpConnection.CLOSE);
                }
                ch.writeAndFlush(frame);
            }
        });
        context.bind();
    }

    static void connect_to_download(ProcessorWindow pw) throws Exception {
        ChannelConnector conn = new ChannelConnector(pw.el, HOST, r_port);
        conn.addChannelEventListener(NO_DELAY_LISTENER);
        conn.setPrintConfig(false);
        conn.addProtocolCodec(new ProtocolCodec() {

            @Override
            protected void read_plain(Channel ch) throws Exception {
                for (; ; ) {
                    ByteBuf read_buf = pw.get_read_buf();
                    read_data_no_store(ch, read_buf);
                    if (pw.header_decoded) {
                        if (pw.download_length + read_buf.writeIndex() == pw.content_length) {
                            pw.download_length = pw.content_length;
                            if (pw.index == 0) {
                                int n_pos = read_buf.lastIndexOf(N);
                                read_buf.writeIndex(n_pos + 1);
                            }
                            handle_buf(pw, read_buf);
                            call_finish(pw);
                            if (OFFLINE) {
                                log("read buf complete");
                            }
                            ch.close();
                            break;
                        } else {
                            if (read_buf.hasWritableBytes()) {
                                break;
                            }
                            ByteBuf new_read_buf = pw.next_read_buf();
                            int     n_pos        = read_buf.lastIndexOf(N) + 1;
                            int     len          = read_buf.writeIndex() - n_pos;
                            long    address      = read_buf.address() + n_pos;
                            new_read_buf.writeBytes(address, len);
                            read_buf.writeIndex(n_pos);
                            pw.set_read_buf(new_read_buf);
                            pw.download_length += read_buf.writeIndex();
                            handle_buf(pw, read_buf);
                        }
                    } else {
                        if (read_buf.writeIndex() < 1000) {
                            break;
                        }
                        int n_pos      = 0;
                        int last_n_pos = 0;
                        int end        = read_buf.writeIndex();
                        for (; ; ) {
                            n_pos = read_buf.indexOf(N, last_n_pos, end);
                            if (n_pos - last_n_pos < 2) {
                                break;
                            }
                            last_n_pos = n_pos + 1;
                        }
                        if (pw.index == 1) {
                            int data_n_pos = read_buf.indexOf(N, n_pos + 1, end);
                            read_buf.readIndex(data_n_pos + 1);
                        } else {
                            read_buf.readIndex(n_pos + 1);
                        }
                        pw.download_length = -(n_pos + 1);
                        pw.header_decoded = true;
                        pw.set_read_buf(read_buf);
                        if (OFFLINE) {
                            log("header-decoded");
                        }
                    }
                }
            }
        });
        conn.connect((res, ex) -> {
            if (ex != null) {
                DebugUtil.error("connect_to_download_error", ex);
                log_and_exit("connect_to_download_error");
            } else {
                pw.ch_download = res;
            }
        });
    }

    static void connect_to_get_length(NioEventLoop el) throws Exception {
        ChannelConnector conn = new ChannelConnector(el, HOST, r_port);
        conn.addChannelEventListener(NO_DELAY_LISTENER);
        conn.setPrintConfig(false);
        conn.addProtocolCodec(new ProtocolCodec() {
            @Override
            protected void read_buf(Channel ch, ByteBuf src) throws Exception {
                long length     = decode_content_length(src);
                int  redundancy = 1024 * 1024 * 16;
                long half       = length / 2;
                DebugUtil.info("content length: {}", length);
                write_download_req(PW_ARRAY[0], 0, half + redundancy);
                write_download_req(PW_ARRAY[1], half - redundancy, length - 1);
            }
        });

        conn.connect((res, ex) -> {
            if (ex != null) {
                Util.sleep(500);
                try {
                    connect_to_get_length(el);
                } catch (Exception e) {
                    DebugUtil.error(e.getMessage(), e);
                }
            } else {
                if (ONLINE) {
                    if (L_PORT == 8000) {
                        Util.sleep(100);
                    }
                }
                res.writeAndFlush(req_get_length_buf);
            }
        });
    }

    static void write_download_req(ProcessorWindow pw, long start, long end) {
        pw.content_length = end - start + 1;
        ByteBuf buf = get_download_req_buf(start, end);
        pw.ch_download.writeAndFlush(buf);
    }

    static void connect_to_collector(ProcessorWindow pw) throws Exception {
        ChannelConnector conn = new ChannelConnector(pw.el, HOST, C_PORT);
        conn.addChannelEventListener(NO_DELAY_LISTENER);
        conn.addProtocolCodec(new ProtocolCodec() {
            @Override
            protected void read_buf(Channel ch, ByteBuf src) throws Exception {
                for (; ; ) {
                    if (src.readableBytes() < 4) {
                        break;
                    }
                    int raw_len = src.readIntLE();
                    int len     = get_len_from_type_len(raw_len);
                    if (src.readableBytes() < len) {
                        src.skipRead(-4);
                        break;
                    }
                    int type = get_type(raw_len);
                    if (type == TYPE_ERR_ID) {
                        handle_err_buf(pw, src, len);
                    } else if (type == TYPE_ADD_WAIT) {
                        pw.wait_block++;
                    } else if (type == TYPE_FINISH) {
                        handle_finish(pw);
                    } else if (type == TYPE_WARM_UP) {
                        try (FileChannel fileChannel = openFileChannel()) {
                            long size       = fileChannel.size();
                            int  redundancy = 20000 * MAX_TRACE_LEN;
                            long half       = size / 2;
                            dispatch_warm_up(PW_ARRAY[0], 0, half + redundancy);
                            dispatch_warm_up(PW_ARRAY[1], half - redundancy, size - 1);
                        } catch (Throwable t) {
                            DebugUtil.error(t.getMessage(), t);
                            log_and_exit(t.getMessage());
                        }
                    } else {
                        log_and_exit("unknow type: " + raw_len);
                    }
                }
                store_plain_remain(ch, src);
            }
        });
        conn.setPrintConfig(false);
        conn.connect(new Callback<Channel>() {
            @Override
            public void call(Channel res, Throwable ex) {
                if (ex != null) {
                    Util.sleep(500);
                    try {
                        connect_to_collector(pw);
                    } catch (Exception exception) {
                        DebugUtil.error(exception.getMessage(), exception);
                    }
                } else {
                    pw.ch_collector = res;
                    ByteBuf buf = allocate(res.getEventLoop(), 8);
                    buf.writeIntLE(update_type(4, TYPE_SET_PORT));
                    buf.writeIntLE(L_PORT * 10 + pw.index);
                    res.writeAndFlush(buf);
                }
            }
        });
    }

    static void dispatch_warm_up(ProcessorWindow pw, long start, long end) {
        pw.el.submit(() -> {
            long want_read    = end - start + 1;
            long already_read = 0;
            try (FileChannel fileChannel = openFileChannel()) {
                if (OFFLINE) {
                    DebugUtil.info("warm up...");
                }
                fileChannel.position(start);
                if (pw.index == 1) {
                    ByteBuf read_buf = pw.get_read_buf();
                    int     read     = read_full(fileChannel, read_buf);
                    int     n_pos    = read_buf.indexOf(N, 0, read_buf.writeIndex());
                    read_buf.readIndex(n_pos + 1);
                    read_buf.writeIndex(read_buf.writeIndex() - 10);
                    fileChannel.position(fileChannel.position() - 10);
                    already_read = read;
                    pw.set_read_buf(read_buf);
                }
                for (; ; ) {
                    ByteBuf read_buf = pw.get_read_buf();
                    int     read     = read_full(fileChannel, read_buf);
                    boolean has_remain_data;
                    if (already_read + read_buf.writeIndex() < want_read) {
                        has_remain_data = true;
                        already_read += read;
                    } else {
                        has_remain_data = false;
                        if (pw.index == 0) {
                            read_buf.writeIndex((int) (want_read - already_read));
                            int n_pos = read_buf.lastIndexOf(N);
                            read_buf.writeIndex(n_pos + 1);
                        }
                    }
                    if (has_remain_data) {
                        ByteBuf new_read_buf = pw.next_read_buf();
                        int     n_pos        = read_buf.lastIndexOf(N) + 1;
                        int     len          = read_buf.writeIndex() - n_pos;
                        long    address      = read_buf.address() + n_pos;
                        new_read_buf.writeBytes(address, len);
                        read_buf.writeIndex(n_pos);
                        pw.set_read_buf(new_read_buf);
                        handle_buf(pw, read_buf);
                    } else {
                        handle_buf(pw, read_buf);
                        call_finish(pw);
                        if (OFFLINE) {
                            log("read buf complete");
                        }
                        break;
                    }
                }
            } catch (Throwable e) {
                DebugUtil.error(e.getMessage(), e);
            }
        });
    }

    static int read_full(FileChannel channel, ByteBuf src) throws IOException {
        int ret = 0;
        for (; ; ) {
            int read = channel.read(src.nioWriteBuffer());
            if (read < 1) {
                return ret;
            } else {
                ret += read;
                src.skipWrite(read);
                if (!src.hasWritableBytes()) {
                    return ret;
                }
            }
        }
    }

    static FileChannel openFileChannel() throws IOException {
        String file_path;
        if (ONLINE) {
            if (L_PORT == 8000) {
                file_path = "/home/data/trace1.data";
            } else {
                file_path = "/home/data/trace2.data";
            }
        } else {
            if (L_PORT == 8000) {
                file_path = "C:/programs/nginx-1.18.0/html/trace111.data";
                if (!new File(file_path).exists()) {
                    file_path = "D:/programs/nginx-1.18.0/html/trace111.data";
                }
            } else {
                file_path = "C:/programs/nginx-1.18.0/html/trace222.data";
                if (!new File(file_path).exists()) {
                    file_path = "D:/programs/nginx-1.18.0/html/trace222.data";
                }
            }
        }
        return FileChannel.open(Paths.get(file_path), StandardOpenOption.READ);
    }

    static void call_finish(ProcessorWindow pw) {
        NioEventLoop el = pw.el;
        el.submit(() -> {
            ByteBuf buf = allocate(el, 8);
            buf.writeIntLE(update_type(4, TYPE_FINISH));
            buf.writeIntLE(1);
            pw.ch_collector.write(buf);
            pw.ch_collector.run();
        });
    }

    static void handle_finish(ProcessorWindow pw) throws Exception {
        if (OFFLINE) {
            log("finish ...");
        }
        pw.ch_collector.read();
        handle_finish_rm_trace_id(pw);
        int clean_cnt = clean_count.getAndIncrement();
        if (clean_cnt < 2) {
            pw.cleanup();
        }
        if (OFFLINE) {
            log("wait_block_size: " + pw.wait_block);
        }
        ByteBuf buf = allocate(pw.el, 8);
        buf.writeIntLE(update_type(4, TYPE_FINISH));
        buf.writeIntLE(2);
        pw.ch_collector.write(buf);
        pw.ch_collector.run();
        if (DEBUG) {
            Info.print();
        }
    }

    static void handle_finish_rm_trace_id(ProcessorWindow pw) {
        NioEventLoop el            = pw.el;
        LongHashSet  err_set       = pw.err_set;
        LongIntMap   trace_map     = pw.trace_map;
        Channel      ch_collector  = pw.ch_collector;
        ByteBuf[]    raw_buf_array = pw.raw_buf_array;
        long[]       window_array  = pw.window_array;
        ByteBuf      pos_array     = pw.pos_array;
        for (int i = 0; i < WINDOW_SIZE; i++) {
            rm_trace_id(pw, ch_collector, el, err_set, trace_map, raw_buf_array, pos_array, window_array[i]);
        }
    }

    static void debug_sp(ByteBuf buf, int r_index) {
        if (DEBUG) {
            for (int i = ID_SKIP; i < 17; i++) {
                if (buf.getByteAbs(r_index + i) == SP) {
                    return;
                }
            }
            log(new String(buf.getBytes(Math.max(0, r_index - 20), 100)));
            log_and_exit("SP error");
        }
    }

    static void rm_trace_id(ProcessorWindow pw, Channel ch, NioEventLoop el, LongHashSet err_set, LongIntMap trace_map, ByteBuf[] raw_buf_array, ByteBuf pos_array, long rm_trace_id) {
        int rm_t_start = trace_map.remove(rm_trace_id) - 1;
        if (rm_t_start > -1) {
            int rm_t_off = rm_t_start * T_START_OFF;
            if (err_set.remove(rm_trace_id)) {
                int rm_t_size = (int) pos_array.getLongLE(rm_t_off << 3);
                if (DEBUG) {
                    if (rm_t_size < 1) {
                        log_and_exit("rm_t_size");
                    }
                }
                int     all_len = (MAX_TRACE_LEN * rm_t_size) * 4;
                ByteBuf err_buf = el.alloc().allocate(all_len);
                err_buf.skipWrite(4);
                err_buf.writeLongLE(rm_trace_id);
                int _start = rm_t_off + 1;
                int _end   = _start + rm_t_size;
                for (int i = _start; i < _end; i++) {
                    long rm_pos       = pos_array.getLongLE(i << 3);
                    int  rm_buf_index = get_pos_buf_index(rm_pos);
                    int  rm_len       = get_pos_len(rm_pos);
                    int  rm_r_index   = get_pos_r_index(rm_pos);
                    err_buf.writeShortLE(rm_len);
                    ByteBuf raw_buf = raw_buf_array[rm_buf_index];
                    if (DEBUG) {
                        if (err_buf.writableBytes() < rm_len) {
                            log_and_exit("write_overflow");
                        }
                    }
                    if (OFFLINE) {
                        if (raw_buf.getByteAbs(rm_r_index + rm_len - 1) != '\n') {
                            log_and_exit("not end with n");
                        }
                    }
                    err_buf.writeBytes(raw_buf.address() + rm_r_index, rm_len);
                }
                err_buf.setIntLE(0, update_type(err_buf.writeIndex() - 4, TYPE_ERR_DATA));
                ch.write(err_buf);
                ch.run();
            }
            pos_array.setLongLE(rm_t_off << 3, 0);
            pw.release_pos(rm_t_start);
        }
    }

    static void handle_buf(ProcessorWindow pw, ByteBuf buf) throws Exception {
//        long          startTime        = System.nanoTime();
        NioEventLoop  el               = pw.el;
        LongHashSet   err_set          = pw.err_set;
        LongArrayList err_list         = pw.err_list;
        LongIntMap    trace_map        = pw.trace_map;
        Channel       ch_collector     = pw.ch_collector;
        ByteBuf[]     raw_buf_array    = pw.raw_buf_array;
        long[]        window_array     = pw.window_array;
        ByteBuf       pos_array        = pw.pos_array;
        int           win_index        = pw.win_index;
        int           buf_index        = pw.buf_index;
        int           r_index          = buf.readIndex();
        int           w_index          = buf.writeIndex();
        int           send_block_count = pw.send_block_count;
        err_list.clear();
        ch_collector.read();
        for (; ; ) {
            if (DEBUG) {
                Info.line_count.incrementAndGet();
            }
            debug_sp(buf, r_index);
            long rm_trace_id = window_array[win_index];
            if (rm_trace_id != 0) {
                rm_trace_id(pw, ch_collector, el, err_set, trace_map, raw_buf_array, pos_array, rm_trace_id);
            }

            long trace_id = get_long_from_hex(buf, r_index);
            int  r_cursor = buf.indexOf(AND, r_index + 65, w_index);
            int  ne       = buf.indexOf(N, r_cursor, w_index) + 1;

            if (DEBUG) {
                if (buf.getByteAbs(ne - 1) != N) {
                    log_and_exit("find_n_error");
                }
                if (ne == 0) {
                    log_and_exit("find_n_error");
                }
            }

            int t_start = trace_map.getOrDefault(trace_id, 0) - 1;
            if (t_start == -1) {
                t_start = pw.acquire_pos();
                trace_map.put(trace_id, t_start + 1);
            }
            int t_off  = t_start * T_START_OFF;
            int t_size = (int) pos_array.getLongLE(t_off << 3);
            pos_array.setLongLE(t_off << 3, t_size + 1);
            if (DEBUG) {
                if (t_size + 2 > T_START_OFF) {
                    log_and_exit("t_start_off");
                }
            }

            long t_size_value = get_t_size_value(buf_index, r_index, ne);
            pos_array.setLongLE((t_off + t_size + 1) << 3, t_size_value);

            if (!err_set.contains(trace_id)) {
                if (check_error(buf, r_cursor, ne)) {
                    if (DEBUG) {
                        Info.trace_count.incrementAndGet();
                    }
                    err_list.add(trace_id);
                    err_set.add(trace_id);
                }
            }

            if (DEBUG) {
                Info.set_max_trace_len(ne - r_index);
                Info.set_max_trace_size(t_size);
            }

            window_array[win_index] = trace_id;
            send_block_count++;
            if (send_block_count == SEND_WAIT_BLOCK_RATE) {
                send_block_count = 0;
                ByteBuf write_buf = allocate(el, 4);
                write_buf.writeIntLE(update_type(0, TYPE_ADD_WAIT));
                ch_collector.write(write_buf);
                ch_collector.run();
                pw.wait_block();
            }
            if (ne == w_index) {
                break;
            }
            win_index = inc_win_index(win_index);
            r_index = ne;
        }
        int error_size = err_list.size();
        if (error_size > 0) {
            int     alloc_len = error_size * 8 + 4;
            ByteBuf error_buf = allocate(el, alloc_len);
            error_buf.writeIntLE(update_type(error_size * 8, TYPE_ERR_ID));
            for (int i = 0; i < err_list.size(); i++) {
                long err_id = err_list.get(i);
                error_buf.writeLongLE(err_id);
            }
            ch_collector.write(error_buf);
            ch_collector.run();
        }
        pw.win_index = inc_win_index(win_index);
        pw.buf_index = inc_buf_index(buf_index);
        pw.send_block_count = send_block_count;
        pw.handled++;

//        long cost = (System.nanoTime() - startTime) / 1000;
        if (ONLINE) {
//            if (DEBUG) {
//                if (pw.handled < 30 || pw.handled % 50 == 0) {
//                    DebugUtil.info("handled buf: el: {}, buf: {}, cost: {} ", pw_index, pw.handled, cost);
//                }
//            } else {
//                if (pw.handled < 30 || pw.handled % 50 == 0 || cost > 3000) {
//                    DebugUtil.info("handled buf: el: {}, buf: {}, cost: {} ", pw_index, pw.handled, cost);
//                }
//            }
        } else {
//            DebugUtil.info("handled buf: el: {}, buf: {}, cost: {} ", pw.index, pw.handled, cost);
//            DebugUtil.info("handled buf: el: {}, buf: {}", pw_index, pw.handled);
        }
    }

    static void handle_err_buf(ProcessorWindow pw, ByteBuf buf, int len) {
        if (DEBUG) {
            if (buf.isReleased()) {
                log_and_exit("released");
            }
        }
        LongHashSet err_set = pw.err_set;
        int         end     = len + 4;
        for (int i = 4; i < end; i += 8) {
            long trace_id = buf.readLongLE();
            err_set.add(trace_id);
        }
    }

    static boolean check_error(ByteBuf buf, int ns, int ne) {
        if (FAST_CHECK) {
            return check_error_fast2(buf, ns, ne);
        } else {
//            return check_error_kmp(buf, ns, ne);
            return false;
        }
    }

    static long decode_content_length(ByteBuf src) {
        long content_length = 0;
        int  w_idx          = src.writeIndex();
        int  ps             = src.indexOf(N, 0, w_idx) + 1;
        for (; ; ) {
            int pe = src.indexOf(N, ps, w_idx);
            if (pe - ps == 1) {
                src.readIndex(pe + 1);
                break;
            }
            if (start_with(src, ps, CONTENT_LENGTH_MATCH)) {
                int  cp  = ps + CONTENT_LENGTH_MATCH.length;
                int  cps = ByteUtil.skip(src, cp, pe, (byte) ' ');
                long ctl = 0;
                int  pee = pe - 1;
                for (int i = cps; i < pee; i++) {
                    ctl = ctl * 10 + (src.getByteAbs(i) - '0');
                }
                content_length = ctl;
            }
            ps = pe + 1;
        }
        return content_length;
    }

    static long get_t_size_value(long buf_index, long r_index, long ne) {
        return (buf_index << 48) | ((ne - r_index) << 32) | r_index;
    }

    static int get_pos_buf_index(long raw_pos) {
        return (int) (raw_pos >>> 48);
    }

    static int get_pos_r_index(long raw_pos) {
        return (int) raw_pos;
    }

    static int get_pos_len(long raw_pos) {
        return (int) ((raw_pos >>> 32) & 0xffffL);
    }

    static int inc_win_index(int index) {
//        if (++index == WINDOW_SIZE) {
//            return 0;
//        } else {
//            return index;
//        }
        return (index + 1) & (WINDOW_SIZE - 1);
    }

    static int inc_buf_index(int index) {
        if (++index == WINDOW_BUF_SIZE) {
            return 0;
        } else {
            return index;
        }
    }

    static boolean check_error_fast2(ByteBuf buf, int off, int end) {
        if (buf.getByteAbs(off - 20) == H && (buf.getByteAbs(off - 4) == EQ)) {
            return (buf.getByteAbs(off - 3) != C2);
        }
        if ((buf.getByteAbs(off - 7) == E)
                && (buf.getByteAbs(off - 2) == EQ)
                && (buf.getByteAbs(off - 1) == C1)) {
            return true;
        }
        int end1 = end - 20;
        int i    = off + 1;
        for (; i < end1; ) {
            byte c = buf.getByteAbs(i);
            if (c == H) {
                if ((buf.getByteAbs(i + 16) == EQ)) {
                    return (buf.getByteAbs(i + 17) != C2);
                } else {
                    i = buf.indexOf(AND, i + 1, end) + 1;
                    if (i == 0) {
                        return false;
                    }
                }
            } else if (c == E) {
                if ((buf.getByteAbs(i + 5) == EQ)) {
                    if (buf.getByteAbs(i + 6) == C1) {
                        return true;
                    } else {
                        i += 8;
                    }
                } else {
                    i = buf.indexOf(AND, i + 1, end) + 1;
                    if (i == 0) {
                        return false;
                    }
                }
            } else if (c == AND) {
                i++;
            } else {
                i = buf.indexOf(AND, i + 1, end) + 1;
                if (i == 0) {
                    return false;
                }
            }
        }
        for (; i < end; ) {
            byte c = buf.getByteAbs(i);
            if (c == E) {
                if ((i + 6 < end) && (buf.getByteAbs(i + 5) == EQ)) {
                    if (buf.getByteAbs(i + 6) == C1) {
                        return true;
                    } else {
                        i += 8;
                    }
                } else {
                    i = buf.indexOf(AND, i + 1, end);
                    if (i == -1) {
                        return false;
                    }
                }
            } else if (c == AND) {
                i++;
            } else {
                i = buf.indexOf(AND, i + 1, end);
                if (i == -1) {
                    return false;
                }
            }
        }
        return false;
    }

    static ByteBuf get_get_length_req_buf() {
        String req = "HEAD " + file_path + " HTTP/1.1\r\n" +
                "Host: localhost\r\n" +
                "\r\n";
        return ByteBuf.wrapAuto(req.getBytes());
    }

    static ByteBuf get_download_req_buf(long start, long end) {
        String req = "GET " + file_path + " HTTP/1.1\r\n" +
                "Host: localhost\r\n" +
                "Range: bytes=" + start + "-" + end + "\r\n" +
                "\r\n";
        return ByteBuf.wrapAuto(req.getBytes());
    }

    static ByteBuf allocate(NioEventLoop el, int size) {
        ByteBuf buf = el.alloc().allocate(size);
        if (DEBUG) {
            if (!buf.isPooled()) {
                log("allocate unpooled byte buf: " + size);
            }
        }
        return buf;
    }

    static boolean start_with(ByteBuf src, int ps, byte[] match) {
        for (int i = 0; i < match.length; i++) {
            if (src.getByteAbs(ps + i) != match[i]) {
                return false;
            }
        }
        return true;
    }

    static class ProcessorWindow {


        ProcessorWindow(NioEventLoop el) {
            this.el = el;
            this.index = el.getIndex();
            init();
        }

        int            send_block_count;
        long           content_length;
        long           download_length = 0;
        boolean        header_decoded  = false;
        int            wait_block      = WAIT_BLOCK_SIZE;
        ByteBuf        read_buf;
        NioEventLoop   el;
        int            index;
        Channel        ch_collector;
        Channel        ch_download;
        int            handled         = 0;
        int            win_index       = 0;
        int            buf_index       = 0;
        long[]         window_array    = new long[WINDOW_SIZE];
        ByteBuf        pos_array       = ByteBuf.buffer(POS_ARRAY_SIZE * T_START_OFF * 8);
        ByteBuf[]      raw_buf_array   = new ByteBuf[WINDOW_BUF_SIZE];
        IntStack       pos_stack       = new IntStack(POS_ARRAY_SIZE);
        LongArrayList  err_list        = new LongArrayList(32);
        LongHashSet    err_set         = new LongScatterSet(1024 * 16);
        LongIntHashMap trace_map       = new LongIntScatterMap(WINDOW_SIZE);

        void cleanup() {
            this.handled = 0;
            this.win_index = 0;
            this.buf_index = 0;
            Arrays.fill(window_array, 0);
            for (int i = 0; i < pos_array.capacity(); i++) {
                pos_array.setByte(i, (byte) 0);
            }
            if (pos_stack.size() != POS_ARRAY_SIZE) {
                log_and_exit("pos_stack_not_clean");
            }
            this.err_list.clear();
            this.err_set.clear();
            this.trace_map.clear();
            for (int i = 0; i < WINDOW_BUF_SIZE; i++) {
                raw_buf_array[i].clear();
            }
            read_buf = raw_buf_array[0];
        }

        void init() {
            for (int i = POS_ARRAY_SIZE - 1; i >= 0; i--) {
                pos_stack.push(i);
            }
            for (int i = 0; i < WINDOW_BUF_SIZE; i++) {
                raw_buf_array[i] = ByteBuf.buffer(READ_BUF_UNIT_SIZE);
            }
            read_buf = raw_buf_array[0];
        }

        int acquire_pos() {
            if (DEBUG) {
                if (pos_stack.isEmpty()) {
                    log_and_exit("empty_pos_stack");
                }
            }
            return pos_stack.pop();
        }

        void release_pos(int pos) {
            if (DEBUG) {
                if (pos_stack.size() == POS_ARRAY_SIZE) {
                    log_and_exit("full_pos_stack");
                }
            }
            pos_stack.push(pos);
        }

        void wait_block() throws Exception {
            if (wait_block == 0) {
                for (; ; ) {
                    ch_collector.read();
                    if (wait_block > 0) {
                        break;
                    }
                    Thread.yield();
                }
            }
            wait_block--;
        }

        ByteBuf get_read_buf() {
            return read_buf;
        }

        ByteBuf next_read_buf() {
            return raw_buf_array[inc_buf_index(buf_index)].clear();
        }

        void set_read_buf(ByteBuf read_buf) {
            this.read_buf = read_buf;
        }
    }

    static class Info {

        static int           max_trace_len  = 0;
        static int           max_trace_size = 0;
        static int           id_skip        = 16;
        static AtomicInteger line_count     = new AtomicInteger();
        static AtomicInteger trace_count    = new AtomicInteger();

        static synchronized void set_max_trace_len(int value) {
            if (value > max_trace_len) {
                max_trace_len = value;
            }
        }

        static synchronized void set_max_trace_size(int value) {
            if (value > max_trace_size) {
                max_trace_size = value;
            }
        }

        static synchronized void set_id_skip(int value) {
            if (value < id_skip) {
                id_skip = value;
            }
        }

        static void print() {
            DebugUtil.info("max_trace_len: {}, max_trace_size: {}, line: {}, trace: {}, id_skip: {}", max_trace_len, max_trace_size, line_count.get(), trace_count.get(), id_skip);
        }

    }

}
