package my;

import com.carrotsearch.hppc.LongObjectHashMap;
import com.carrotsearch.hppc.LongObjectMap;
import com.carrotsearch.hppc.LongScatterSet;
import com.carrotsearch.hppc.procedures.LongObjectProcedure;
import com.firenio.buffer.ByteBuf;
import com.firenio.codec.http11.HttpCodec;
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

import java.net.URLDecoder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import static my.Main.*;

/**
 * @author: wangkai
 **/
public class Collector {

    static final int                  TRACE_COUNT      = 5000;
    static final int                  MAX_ERR_TRACE    = 256;
    static final int                  CORE_SIZE        = 1;
    static final MessageDigest        MD5              = initMD5();
    static final NioEventLoopGroup    EL_GROUP         = new NioEventLoopGroup(true, CORE_SIZE);
    static final LongObjectMap<Trace> TRACE_MAP        = new LongObjectHashMap<>(1024 * 16);
    static final ByteBuf              RESULT           = ByteBuf.buffer(1024 * 1024);
    static final int[]                TRACE_POS_ARRAY  = new int[MAX_ERR_TRACE * TRACE_COUNT];
    static final long[]               TRACE_TIME_ARRAY = new long[MAX_ERR_TRACE * TRACE_COUNT];
    static final byte[]               TRACE_DATA       = new byte[MAX_TRACE_LEN * MAX_ERR_TRACE * TRACE_COUNT];
    static final LongScatterSet       TIME_SET         = new LongScatterSet(MAX_ERR_TRACE * 2 * TRACE_COUNT);

    static int          client_size;
    static ByteBuf      score_header;
    static boolean      ready;
    static int          r_port;
    static int          finish_1;
    static int          finish_2;
    static Channel      ch_score;
    static Channel      ch_8000_0;
    static Channel      ch_8000_1;
    static Channel      ch_8001_0;
    static Channel      ch_8001_1;
    static NioEventLoop event_loop;

    static void init(int port) throws Exception {
        RESULT.writeBytes("result=%7B".getBytes());
        EL_GROUP.setMemoryUnit(1024 * 64);
        EL_GROUP.setMemoryCapacity(1024 * 1024 * 256);
        EL_GROUP.setEventLoopName("nio-collector");
        EL_GROUP.setIdleTime(Long.MAX_VALUE);
        EL_GROUP.start();
        event_loop = EL_GROUP.getEventLoop(0);

        String req = "POST /api/finished HTTP/1.1\r\n" +
                "Host: localhost\r\n" +
                "Content-Type: application/x-www-form-urlencoded\r\n" +
                "Content-Length: ";
        score_header = ByteBuf.buffer(req.length() + 32);
        score_header.writeBytes(req.getBytes());
    }

    static void runCollector(int port) throws Exception {
        init(port);
        ChannelAcceptor http_context = new ChannelAcceptor(EL_GROUP, HOST, port);
        http_context.addChannelEventListener(NO_DELAY_LISTENER);
        http_context.addProtocolCodec(new HttpCodec("s", 0, true));
        http_context.setPrintConfig(false);
        http_context.setIoEventHandle(new IoEventHandle() {
            @Override
            public void accept(Channel ch, Frame frame) throws Exception {
                HttpFrame f   = (HttpFrame) frame;
                String    url = f.getRequestURL();
                if ("/setParameter".equals(url)) {
                    r_port = Integer.parseInt(f.getRequestParam("port"));
                    if (DEBUG) {
                        DebugUtil.info("set port: " + r_port);
                    }
                    connect_to_score(r_port);
                }
                DebugUtil.info("is_ready: " + ready);
                if (!ready) {
                    f.setStatus(HttpStatus.C503);
                }
                ch.writeAndFlush(frame);
            }
        });
        http_context.bind();
        ChannelAcceptor context = new ChannelAcceptor(EL_GROUP, HOST, C_PORT);
        context.addChannelEventListener(NO_DELAY_LISTENER);
        context.addProtocolCodec(new ProtocolCodec() {

            @Override
            public void read_buf(Channel ch, ByteBuf src) throws Exception {
                for (; ; ) {
                    int raw_len = src.readIntLE();
                    int len     = get_len_from_type_len(raw_len);
                    if (src.readableBytes() < len) {
                        src.skipRead(-4);
                        store_plain_remain(ch, src);
                        return;
                    }
                    int type = get_type(raw_len);
                    if (type == TYPE_ERR_ID) {
                        ByteBuf buf = ch.allocate(len + 4);
                        buf.writeIntLE(len);
                        buf.writeBytes(src, len);
                        handle_write_pair(ch, buf);
                    } else if (type == TYPE_ADD_WAIT) {
                        ByteBuf buf = ch.allocate(4);
                        buf.writeIntLE(raw_len);
                        handle_write_pair(ch, buf);
                    } else if (type == TYPE_ERR_DATA) {
                        src.markWriteIndex();
                        src.writeIndex(src.readIndex() + len);
                        long  trace_id = src.readLongLE();
                        Trace trace    = getTrace(trace_id);
                        trace.write(src);
                        src.resetWriteIndex();
                    } else if (type == TYPE_FINISH) {
                        int s_type = src.readIntLE();
                        if (s_type == 1) {
                            if (++finish_1 == 4) {
                                call_processor_finish();
                            }
                        } else {
                            if (++finish_2 == 4) {
                                handle_score_finish();
                            }
                        }
                    } else if (type == TYPE_SET_PORT) {
                        int port = src.readIntLE();
                        if (port == 8000_0) {
                            ch_8000_0 = ch;
                        } else if (port == 8000_1) {
                            ch_8000_1 = ch;
                        } else if (port == 8001_0) {
                            ch_8001_0 = ch;
                        } else if (port == 8001_1) {
                            ch_8001_1 = ch;
                        }
                        if (++client_size == 4) {
                            call_processor_warm_up();
                        }
                    } else {
                        log_and_exit("error read type: " + raw_len);
                    }
                    if (!src.hasReadableBytes()) {
                        return;
                    }
                }
            }
        });
        context.setPrintConfig(false);
        context.bind();
    }

    static void connect_to_score(int port) throws Exception {
        ChannelConnector conn = new ChannelConnector(event_loop, HOST, port);
        conn.addChannelEventListener(NO_DELAY_LISTENER);
        conn.addProtocolCodec(new ProtocolCodec() {
            @Override
            public void read_buf(Channel ch, ByteBuf src) throws Exception {
                if (OFFLINE) {
                    System.out.println(new String(src.readBytes(src.writeIndex())));
                }
            }
        });
        conn.setPrintConfig(false);
        conn.connect(new Callback<Channel>() {
            @Override
            public void call(Channel res, Throwable ex) {
                if (ex != null) {
                    Util.sleep(100);
                    try {
                        connect_to_score(port);
                    } catch (Exception exception) {
                        DebugUtil.error(exception.getMessage(), exception);
                    }
                } else {
                    ch_score = res;
                }
            }
        });

    }

    static void handle_write_pair(Channel ch, ByteBuf buf) {
        if (ch == ch_8000_0) {
            ch_8001_0.writeAndFlush(buf);
        } else if (ch == ch_8000_1) {
            ch_8001_1.writeAndFlush(buf);
        } else if (ch == ch_8001_0) {
            ch_8000_0.writeAndFlush(buf);
        } else if (ch == ch_8001_1) {
            ch_8000_1.writeAndFlush(buf);
        }
    }

    static void call_processor_warm_up() {
        if (OFFLINE) {
            DebugUtil.info("call_processor_warm_up");
        }
        ByteBuf buf = event_loop.alloc().allocate(4);
        buf.writeIntLE(update_type(0, TYPE_WARM_UP));
        ch_8000_0.writeAndFlush(buf.duplicate());
        ch_8001_0.writeAndFlush(buf);
    }

    static void call_processor_finish() {
        ByteBuf buf = event_loop.alloc().allocate(4);
        buf.writeIntLE(update_type(0, TYPE_FINISH));
        ch_8000_0.writeAndFlush(buf.duplicate());
        ch_8000_1.writeAndFlush(buf.duplicate());
        ch_8001_0.writeAndFlush(buf.duplicate());
        ch_8001_1.writeAndFlush(buf);
    }

    static void handle_score_finish() {
        if (ready) {
            TRACE_MAP.forEach((LongObjectProcedure<? super Trace>) (key, value) -> {
                append_result(value);
            });
            RESULT.skipWrite(-3);
            append_right_brace();
            if (OFFLINE) {
                String result  = new String(RESULT.getBytes(0, RESULT.writeIndex()));
                String decoded = URLDecoder.decode(result);
                System.out.println(decoded);
                System.out.println();
            }
            score_header.writeBytes(String.valueOf(RESULT.writeIndex()).getBytes());
            score_header.writeByte((byte) '\r');
            score_header.writeByte((byte) '\n');
            score_header.writeByte((byte) '\r');
            score_header.writeByte((byte) '\n');
            ch_score.writeAndFlush(score_header);
            ch_score.writeAndFlush(RESULT);
        } else {
            TRACE_MAP.clear();
            TIME_SET.clear();
            RESULT.writeIndex(10);
            Arrays.fill(TRACE_POS_ARRAY, 0);
            Arrays.fill(TRACE_DATA, (byte) 0);
            Arrays.fill(TRACE_TIME_ARRAY, 0);
            finish_1 = 0;
            finish_2 = 0;
            ready = true;
            System.gc();
            if (OFFLINE) {
                DebugUtil.info("warm up ready");
            }
        }
    }

    static Trace getTrace(long trace_id) {
        Trace trace = TRACE_MAP.get(trace_id);
        if (trace == null) {
            trace = new Trace();
            TRACE_MAP.put(trace_id, trace);
        }
        return trace;
    }

    static class Trace {

        static int index = 0;

        Trace() {
            this.p = index++;
            this.pt_off = p * MAX_ERR_TRACE;
            this.data_off = p * MAX_TRACE_LEN * MAX_ERR_TRACE;
        }

        final int p;
        final int data_off;
        final int pt_off;
        int    size;
        int    data_size;
        byte[] md5;
        int    id_len = 0;

        void write(ByteBuf buf) {
            if (id_len == 0) {
                id_len = buf.indexOf(SP, buf.readIndex() + 2, buf.writeIndex()) - buf.readIndex() - 2;
            }
            int pt_off    = this.pt_off;
            int data_off  = this.data_off;
            int data_size = this.data_size;
            int i         = size;
            for (; buf.hasReadableBytes(); ) {
                int len = buf.readShortLE();
                buf.readBytes(TRACE_DATA, data_off + data_size, len);
                long time = get_trace_time(TRACE_DATA, data_off + data_size + id_len + 1, 16);
                if (TIME_SET.contains(time)) {
                    continue;
                } else {
                    TIME_SET.add(time);
                }
                TRACE_TIME_ARRAY[pt_off + i] = to_trace_time(time, i);
                TRACE_POS_ARRAY[pt_off + i] = to_trace_pos(data_size, len);
                data_size += len;
                i++;
            }
            this.size = i;
            this.data_size = data_size;
            this.md5();
            if (DEBUG) {
                if (size > MAX_ERR_TRACE) {
                    log_and_exit("err_trace_overflow");
                }
            }
        }

        void md5() {
            MD5.reset();
            int pt_off = this.pt_off;
            int pt_end = pt_off + size;
            Arrays.sort(TRACE_TIME_ARRAY, pt_off, pt_end);
            for (int i = pt_off; i < pt_end; i++) {
                long raw_time = TRACE_TIME_ARRAY[i];
                int  index    = get_trace_index(raw_time);
                int  raw_pos  = TRACE_POS_ARRAY[pt_off + index];
                int  pos      = get_trace_pos(raw_pos);
                int  len      = get_trace_len(raw_pos);
                MD5.update(TRACE_DATA, data_off + pos, len);
            }
            md5 = MD5.digest();
        }

        String getText() {
            return new String(TRACE_DATA, data_off, data_size);
        }

    }

    static MessageDigest initMD5() {
        try {
            return MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    static long get_trace_time(byte[] data, int off, int len) {
        int  end  = off + len;
        long time = 0;
        for (int i = off; i < end; i++) {
            time = time * 10 + data[i] - '0';
        }
        return time;
    }

    static int to_trace_pos(int pos, int len) {
        return (pos << 16) | len;
    }

    static int get_trace_pos(int raw_pos) {
        return raw_pos >>> 16;
    }

    static int get_trace_len(int raw_pos) {
        return raw_pos & 0xffff;
    }

    static long to_trace_time(long time, long index) {
        return (time << 7) | index;
    }

    static long get_trace_time(long raw_time) {
        return raw_time >>> 7;
    }

    static int get_trace_index(long raw_time) {
        return (int) (raw_time & (0xff >>> 1));
    }

    static void append_result(Trace trace) {
//        =  %3D
//        {  %7B
//        "  %22
//        :  %3A
//        }  %7D
//        ,  %2C

        int    data_off = trace.data_off;
        int    end      = data_off + trace.id_len;
        byte[] md5      = trace.md5;
        append_double_q();
        for (int i = data_off; i < end; i++) {
            RESULT.writeByte(TRACE_DATA[i]);
        }
        append_double_q();
        append_colon();
        append_double_q();
        for (int i = 0; i < md5.length; i++) {
            String hex = ByteUtil.getHexString(md5[i]);
            RESULT.writeByte((byte) hex.charAt(0));
            RESULT.writeByte((byte) hex.charAt(1));
        }
        append_double_q();
        append_comma();
    }

    static void append_eq() {
        RESULT.writeByte((byte) '%');
        RESULT.writeByte((byte) '3');
        RESULT.writeByte((byte) 'D');
    }

    static void append_left_brace() {
        RESULT.writeByte((byte) '%');
        RESULT.writeByte((byte) '7');
        RESULT.writeByte((byte) 'B');
    }

    static void append_double_q() {
        RESULT.writeByte((byte) '%');
        RESULT.writeByte((byte) '2');
        RESULT.writeByte((byte) '2');
    }

    static void append_colon() {
        RESULT.writeByte((byte) '%');
        RESULT.writeByte((byte) '3');
        RESULT.writeByte((byte) 'A');
    }

    static void append_right_brace() {
        RESULT.writeByte((byte) '%');
        RESULT.writeByte((byte) '7');
        RESULT.writeByte((byte) 'D');
    }

    static void append_comma() {
        RESULT.writeByte((byte) '%');
        RESULT.writeByte((byte) '2');
        RESULT.writeByte((byte) 'C');
    }

}
