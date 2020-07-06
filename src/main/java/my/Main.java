package my;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.List;

import com.firenio.Options;
import com.firenio.buffer.ByteBuf;
import com.firenio.common.Util;
import com.firenio.component.Channel;
import com.firenio.component.LoggerChannelOpenListener;
import com.firenio.component.SocketOptions;
import com.firenio.log.DebugUtil;
import com.firenio.log.LoggerFactory;

/**
 * @author: wangkai
 **/
public class Main {

    static final byte            H                 = 'h';
    static final byte            T                 = 't';
    static final byte            EQ                = '=';
    static final byte            E                 = 'e';
    static final byte            O                 = 'o';
    static final byte            C0                = '0';
    static final byte            C1                = '1';
    static final byte            C2                = '2';
    static final byte            SP                = '|';
    static final byte            N                 = '\n';
    static final byte            AND               = '&';
    static final int             TYPE_ERR_ID       = 0;
    static final int             TYPE_ERR_DATA     = 1;
    static final int             TYPE_FINISH       = 2;
    static final int             TYPE_SET_PORT     = 4;
    static final int             TYPE_WARM_UP      = 5;
    static final int             TYPE_ADD_WAIT     = 6;
    static final int             ID_SKIP           = 11;
    static final int             C_PORT            = 8003;
    static final int             MAX_TRACE_LEN     = 450;
    static final boolean         DEBUG             = false;
    static final String          HOST              = "localhost";
    static final byte[]          HEX_NUM           = new byte[127];
    static final NoDelayListener NO_DELAY_LISTENER = new NoDelayListener();
    static final boolean         ONLINE;
    static final boolean         OFFLINE;
    static final int             L_PORT;
    static final boolean         IS_PROCESSOR;

    static {
        LoggerFactory.setEnableSLF4JLogger(false);
        LoggerFactory.setLogLevel(LoggerFactory.LEVEL_DEBUG);
        Options.setBufFastIndexOf(true);
        L_PORT = Util.getIntProperty("server.port");
        ONLINE = Util.getBooleanProperty("online", true);
        OFFLINE = !ONLINE;
        IS_PROCESSOR = L_PORT != 8002;
        for (int i = '0'; i <= '9'; i++) {
            HEX_NUM[i] = (byte) (i - '0');
        }
        for (int i = 'A'; i <= 'F'; i++) {
            HEX_NUM[i] = (byte) (i - 'A' + 10);
        }
        for (int i = 'a'; i <= 'f'; i++) {
            HEX_NUM[i] = (byte) (i - 'a' + 10);
        }
        log("ONLINE: " + ONLINE);
    }

    public static void main(String[] args) {
        try {
            if (OFFLINE){
                print_jvm_args();
            }
            if (IS_PROCESSOR) {
                my.Processor.runProcessor(L_PORT);
            } else {
                my.Collector.runCollector(L_PORT);
            }
            if (OFFLINE){
                print_jvm_args();
            }
        } catch (Exception e) {
            DebugUtil.error(e.getMessage(), e);
        }
    }

    static long get_long_from_hex(ByteBuf buf, int off) {
        long v    = 0;
        v = (v << 4) | (HEX_NUM[buf.getByteAbs(off)]);
        v = (v << 4) | (HEX_NUM[buf.getByteAbs(off + 1)]);
        v = (v << 4) | (HEX_NUM[buf.getByteAbs(off + 2)]);
        v = (v << 4) | (HEX_NUM[buf.getByteAbs(off + 3)]);
        v = (v << 4) | (HEX_NUM[buf.getByteAbs(off + 4)]);
        v = (v << 4) | (HEX_NUM[buf.getByteAbs(off + 5)]);
        v = (v << 4) | (HEX_NUM[buf.getByteAbs(off + 6)]);
        v = (v << 4) | (HEX_NUM[buf.getByteAbs(off + 7)]);
        v = (v << 4) | (HEX_NUM[buf.getByteAbs(off + 8)]);
        return v;
    }

    static int update_type(int len, int type) {
        return len | (type << 24);
    }

    static int get_type(int len) {
        return (len >>> 24);
    }

    static void log_and_exit(String msg) {
        Exception e = new Exception(msg);
        DebugUtil.error(e.getMessage(), e);
        System.exit(-1);
    }

    static void log(String msg) {
        DebugUtil.info(Thread.currentThread().getName() + ": " + msg);
    }

    static void debug(String msg) {
        if (DEBUG) {
            DebugUtil.info(msg);
        }
    }

    static int get_len_from_type_len(int raw_len) {
        return raw_len & 0xffffff;
    }

    static void print_jvm_args() {
        long         mb         = 1024 * 1024;
        long         total      = Runtime.getRuntime().totalMemory() / mb;
        long         free       = Runtime.getRuntime().freeMemory() / mb;
        long         max        = Runtime.getRuntime().maxMemory() / mb;
        MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
        List<String> inputArgs  = ManagementFactory.getRuntimeMXBean().getInputArguments();
//        DebugUtil.info("Heap memory: {}", memoryBean.getHeapMemoryUsage());
//        DebugUtil.info("Method memory: {}", memoryBean.getNonHeapMemoryUsage());
        DebugUtil.info("Jvm arguments: {} ", String.valueOf(inputArgs));
        DebugUtil.info("Total: {}, free: {}, max: {}", total, free, max);
    }

    static class NoDelayListener extends LoggerChannelOpenListener {

        @Override
        public void channelOpened(Channel ch) {
            if (OFFLINE){
                super.channelOpened(ch);
            }
            try {
                ch.setOption(SocketOptions.TCP_NODELAY, 1);
                ch.setOption(SocketOptions.SO_KEEPALIVE, 0);
            } catch (IOException e) {
                DebugUtil.error(e.getMessage(), e);
            }
        }

        @Override
        public void channelClosed(Channel ch) {
            if (OFFLINE){
                super.channelClosed(ch);
            }
        }
    }

}
