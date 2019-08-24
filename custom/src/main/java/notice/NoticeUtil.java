package notice;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.SystemUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wechat.SendWechatMessage;

import java.io.FileInputStream;
import java.util.*;

public class NoticeUtil {
    private static Logger logger = LoggerFactory.getLogger(NoticeUtil.class);
    private static final String    CLASSPATH_URL_PREFIX = "classpath:";

    protected static final String SEP = SystemUtils.LINE_SEPARATOR;

    private static String serverr_terminal = null;
    private static long intervalTime = 60000L;
    private static String wechat_id = null;
    private static String wechat_corpid = null;
    private static String wechat_corpsecret = null;
    private static String if_send_wechat = null;
    private static Map<Integer,Long> wechatMap = new HashMap();
    private static Properties properties = null;

    static {
        try {
            String conf = System.getProperty("canal.conf", "classpath:canal.properties");
            properties = new Properties();
            if (conf.startsWith(CLASSPATH_URL_PREFIX)) {
                conf = StringUtils.substringAfter(conf, CLASSPATH_URL_PREFIX);
                properties.load(NoticeUtil.class.getClassLoader().getResourceAsStream(conf));
            } else {
                properties.load(new FileInputStream(conf));
            }


            serverr_terminal = properties.getProperty("serverr_terminal");
            wechat_id = properties.getProperty("wechat_id");
            wechat_corpid = properties.getProperty("wechat_corpid");
            wechat_corpsecret = properties.getProperty("wechat_corpsecret");
            if_send_wechat = properties.getProperty("if_send_wechat");
        }catch (Exception e){
            logger.error(e.getMessage());
        }


        if(StringUtils.isNotEmpty(properties.getProperty("intervalTime"))){
            intervalTime = Long.parseLong(properties.getProperty("intervalTime"));
        }
//		重置wechatMap
        Timer timer = new Timer();
        TimerTask task = new TimerTask (){
            public void run() {
                wechatMap = new HashMap();
            }
        };
        timer.schedule (task, 1, intervalTime);
    }


    public static void sendNotice(String database,String tableName,String sql,String exception){
        if(StringUtils.isNotEmpty(exception)&&exception.length()>130){
            exception = exception.substring(0,125);
        }
        String content = "database: "+database + SEP
                +"tablename: "+tableName + SEP
                +"sql: "+ sql + SEP
                +"exception: "+exception + SEP;
        wechatNotice(content,exception);
    }

    public static void sendExceptionNotice(String desc,Throwable e){
        String exception = ExceptionUtils.getFullStackTrace(e);
        if(StringUtils.isNotEmpty(exception)&&exception.length()>130){
            exception = exception.substring(0,125);
        }
        String content = "desc: "+desc + SEP
                +"exception: "+exception + SEP;
        wechatNotice(content,exception);
    }

//    public static void sendNotice(String content){
//        wechatNotice(content);
//    }

    /**
     * 相同信息一段时间只发送一次
     * */
    private static void wechatNotice(String content,String hashString){
        if("true".equals(if_send_wechat)){
            long now = System.currentTimeMillis();
            Integer hashCode = (hashString).hashCode();

            if(wechatMap.containsKey(hashCode)){
                if((now-wechatMap.get(hashCode))>intervalTime) {
                    content = "terminal: "+serverr_terminal+ SEP + content;
                    SendWechatMessage.sendWechat(content,wechat_id,wechat_corpid,wechat_corpsecret);
                    if(wechatMap.size()>10000){
                        wechatMap = new HashMap();
                    }
                    wechatMap.put(hashCode,now);
                }
            }else{
                content = "terminal: "+serverr_terminal+ SEP + content;
                SendWechatMessage.sendWechat(content,wechat_id,wechat_corpid,wechat_corpsecret);
                wechatMap.put(hashCode,now);
            }
        }
    }
}
