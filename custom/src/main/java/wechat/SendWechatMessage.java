package wechat;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;


public class SendWechatMessage {
    private CloseableHttpClient httpClient;
    private HttpPost httpPost;//用于提交登陆数据
    private HttpGet httpGet;//用于获得登录后的页面
    public static final String CONTENT_TYPE = "Content-Type";
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//

    private static Gson gson = new Gson();

    private static Logger logger = LoggerFactory.getLogger(SendWechatMessage.class);


    //按时限发送,一段时间内发送一条
    public static void sendWechat(String content,String wechat_id, String wechat_corpid,String wechat_corpsecret) {
        try {
            send(content, wechat_id,wechat_corpid,wechat_corpsecret);
        } catch (Exception e) {
            logger.error(ExceptionUtils.getStackTrace(e));
        }
    }

    //发送企业微信消息
    private static void send(String content,String wechat_id, String wechat_corpid,String wechat_corpsecret) throws IOException {

        SendWechatMessage sw = new SendWechatMessage();
        String token = sw.getToken(wechat_corpid, wechat_corpsecret);
        String postdata = sw.createpostdata("@all", "text", Integer.parseInt(wechat_id), "content", content);
        String resp = sw.post("utf-8", SendWechatMessage.CONTENT_TYPE, (new UrlData()).getSendMessage_Url(), postdata, token);

    }


    /**
     * 微信授权请求，GET类型，获取授权响应，用于其他方法截取token
     *
     * @param Get_Token_Url
     * @return String 授权响应内容
     * @throws IOException
     */
    protected String toAuth(String Get_Token_Url) throws IOException {
        httpClient = HttpClients.createDefault();
        httpGet = new HttpGet(Get_Token_Url);
        CloseableHttpResponse response = httpClient.execute(httpGet);
        String resp;
        try {
            HttpEntity entity = response.getEntity();
            resp = EntityUtils.toString(entity, "utf-8");
            EntityUtils.consume(entity);
        } finally {
            response.close();
        }
        LoggerFactory.getLogger(getClass()).info(" resp:{}", resp);
        return resp;
    }

    /**
     * 获取toAuth(String Get_Token_Url)返回结果中键值对中access_token键的值
     */
    public String getToken(String corpid, String corpsecret) throws IOException {
        SendWechatMessage sw = new SendWechatMessage();
        UrlData uData = new UrlData();
        uData.setGet_Token_Url(corpid, corpsecret);
        String resp = sw.toAuth(uData.getGet_Token_Url());

        Map<String, Object> map = gson.fromJson(resp,
                new TypeToken<Map<String, Object>>() {
                }.getType());
        return map.get("access_token").toString();
    }


    /**
     * @return String
     * @Title:创建微信发送请求post数据
     */
    public String createpostdata(String touser, String msgtype,
                                 int application_id, String contentKey, String contentValue) {
        WechatData wcd = new WechatData();
        wcd.setTouser(touser);
        wcd.setAgentid(application_id);
        wcd.setMsgtype(msgtype);
        Map<Object, Object> content = new HashMap<Object, Object>();
        content.put(contentKey, contentValue + "\n--------\n" + df.format(new Date()));
        wcd.setText(content);
        return gson.toJson(wcd);
    }

    /**
     * @return String
     * @Title 创建微信发送请求post实体
     */
    public String post(String charset, String contentType, String url,
                       String data, String token) throws IOException {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        httpPost = new HttpPost(url + token);
        httpPost.setHeader(CONTENT_TYPE, contentType);
        httpPost.setEntity(new StringEntity(data, charset));
        CloseableHttpResponse response = httpclient.execute(httpPost);
        String resp;
        try {
            HttpEntity entity = response.getEntity();
            resp = EntityUtils.toString(entity, charset);
            EntityUtils.consume(entity);
        } finally {
            response.close();
        }
        LoggerFactory.getLogger(getClass()).info(
                "call [{}], param:{}, resp:{}", url, data, resp);
        return resp;
    }

}