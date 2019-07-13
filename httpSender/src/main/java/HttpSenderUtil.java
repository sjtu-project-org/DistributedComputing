import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URL;
import java.net.URLConnection;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSON;
import java.nio.file.Files;
import java.util.List;
import java.io.File;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.URIException;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.util.URIUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.httpclient.HttpException;

public class HttpSenderUtil {

    /**
     * 向指定 URL 发送POST方法的请求
     *
     * @param url
     *            发送请求的 URL
     * @param param
     *            请求参数，请求参数应该是 name1=value1&name2=value2 的形式。
     * @return 所代表远程资源的响应结果
     */
    public static String sendPost(String url, String param) {
        PrintWriter out = null;
        BufferedReader in = null;
        String result = "";
        try {
            URL realUrl = new URL(url);
            // 打开和URL之间的连接
            URLConnection conn = realUrl.openConnection();
            // 设置通用的请求属性
            conn.setRequestProperty("accept", "*/*");
            conn.setRequestProperty("connection", "Keep-Alive");
            conn.setRequestProperty("user-agent",
                    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
            // 发送POST请求必须设置如下两行
            conn.setDoOutput(true);
            conn.setDoInput(true);
            // 获取URLConnection对象对应的输出流
            out = new PrintWriter(conn.getOutputStream());
            // 发送请求参数
            out.print(param);
            // flush输出流的缓冲
            out.flush();
            // 定义BufferedReader输入流来读取URL的响应
            in = new BufferedReader(
                    new InputStreamReader(conn.getInputStream()));
            String line;
            while ((line = in.readLine()) != null) {
                result += line;
            }
        } catch (Exception e) {
            System.out.println("发送 POST 请求出现异常！" + e);
            e.printStackTrace();
        }
        // 使用finally块来关闭输出流、输入流
        finally {
            try {
                if (out != null) {
                    out.close();
                }
                if (in != null) {
                    in.close();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        return result;
    }

    public static  String httpSend(String url, Map<String, Object> propsMap)  throws Exception{
        HttpClient httpClient = new HttpClient();
        PostMethod postMethod = new PostMethod(url);// POST请求
        String returnString ="";
        // 参数设置
        Set<String> keySet = propsMap.keySet();
        NameValuePair[] postData = new NameValuePair[keySet.size()];
        int index = 0;
        for (String key : keySet) {
            postData[index++] = new NameValuePair(key, propsMap.get(key).toString());
        }
        postMethod.addParameters(postData);
        try {
            httpClient.executeMethod(postMethod);// 发送请求
            java.io.InputStream input = postMethod.getResponseBodyAsStream();
            returnString = convertStreamToString(input).toString();

        } catch (HttpException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            postMethod.releaseConnection();// 关闭连接
        }
        return returnString;
    }

    public static String convertStreamToString(java.io.InputStream input)
            throws Exception {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input,
                "UTF-8"));
        StringBuilder sb = new StringBuilder();
        String line = null;
        while ((line = reader.readLine()) != null) {
            sb.append(line + "\n");
        }
        input.close();
        return sb.toString();
    }

    private static String url = "http://202.120.40.8:30361";

    public static void main(String[] strs){
        String filename = "E:\\junior big\\分布式系统\\lab_DistributedTransactionSettlementSystem\\order.json";
        String contents = "";
        try {
            contents = new String(Files.readAllBytes(new File(filename).toPath()));
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        //Order order = new JSONObject().parseObject(contents,Order.class);
        List<Order> orderList = JSONArray.parseArray(contents.toString(),Order.class);
        for (Order od : orderList) {
            String msg = JSON.toJSONString(od);
            String param = "msg="+msg;
            String res = HttpSenderUtil.sendPost(url, param);
            System.out.println(res);
            //System.out.println(od.items);
        }
        long time=new Date().getTime();
        //String check=MD5.getMD5(time+"www.j1.com");
       /* String mobile="13053702096";
        String msg="尊敬的用户  ， 您在健一网的安全验证码为897489，健一网祝您身体健康";
        String param="t="+time+"&mobile="+mobile+"&msg="+msg;
        //String res=HttpSenderUtil.sendPost("http://localhost:8080/ec-dec/page/sms/sendSms/code",param);
        String res = HttpSenderUtil.sendPost(url, param);
        System.out.println(res);*/
    }
    /**
     * 执行get方法
     * @param url
     * @param queryString
     * @return
     */
    public static String doGet(String url, String queryString) {
        String response = null;
        HttpClient client = new HttpClient();
        HttpMethod method = new GetMethod(url);
        try {
            if (StringUtils.isNotBlank(queryString))
                method.setQueryString(URIUtil.encodeQuery(queryString));
            client.executeMethod(method);
            if (method.getStatusCode() == HttpStatus.SC_OK) {
                response = method.getResponseBodyAsString();
            }
        } catch (URIException e) {
            //logger.error("执行HTTP Get请求时，编码查询字符串“" + queryString + "”发生异常！", e); 
        } catch (IOException e) {
            //logger.error("执行HTTP Get请求" + url + "时，发生异常！", e); 
        } finally {
            method.releaseConnection();
        }
        return response;
    }

}