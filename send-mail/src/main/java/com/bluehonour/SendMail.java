package com.bluehonour;

import freemarker.template.Configuration;
import freemarker.template.Template;
import org.apache.commons.mail.HtmlEmail;
import org.springframework.ui.freemarker.FreeMarkerTemplateUtils;

import java.io.File;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 发送邮件工具类
 * 用到的jar包（commons-email-1.5.jar、activation.jar、mail.jar、freemarker.jar、spring-context-support-3.2.4.RELEASE.jar）
 */
public class SendMail {

    private String templatePath = getClass().getClassLoader().getResource("message.ftl").getPath();
    //发送邮箱地址
    private String from = "";
    //发送人
    private String fromName = "";
    //用户名
    private String username = "";
    //密码
    private String password = "";
    //编码格式
    private String charSet = "UTF-8";

    /**
     * 构造方法
     *
     * @param from
     * @param fromName
     * @param username
     * @param password
     * @param charSet
     */
    public SendMail(String from, String fromName, String username, String password, String charSet) {
        this.from = from;
        this.fromName = fromName;
        this.username = username;
        this.password = password;
        this.charSet = charSet;
//        this.templatePath = templatePath;
    }


    private static Map<String, String> hostMap = new HashMap<String, String>();

    static {
        // 126
        hostMap.put("smtp.126", "smtp.126.com");

        // qq
        hostMap.put("smtp.qq", "smtp.qq.com");

        // 163
        hostMap.put("smtp.163", "smtp.163.com");

        // sina
        hostMap.put("smtp.sina", "smtp.sina.com.cn");

        // tom
        hostMap.put("smtp.tom", "smtp.tom.com");

        // 263
        hostMap.put("smtp.263", "smtp.263.net");

        // yahoo
        hostMap.put("smtp.yahoo", "smtp.mail.yahoo.com");

        // hotmail
        hostMap.put("smtp.hotmail", "smtp.live.com");

        // gmail
        hostMap.put("smtp.gmail", "smtp.gmail.com");
        hostMap.put("smtp.port.gmail", "465");

        hostMap.put("smtp.trht", "smtp.exmail.qq.com");
    }

    /**
     * 获取邮箱服务器
     *
     * @param email 邮箱地址
     * @return
     * @throws Exception
     */
    public static String getHost(String email) throws Exception {
        Pattern pattern = Pattern.compile("\\w+@(\\w+)(\\.\\w+){1,2}");
        Matcher matcher = pattern.matcher(email);
        String key = "unSupportEmail";
        if (matcher.find()) {
            key = "smtp." + matcher.group(1);
        }
        if (hostMap.containsKey(key)) {
            return hostMap.get(key);
        } else {
            throw new Exception("unSupportEmail");
        }
    }

    /**
     * 获取邮箱发送端口
     *
     * @param email
     * @return
     * @throws Exception
     */
    public static int getSmtpPort(String email) throws Exception {
        Pattern pattern = Pattern.compile("\\w+@(\\w+)(\\.\\w+){1,2}");
        Matcher matcher = pattern.matcher(email);
        String key = "unSupportEmail";
        if (matcher.find()) {
            key = "smtp.port." + matcher.group(1);
        }
        if (hostMap.containsKey(key)) {
            return Integer.parseInt(hostMap.get(key));
        } else {
            return 25;
        }
    }

    /**
     * 发送普通邮件
     *
     * @param toMailAddr 收信人地址
     * @param subject    email主题
     * @param message    发送email信息
     */
    public void sendCommonMail(String toMailAddr, String subject, String message) {
        HtmlEmail hemail = new HtmlEmail();
        try {
            hemail.setHostName(getHost(from));
            hemail.setSmtpPort(getSmtpPort(from));
            hemail.setCharset(charSet);
            hemail.addTo(toMailAddr);
            hemail.setFrom(from, fromName);
            hemail.setAuthentication(username, password);
            hemail.setSubject(subject);
            hemail.setMsg(message);
            hemail.send();
            System.out.println("email send true!");
        } catch (Exception e) {
            System.out.println("email send error!" + e.getMessage());
        }

    }

    /**
     * 发送模板邮件
     *
     * @param toMailAddr   收信人地址
     * @param subject      email主题
     * @param templatePath 模板地址
     * @param map          模板map
     */
    public void sendFtlMail(String toMailAddr, String subject, String templatePath, Map<String, Object> map) {
        Template template = null;
        Configuration freeMarkerConfig = null;
        HtmlEmail hemail = new HtmlEmail();
        try {
            hemail.setHostName(getHost(from));
            hemail.setSmtpPort(getSmtpPort(from));
            hemail.setCharset(charSet);
            hemail.addTo(toMailAddr);
            hemail.setFrom(from, fromName);
            hemail.setAuthentication(username, password);
            hemail.setSubject(subject);
            freeMarkerConfig = new Configuration();
            freeMarkerConfig.setDirectoryForTemplateLoading(new File(getFilePath(templatePath)));
            // 获取模板
            template = freeMarkerConfig.getTemplate(getFileName(templatePath), new Locale("Zh_cn"), "UTF-8");
            // 模板内容转换为string
            String htmlText = FreeMarkerTemplateUtils.processTemplateIntoString(template, map);
            System.out.println(htmlText);
            hemail.setMsg(htmlText);
            hemail.send();
            System.out.println("email send true!");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("email send error!");
        }
    }

    /**
     * 获取模板文件内容
     *
     * @param templatePath
     * @param map
     * @return
     */
    public static String getHtmlText(String templatePath, Map<String, Object> map) {
        Template template = null;
        String htmlText = "";
        try {
            Configuration freeMarkerConfig = null;
            freeMarkerConfig = new Configuration();
            freeMarkerConfig.setDirectoryForTemplateLoading(new File(getFilePath(templatePath)));
            // 获取模板
            template = freeMarkerConfig.getTemplate(getFileName(templatePath), new Locale("Zh_cn"), "UTF-8");
            // 模板内容转换为string
            htmlText = FreeMarkerTemplateUtils.processTemplateIntoString(template, map);
            System.out.println(htmlText);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return htmlText;
    }


    //获取模板路径
    private static String getFilePath(String path) {
        path = path.replace("\\", "/");
        System.out.println(path);
        return path.substring(0, path.lastIndexOf("/") + 1);
    }

    //获取模板名
    private static String getFileName(String path) {
        path = path.replace("\\", "/");
        System.out.println(path);
        return path.substring(path.lastIndexOf("/") + 1);
    }


    /**
     * 测试方法
     *
     * @param args
     */
    public static void main(String[] args) {
        try {
            SendMail smu = new SendMail("", "", "", "", "UTF-8");

            while (true) {
                String html = CrawlerUtil.getHTMLContent("https://hitokoto.cn/");
                String content = CrawlerUtil.getContentAuther(html);
                if (!content.equals("『彼岸花花开彼岸，断肠草草断肝肠。』—— 「MXLBS」")) {
                    System.out.println(content);
                    //测试发送普通邮件
//                smu.sendCommonMail("xxx@163.com", "一言", content);
                    Thread.sleep(1000l);
                }

            }
//            smu.sendCommonMail("", "一言", content);
            //测试通过模板发送邮件
//            Map<String, Object> map = new HashMap<String, Object>();
//            map.put("mailTitle", "测试短信模板发送");
//            map.put("mailContent", "测试短信模板发送测试短信模板发送测试短信模板发送测试短信模板发送测试短信模板发送测试短信模板发送测试短信模板发送测试短信模板发送");
//            smu.sendFtlMail(" @163.com", "sendemail test!", smu.templatePath, map);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("email send error!");
        }
    }

}
