/**
 * Created by 金伟华 on 2018/1/16.
 */
import java.io.UnsupportedEncodingException;
import java.util.Properties;
import javax.mail.*;
import javax.mail.internet.MimeMessage;
import javax.annotation.*;
import javax.mail.internet.InternetAddress;

public class MyJavaMail {
    public static String from = "jinweihuaailuoli@163.com";
    static String to = "1085425378@qq.com";
    static String host = "smtp.163.com";
    static String passwd = "wo8751198bei";
    static String content = "hello javax.mail";
    public static void main(String[] args) throws Exception{
        Properties props = System.getProperties();
        props.setProperty("mail.smtp.host",host);
        props.setProperty("mail.smtp.auth","true");
        props.setProperty("mail.transport.protocol","smtp");
        //开启会话
        Session session = Session.getInstance(props);
        session.setDebug(true);
        //创建传送对象
        Transport transport = session.getTransport();
        transport.connect(host,from,passwd);
        MimeMessage message = sendMyMessage(session,from,content);
        transport.sendMessage(message,message.getAllRecipients());
        transport.close();

    }
    private static MimeMessage sendMyMessage(Session session,String userFrom,String content) throws MessagingException,UnsupportedEncodingException{
        MimeMessage message = new MimeMessage(session);
        try{
            //设置发件人的信息，依次：地址，名称，编码格式
            message.setFrom(new InternetAddress(userFrom,"金伟华","UTF-8"));
            message.setRecipients(Message.RecipientType.TO,to);

            message.setSubject("java mail test");
            message.setText(content);
            //html

        }catch (Exception e){
            e.getMessage();
        }
        return message;
    }
}
