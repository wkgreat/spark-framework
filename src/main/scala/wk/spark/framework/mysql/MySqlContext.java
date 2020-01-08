package wk.spark.framework.mysql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;

/**
 * Created by KE.WANG on 2018/5/8.
 */
public class MySqlContext implements Serializable {

    private final static Logger LOG = LoggerFactory.getLogger(MySqlContext.class);

    String driver = null;
    String url = null;
    String username = null;
    String password = null;

    public void init() {
        init("dev/conf/jdbc.properties");
    }

    public void init(String path) {

        InputStream is = MySqlContext.class.getClassLoader().getResourceAsStream(path);
        Properties props = new Properties();

        try {
            //props.load(is);
//            driver = props.getProperty("driver");
//            url = props.getProperty("url");
//            username = props.getProperty("username");
//            password = props.getProperty("password");
            driver = "com.mysql.jdbc.Driver";
            url = "jdbc:mysql://12.2.3.154:3306/webtls?useUnicode=true&characterEncoding=utf-8";
            username = "root";
            password = "123456";

            System.out.println(driver);
            System.out.println(url);
            System.out.println(username);
            System.out.println(password);

            Class.forName(driver);

        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("Mysql init fail!!");
        }

    }

    public String getDirver() {
        return driver;
    }

    public String getUrl() {
        return url;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }
}
