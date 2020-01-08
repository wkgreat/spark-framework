package wk.spark.framework.mysql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by KE.WANG on 2018/5/8.
 */
public class MySqlHelper {

    private final static Logger LOG = LoggerFactory.getLogger(MySqlHelper.class);

    MySqlContext context = null;
    String driver;
    String url;
    String username;
    String password;

    public MySqlHelper(MySqlContext context) {
        this.context = context;
    }

    public MySqlHelper(String driver, String url, String username, String password) {
        this.driver = driver;
        this.url = url;
        this.username = username;
        this.password = password;
    }

//    public Connection getConnection() {
//
//        try {
//            Class.forName(context.getDirver());
//            return DriverManager.getConnection(context.getUrl(),context.getUsername(), context.getPassword());
//        } catch (SQLException | ClassNotFoundException e) {
//            e.printStackTrace();
//        }
//        return null;
//
//    }

    public static void releaseDb(PreparedStatement ps, Connection ct) {
        try {
            ps.close();
            ct.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void close(Connection ct, PreparedStatement ps, ResultSet rs) {
        try {
            if (rs != null) {
                rs.close();
            }
            if (ps != null) {
                ps.close();
            }
            if (ct != null) {
                ct.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public Connection getConnection() {

        try {
            Class.forName(driver);
            return DriverManager.getConnection(url, username, password);
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;

    }

    public void readData(String tableName) {
        Connection ct = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            ct = getConnection();
            String sql = String.format("select * from %s limit 10", tableName);
            ps = ct.prepareStatement(sql);
            rs = ps.executeQuery();

            ResultSetMetaData meta = rs.getMetaData();
            System.out.println("Count " + meta.getColumnCount());
            int cols = meta.getColumnCount();
            for (int i = 1; i <= cols; ++i) {
                System.out.print(meta.getColumnName(i) + "\t");
            }
            System.out.println();
            while (rs.next()) {
                for (int i = 1; i <= cols; ++i) {
                    System.out.print(rs.getObject(i) + "\t");
                }
                System.out.println();
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            close(ps, rs);
        }
    }

    public void getDataBySql(String sql) {
        Connection ct = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            ct = getConnection();
            ps = ct.prepareStatement(sql);
            rs = ps.executeQuery();

            ResultSetMetaData meta = rs.getMetaData();
            System.out.println("Count " + meta.getColumnCount());
            int cols = meta.getColumnCount();
            for (int i = 1; i <= cols; ++i) {
                System.out.print(meta.getColumnName(i) + ",");
            }
            System.out.println();
            while (rs.next()) {
                for (int i = 1; i <= cols; ++i) {
                    System.out.print(rs.getObject(i) + ",");
                }
                System.out.println();
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            close(ps, rs);
        }
    }

    public Set<String> getColumnValues(String sql) {
        Connection ct = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        Set<String> valueSet = new HashSet<>();
        try {
            ct = getConnection();
            ps = ct.prepareStatement(sql);
            rs = ps.executeQuery();
            ResultSetMetaData meta = rs.getMetaData();
            if (meta.getColumnCount() != 1) {
                LOG.error("The column count must be 1!");
            } else {
                while (rs.next()) {
                    valueSet.add(rs.getString(1));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            close(ps, rs);
            return valueSet;
        }
    }

    public List<List<Object>> getTableValues(String sql) {
        Connection ct = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<List<Object>> records = new ArrayList<>();
        try {
            ct = getConnection();
            ps = ct.prepareStatement(sql);
            rs = ps.executeQuery();
            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();
            if (null != rs) {
                while (rs.next()) {
                    List<Object> fields = new ArrayList<>();
                    for (int i = 1; i <= columnCount; ++i) {
                        fields.add(rs.getObject(i));
                    }
                    records.add(fields);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            close(ps, rs);
            return records;
        }
    }

    public synchronized void insertData(String sql, Object[][] data) {
        Connection ct = null;
        PreparedStatement ps = null;

        try {
            ct = getConnection();
            ct.setAutoCommit(false);
            ps = ct.prepareStatement(sql);

            for (Object[] d : data) {
                for (int i = 0; i < d.length; ++i) {
                    ps.setObject(i + 1, d[i]);
                }
                ps.addBatch();
            }
            ps.executeBatch();
            ct.commit();

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                ct.setAutoCommit(true);
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                close(ps, null);
            }
        }
    }

    public synchronized void insertData(String sql, List<List<Object>> data) {
        Connection ct = null;
        PreparedStatement ps = null;

        try {
            ct = getConnection();
            ct.setAutoCommit(false);
            ps = ct.prepareStatement(sql);

            for (List<Object> d : data) {
                for (int i = 0; i < d.size(); ++i) {
                    ps.setObject(i + 1, d.get(i));
                }
                ps.addBatch();
            }
            ps.executeBatch();
            ct.commit();

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                ct.setAutoCommit(true);
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                close(ps, null);
            }
        }
    }

    public synchronized void close(PreparedStatement ps, ResultSet rs) {
        try {
            if (null != rs) {
                rs.close();
            }
            if (null != ps) {
                ps.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
