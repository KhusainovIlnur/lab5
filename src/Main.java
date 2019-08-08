import java.sql.*;

public class Main {
    public static final String DB_URL = "jdbc:h2:C:/Users/Ilnur Khusainov/IdeaProjects/lab5/db/lab5_DB.mv.db"; // последняя часть пути - это название файла
    public static final String DB_Driver = "org.h2.Driver";

    // Получить новое соединение с БД
    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(DB_URL);
    }

    public Main() throws SQLException, ClassNotFoundException{
        Class.forName(DB_Driver);
    }

    private void doWork() {



    }

    private void print(ResultSet rs) throws SQLException{
        int columns = rs.getMetaData().getColumnCount();
        // Перебор строк с данными
        for (int i = 1; i <= columns; i++) {
            System.out.printf("%-8s\t", rs.getMetaData().getColumnName(i));
        }
        System.out.println();
        while(rs.next()){
            for (int i = 1; i <= columns; i++){
                System.out.printf("%-8s\t", rs.getString(i));
            }
            System.out.println();
        }
    }

    private void viewGroups(Connection conn) throws SQLException{
        String sql =  "SELECT * FROM ITEMGROUP";

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        print(rs);

        stmt.close();
    }

    private void viewItems(Connection conn) throws SQLException {
        String sql =  "SELECT * FROM ITEM";

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        print(rs);

        stmt.close();
    }

    private int getGroupId(Connection conn, String groupName) throws SQLException {
        String sql = "SELECT * FROM itemgroup WHERE TITLE = ?";
        PreparedStatement pStmt = conn.prepareStatement(sql);
        pStmt.setString(1, groupName);
        ResultSet rs = pStmt.executeQuery();
        int res;
        if (rs.next()) res = rs.getInt("ID");
        else res = -1;

        pStmt.close();
        return res;
    }

    private void viewItemsInGroup (Connection conn, int groupId) throws SQLException {
        String sql = "SELECT ITEM.ID, ITEM.TITLE, ITEM.GROUPID, ITEMGROUP.TITLE " +
                        "FROM item JOIN itemgroup ON ITEM.GROUPID = ITEMGROUP.ID " +
                        "WHERE ITEM.GROUPID = ?";
        PreparedStatement pStmt = conn.prepareStatement(sql);
        pStmt.setInt(1, groupId);
        ResultSet rs = pStmt.executeQuery();
        print(rs);

        pStmt.close();
    }

    private void viewItemsInGroup (Connection conn, String groupName) throws SQLException {
        viewItemsInGroup(conn, getGroupId(conn, groupName));
    }

    private int getCountInTable(Connection conn, String tableName) throws SQLException {
        String sql = "SELECT COUNT(*) FROM " + tableName;
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        int res;
        if (rs.next()) res = rs.getInt(1);
        else res = -1;

        stmt.close();
        return res;
    }

    private void createTablesIfNeeded(Connection conn) throws SQLException {
        String sqlCreateTables1, sqlCreateTables2;
        Statement stmt;
        sqlCreateTables1 = "" +
                "CREATE TABLE IF NOT EXISTS itemgroup(" +
                "        id INTEGER PRIMARY KEY AUTO_INCREMENT," +
                "        title VARCHAR(100) NOT NULL UNIQUE" +
                ");";

        sqlCreateTables2 =
                "CREATE TABLE IF NOT EXISTS item(" +
                "       id      INTEGER PRIMARY KEY AUTO_INCREMENT," +
                "       title   VARCHAR(100) NOT NULL UNIQUE," +
                "       groupid INTEGER," +
                "       FOREIGN KEY (groupid) REFERENCES itemgroup(id)" +
                ");";
        stmt= conn.createStatement();
        stmt.addBatch(sqlCreateTables1);
        stmt.addBatch(sqlCreateTables2);
        stmt.executeBatch();

        if (getCountInTable(conn, "ITEM") == 0) {
            stmt= conn.createStatement();

            String[] sqlBatchInsert = {
                    "INSERT INTO ITEMGROUP(TITLE) values ( 'Computers' );",
                    "INSERT INTO ITEMGROUP(TITLE) values ( 'Phones' );",
                    "INSERT INTO ITEMGROUP(TITLE) values ( 'TVs' );",

                    "INSERT INTO ITEM(TITLE, GROUPID) values ('Apple', 1);",
                    "INSERT INTO ITEM(TITLE, GROUPID) values ('DELL', 1);",
                    "INSERT INTO ITEM(TITLE, GROUPID) values ('HTC', 2);",
                    "INSERT INTO ITEM(TITLE, GROUPID) values ('Nokia', 2);",
                    "INSERT INTO ITEM(TITLE, GROUPID) values ('LG', 3);"
            };

            for (String sql : sqlBatchInsert) {
                stmt.addBatch(sql);
            }

            stmt.executeBatch();
            System.out.println("Таблицы и данные созданы");
        }
        else {
            System.out.println("Таблицы уже есть");
        }
        stmt.close();
    }

    private void dropTables(Connection conn) throws SQLException{
        String sql =    "DROP TABLE IF EXISTS ITEM;" +
                        "DROP TABLE IF EXISTS ITEMGROUP;";

        Statement stmt = conn.createStatement();
        stmt.executeUpdate(sql);

        stmt.close();
    }

    public static void main(String[] args) {
        Connection conn = null;
        try {
            conn = getConnection();
            System.out.println("Подключение успешно ");
            Main main = new Main();

            main.dropTables(conn);
            main.createTablesIfNeeded(conn);

            main.viewGroups(conn);
            System.out.println();
            main.viewItems(conn);
            System.out.println();
            System.out.println(main.getGroupId(conn, "TVs"));
//            main.viewItemsInGroup(conn, 2);
            main.viewItemsInGroup(conn, "Computers");

            main.createTablesIfNeeded(conn);
//            main.doWork();

            conn.close();
        }

        catch (SQLException e) {
            e.printStackTrace();
            System.out.println("Ошибка SQL !");
        }
        catch (ClassNotFoundException e) {
            System.out.println("JDBC драйвер для СУБД не найден!");
        }
    }
}
