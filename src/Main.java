import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

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
            System.out.printf("%-17s\t", rs.getMetaData().getColumnName(i));
        }
        System.out.println();
        while(rs.next()){
            for (int i = 1; i <= columns; i++){
                System.out.printf("%-17s\t", rs.getString(i));
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
                "       ON DELETE CASCADE " +
                "       ON UPDATE CASCADE" +
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

    private void addItemToGroup(Connection conn, String itemName, String groupName)  throws SQLException{
        int groupId = getGroupId(conn, groupName);
        if (groupId == -1) {
            String sql = "INSERT INTO ITEMGROUP (TITLE) VALUES (?)";
            PreparedStatement pStmt = conn.prepareStatement(sql);
            pStmt.setString(1, groupName);
            pStmt.executeUpdate();
            groupId = getGroupId(conn, groupName);
        }

        String sql = "INSERT INTO ITEM (TITLE, GROUPID) VALUES (?, ?)";
        PreparedStatement pStmt = conn.prepareStatement(sql);
        pStmt.setString(1, itemName);
        pStmt.setInt(2, groupId);
        pStmt.executeUpdate();

        pStmt.close();
    }

    private void removeItemToGroup(Connection conn, String itemName, String groupName)  throws SQLException {
        int groupId = getGroupId(conn, groupName);

        if (groupId == -1) return;

        String sql = "DELETE FROM ITEM WHERE TITLE = ? AND GROUPID = ?";
        PreparedStatement pStmt = conn.prepareStatement(sql);
        pStmt.setString(1, itemName);
        pStmt.setInt(2, groupId);
        pStmt.executeUpdate();

        pStmt.close();
    }

    private List<String> readTxtFile(String filename) {
        String path = "src/txt/" + filename;
        List<String> readList = new ArrayList<>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(path));
            String s;
            while ((s = reader.readLine()) != null) {
                readList.add(s);
            }
        }
        catch (IOException e) { e.printStackTrace(); }
        finally { return readList; }
    }

    private void executeFromFile(Connection conn) throws SQLException {
        conn.setAutoCommit(false);
        try {
            for (String i : readTxtFile("items.txt")) {
                String split[];
                if (i.contains("+")) {
                    split = i.split("\\+", 2);
                    addItemToGroup(conn, split[1], split[0]);
                } else {
                    split = i.split("-", 2);
                    removeItemToGroup(conn, split[1], split[0]);
                }
            }
            conn.commit();
        }
        catch (SQLException e) {
            System.err.println("Execution failed. Transaction rollback");
            conn.rollback();
        }
    }

    private void executeFromFileGroups(Connection conn) throws SQLException {
        conn.setAutoCommit(false);
        try {
            String sql = "SELECT * FROM itemgroup";
            Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = stmt.executeQuery(sql);
            String rowTitle;

            String groupName;
            for (String i : readTxtFile("groups.txt")) {
                boolean add = false, delete = false;
                if (String.valueOf(i.charAt(0)).equals("+")) add = true;
                else delete = true;

                groupName = i.substring(1);
                boolean isExist = false;
                rs.first();
                while (rs.next()) {
                    rowTitle = rs.getString("TITLE");
                    if (rowTitle.equals(groupName) && add) {
                        isExist = true;
                        break;
                    }
                    if (rowTitle.equals(groupName) && delete) {
                        rs.deleteRow();
                        isExist = true;
                        break;
                    }
                }
                if (!isExist && add) {
                    rs.moveToInsertRow(); // переводим курсор в режим вставки
                    rs.updateString("TITLE", groupName);
                    rs.insertRow();
                    rs.moveToCurrentRow(); // переводим курсор в режим просмотра
                }
            }

            conn.commit();

        }
        catch (SQLException e) {
            System.err.println("Execution failed. Transaction rollback");
            conn.rollback();
        }
    }

        public static void main(String[] args) {
        Connection conn = null;
        try {
            conn = getConnection();
            System.out.println("Подключение успешно ");
            Main main = new Main();

           /* main.dropTables(conn);
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

            main.addItemToGroup(conn, "Tefal", "Irons");
            main.viewGroups(conn);
            System.out.println();
            main.viewItems(conn);
            System.out.println();

            main.removeItemToGroup(conn, "Tefal", "Irons");
            main.viewGroups(conn);
            System.out.println();
            main.viewItems(conn);


            main.dropTables(conn);
            main.createTablesIfNeeded(conn);

            main.viewGroups(conn);
            System.out.println();
            main.viewItems(conn);
            System.out.println();

            main.executeFromFile(conn);

            main.viewGroups(conn);
            System.out.println();
            main.viewItems(conn);
            System.out.println();

            main.dropTables(conn);
            main.createTablesIfNeeded(conn);

            main.viewGroups(conn);
            System.out.println();

            main.executeFromFileGroups(conn);

            main.viewGroups(conn);
            System.out.println();
            main.viewItems(conn);
            System.out.println();
*/

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
