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

    public static void main(String[] args) {
        Connection conn = null;
        try {
            conn = getConnection();
            System.out.println("Подключение успешно ");
            Main main = new Main();

            main.viewGroups(conn);
            main.viewItems(conn);


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
