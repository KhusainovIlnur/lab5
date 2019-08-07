import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Main {
    public static final String DB_URL = "jdbc:h2:/C:/Users/Ilnur Khusainov/IdeaProjects/lab5/db/lab5_DB"; // последняя часть пути - это название файла
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

    public static void main(String[] args) {
        Connection conn = null;
        try {
            conn = getConnection();
            System.out.println("Подключение успешно ");
            Main main = new Main();
            main.doWork();

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
