import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

// 假設 cpds 是您已經設定好的 ComboPooledDataSource，並且是單例或可被所有執行緒存取
// ComboPooledDataSource cpds = new ComboPooledDataSource(); 
// ... 設定 cpds 的屬性 ...

class SqlQueryTask implements Runnable {
    private ComboPooledDataSource dataSource;
    private String someDataToQuery;

    public SqlQueryTask(ComboPooledDataSource ds, String data) {
        this.dataSource = ds;
        this.someDataToQuery = data;
    }

    @Override
    public void run() {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        
        try {
            // 1. 從連線池借用一個連線 (每個執行緒都獨立執行這一步)
            conn = dataSource.getConnection();
            
            // 2. 執行您的 SQL 查詢
            String sql = "SELECT * FROM your_table WHERE a_column = ?";
            pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, this.someDataToQuery);
            rs = pstmt.executeQuery();
            
            // 3. 處理查詢結果
            while (rs.next()) {
                // ... process your data ...
            }
            
        } catch (SQLException e) {
            // 務必處理例外狀況
            e.printStackTrace();
        } finally {
            // 4. 關鍵步驟：無論成功或失敗，都必須在 finally 區塊中關閉資源
            // c3p0 會攔截 close() 方法，將連線歸還到池中，而不是真的關閉它
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) { /* log error */ }
            }
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (SQLException e) { /* log error */ }
            }
            if (conn != null) {
                try {
                    conn.close(); // 這一步是「歸還」連線，非常重要！
                } catch (SQLException e) { /* log error */ }
            }
        }
    }
}

// 在您的主程式中，可以這樣使用 ExecutorService 來執行平行任務
// ExecutorService executor = Executors.newFixedThreadPool(10); // 例如開 10 個執行緒
// for (String data : dataToProcess) {
//     executor.submit(new SqlQueryTask(cpds, data));
// }
// executor.shutdown();
