package csvReaderLoader.Controller;

import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.opencsv.CSVReader;



public class ReadCsv {
	public static void main(String[] args) {
		String jdbcUrl = "jdbc:mysql://localhost:3306/test_db_1";
        String username = "root";
        String password = "BPXmysql*123";
        String csvFilePath = "D:\\Code\\Java\\spring\\sample_csv\\zuari1698665250998_2023-10-26+to+2023-10-30.csv";
        
String insertQuery = "INSERT INTO del_rep_sample(sender,receiver,country_code,message_id,conversation_id,request_id,conversation_type,campaign,template,message_payload,payload_type,message_type,MO_MT,sent,delivered,`read`,failed,error_code,error_description,`source`,pair_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        
        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement preparedStatement = connection.prepareStatement(insertQuery);
             CSVReader reader = new CSVReader(new FileReader(csvFilePath))) {
             
            String[] nextLine;
            while ((nextLine = reader.readNext()) != null) {
                for (int i = 0; i < nextLine.length; i++) {
                    preparedStatement.setString(i + 1, nextLine[i]);
                }
                preparedStatement.executeUpdate();
            }
            
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

	}

}
