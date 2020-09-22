package LoadCsvtoDB;

//import sql directories

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class LoadCSVFile {

	public static void main(String[] args)

	{

		// define path of the csv file
		String path = "C:\\Users\\asket\\Documents\\Order_Src_data.csv";
		File file = new File(path); // read the file
		try {
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line;

			Connection connection = null;
			Statement statement = null;
			// connect to database
			try {
				// define buffer class to read the data from File

				Class.forName("oracle.jdbc.driver.OracleDriver");
				connection = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:orcl", "HR", "password");
				statement = connection.createStatement();

			}

			catch (Exception e) {
				System.out.println(e);

			}

			// read the scv data
			while ((line = br.readLine()) != null) {

				try {

					String[] arr = line.split(",");
					String sql = "INSERT INTO t_stg_src_orders "
							+ "(ORDER_ID,ORD_LOCATION,ORD_DATE,CREAETED_BY,TRADE_ID,BID,ASK,PRICE,ORD_QTY,ORD_STATUS,ORD_ACTION,EXECUTED_DATE) "
							+ "VALUES ('" + arr[0] + "','" + arr[1] + "','" + arr[2] + "'" + ",'" + arr[3] + "','"
							+ arr[4] + "','" + arr[5] + "','" + arr[6] + "', '" + arr[7] + "','" + arr[8] + "','"
							+ arr[9] + "','" + arr[10] + "','" + arr[11] + "') ";
					statement.execute(sql);

				} catch (SQLException e) {

					// TODO Auto-generated catch block

					System.out.println(e);

				}

				System.out.println("Data Inserted into Oracle Table");

			}

			br.close();

		} catch (IOException e) {

			// TODO Auto-generated catch block

			System.out.println(e);

		}

	}
}
