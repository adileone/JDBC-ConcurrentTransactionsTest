package transaction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class PerditaAggiornamento {

	public static Connection connectDB() throws ClassNotFoundException, SQLException {
		Connection con = null;
		Class.forName("org.postgresql.Driver");

		con = DriverManager.getConnection(
				"jdbc:postgresql://localhost:5432/test", "postgres",
				"postgres");

		return con;
	}

	public static void main(String[] args) throws ClassNotFoundException, SQLException {

		int a=0;
		int b=0;
		int c=0;
		int d=0;



		Connection conTx1 = connectDB();
		System.out.println("Transazione 1 :  "+ conTx1);
		conTx1.setAutoCommit(false); 
		conTx1.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

		System.out.println("Livello Isolamento Tx1 :  " +conTx1.getTransactionIsolation()+"\n");

		Connection conTx2 = connectDB();
		System.out.println("Transazione 2 :  "+ conTx2);
		conTx2.setAutoCommit(false); 
		conTx2.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

		System.out.println("Livello Isolamento Tx2 :  " + conTx2.getTransactionIsolation()+"\n");


		System.out.println("Aggiornamenti : " + "\n");


		Statement tx1 = conTx1.createStatement();
		ResultSet rs_tx1 = tx1.executeQuery("SELECT B FROM test.public.data WHERE A=1 "  );

		while ( rs_tx1.next()) {

			a = rs_tx1.getInt(1);

			System.out.print("r1(x) ------>  " + " x = " + a);
			System.out.print("\n");
		}


		Statement tx2 = conTx2.createStatement();
		ResultSet rs_tx2 = tx2.executeQuery("SELECT B FROM test.public.data WHERE A=1 "  );

		while ( rs_tx2.next()) {
			b = rs_tx2.getInt(1);

			System.out.print("r2(x) ------>  " + " x = " + b + "\n");
			System.out.print("\n");
		}


		tx1.executeUpdate("UPDATE test.public.data SET b = b+100 WHERE a = '1'");
		ResultSet rs_m1 = tx1.executeQuery("SELECT B FROM test.public.data WHERE A=1 "  );

		while ( rs_m1.next()) {
			c = rs_m1.getInt(1);

			System.out.print("w1(x=x+100)" + "\n" + "r1(x)---->" + c + "\n");
			System.out.print("\n");
		}

		conTx1.commit();
		conTx1.close();


		int b_modificato = b + 1;

		tx2.executeUpdate("UPDATE test.public.data SET b = " + b_modificato + " WHERE a = '1'");
		ResultSet rs_m2 = tx2.executeQuery("SELECT B FROM test.public.data WHERE A=1 "  );

		while ( rs_m2.next()) {
			d = rs_m2.getInt(1);

			System.out.print("w2(x=x+1)" + "\n" + "r2(x) ----> " + d);
			System.out.print("\n");
		}

		conTx2.commit();
		conTx2.close();

	}
}


