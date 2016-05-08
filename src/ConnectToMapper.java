
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;

public class ConnectToMapper {

	private double minX, maxX, minY, maxY;
	private String minDatetime;
	private String maxDatetime;
	private int topK;

	private String address;
	private int port;

	private Socket client = null;
	private ObjectInputStream in = null;
	private ObjectOutputStream out = null;

	
	public ConnectToMapper(int topK, List<Double> values, String[] datetime, String address, int port) {
		setValues(values.get(0), values.get(1), values.get(2), values.get(3), datetime[0], datetime[1]);
		this.topK = topK;
		this.address = address;
		this.port = port;
	}
	
	public ConnectToMapper(double minX, double maxX, double minY, double maxY, String[] datetime) {
		setValues(minX, maxX, minY, maxY, datetime[0], datetime[1]);
	}
	
	public void setValues(double minX, double maxX, double minY, double maxY, String minDatetime, String maxDatetime) {
		this.minX = minX;
		this.maxX = maxX;
		this.minY = minY;
		this.maxY = maxY;
		this.minDatetime = minDatetime;
		this.maxDatetime = maxDatetime;
	}
	
	public void connect() {	//this method establishes the connection
			try {
				client = new Socket(InetAddress.getByName(address), port);
				
				out = new ObjectOutputStream(client.getOutputStream());
				in = new ObjectInputStream(client.getInputStream());

				String serverResponse = null;
//				try {
//					serverResponse = (String) in.readObject();
//				} catch (ClassNotFoundException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//				System.out.println(">"+serverResponse);
				
				out.writeInt(topK);
				out.flush();
				
				out.writeDouble(minX);
				out.flush();
				
				out.writeDouble(maxX);
				out.flush();
				
				out.writeDouble(minY);
				out.flush();
				
				out.writeDouble(maxY);
				out.flush();
				
				out.writeObject(minDatetime);
				out.flush();
				
				out.writeObject(maxDatetime);
				out.flush();
			} catch (UnknownHostException e) {
				System.err.println("Could not find host...");
			} catch (IOException e) {
				System.err.println("Could not connect...");
			}
			finally {
				try {
					if (!this.address.equals("localhost"))
						out.close();
				}
				catch (IOException e) {
					e.printStackTrace();
				}
			}	
		}
	
	public void closeConnection() {
		try {
			in.close();
			out.close();
			client.close();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}
}
