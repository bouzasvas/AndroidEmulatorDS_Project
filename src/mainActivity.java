import java.util.*;

import java.io.*;
import java.net.*;

public class mainActivity {
	public static void main(String[] args) {
		mainActivity activity = new mainActivity();
	}

	// PORTS
	List<String> addr_ports = null;

	// data to Mappers -- TEST
	private int topK = 10;
	double minX = -74.0144996501386;
	double maxX = -73.9018372248612;
	double minY = 40.67747711364791;
	double maxY = 40.76662365086325;
	String fromDate = "2012-05-09 00:00:00";
	String toDate = "2012-11-06 23:59:00";
	private String[] dates = new String[2];

	// final result from reducer
	private Map<Object, Long> finalResult = null;

	// network fields
	private int port;
	private ServerSocket server = null;
	private Socket reducer = null;
	private ObjectInputStream in = null;
	private ObjectOutputStream out = null;

	public mainActivity() {
		addr_ports = new ArrayList<String>();
		initPorts();
		sendToMappers();
		initConnection();
		receiveFromReducer();
		printResults();
	}

	public void initPorts() {
		try {
			FileReader fr = new FileReader("ADDR_PORTS");
			BufferedReader reader = new BufferedReader(fr);
			String line;
			try {
				while ((line = reader.readLine()) != null) {
					addr_ports.add(line);
				}
			} catch (IOException e) {
				System.err.println("Error reading next line...");
			}
		} catch (FileNotFoundException e) {
			System.err.println("Error loading file..");
		}
	}

	private void sendToMappers() {
		initDate(fromDate, toDate);
		
		ConnectToMapper map1 = new ConnectToMapper(topK, initCoordinates(1, minX, maxX, minY, maxY), dates,
				addr_ports.get(0), Integer.parseInt(addr_ports.get(1)));
		map1.connect();
		// map1.closeConnection();

		ConnectToMapper map2 = new ConnectToMapper(topK, initCoordinates(2, minX, maxX, minY, maxY), dates,
				addr_ports.get(2), Integer.parseInt(addr_ports.get(3)));
		map2.connect();
		// map2.closeConnection();

		ConnectToMapper map3 = new ConnectToMapper(topK, initCoordinates(3, minX, maxX, minY, maxY), dates,
				addr_ports.get(4), Integer.parseInt(addr_ports.get(5)));
		map3.connect();
		// map3.closeConnection();
	}

	public void initConnection() {
		port = Integer.valueOf(addr_ports.get(9));
		try {
			server = new ServerSocket(port);
			reducer = server.accept();
			out = new ObjectOutputStream(reducer.getOutputStream());
			in = new ObjectInputStream(reducer.getInputStream());
		} catch (IOException initEx) {
			initEx.printStackTrace();
		}
	}

	private void receiveFromReducer() {
		try {
			finalResult = (Map<Object, Long>) in.readObject();
		} catch (ClassNotFoundException | IOException e) {
			e.printStackTrace();
		} finally {
			try {
				out.close();
				in.close();
				reducer.close();
				server.close();
			} catch (IOException e) {
				System.err.println("Could not close streams...");
			}
		}
	}

	public void printResults() {
		System.out.println("\n-----FINAL RESULTS FROM REDUCER-----");
		System.out.println();
		for (Object key : finalResult.keySet()) {
			System.out.println("POI Name: " + key + "||" + " Count of Checkins: " + finalResult.get(key));
		}
	}

	
	//METHODS USED BY sendToMappers() method
	private List<Double> initCoordinates(int k, double minX, double maxX, double minY, double maxY) {
		List<Double> coordinates = new ArrayList<Double>();

		if (k == 1) { // if is mapper1
			coordinates.add(minX);
			double newMaxX = ((maxX - minX) * 1 / 3) + minX;
			coordinates.add(newMaxX);
			coordinates.add(minY);
			coordinates.add(maxY);
		} else if (k == 2) { // if is mapper2
			double newMinX = ((maxX - minX) * 1 / 3) + minX;
			coordinates.add(newMinX);
			double newMaxX = ((maxX - minX) * 2 / 3) + minX;
			coordinates.add(newMaxX);
			coordinates.add(minY);
			coordinates.add(maxY);
		} else if (k == 3) { // if is mapper3
			double newMinX = ((maxX - minX) * 2 / 3) + minX;
			coordinates.add(newMinX);
			coordinates.add(maxX);
			coordinates.add(minY);
			coordinates.add(maxY);
		}
		return coordinates;
	}

	private void initDate(String min, String max) {
		String minDate = "2012-05-09 00:00:00";
		String maxDate = "2012-11-06 23:59:00";
		this.dates[0] = minDate;
		this.dates[1] = maxDate;
	}
}
