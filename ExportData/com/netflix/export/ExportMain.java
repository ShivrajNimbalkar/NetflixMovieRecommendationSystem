package com.netflix.export;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class ExportMain 
{
	static Properties prop = null;
	
	public static void main(String[] args) throws FileNotFoundException, IOException
	{
		if(args.length < 1) 
		{
			System.out.println("Usage: \njava ExportMain <Path to Config File>");
			return;
		}
		
		String dataFile = args[0];
		System.out.println("Loading configurations from: " + args[0] + "...");
		
		if (null != dataFile)
		{
			loadProperties(dataFile);
		
			System.out.println("Loading data into Database ");
		
			exportAll();

			System.out.println("Done");
		}
	}
	
	public static void exportAll() throws IOException
	{
		String moviesfile = prop.getProperty("data.export.moviesfile");
		if (null != moviesfile)
		{
			MoviesExport me = new MoviesExport();
			me.doExport(moviesfile);
		}

		System.out.println("Loaded movies data into Database ");

		String probefile = prop.getProperty("data.export.probefile");
		if (null != probefile)
		{
			ProbeExport pe = new ProbeExport();
			pe.doExport(probefile);
		}
		
		System.out.println("Loaded probe data into Database ");

		String qualifyingfile = prop.getProperty("data.export.qualifyingfile");
		if (null != qualifyingfile)
		{
			QualifyingExport qe = new QualifyingExport();
			qe.doExport(qualifyingfile);
		}
		
		System.out.println("Loaded qualifying data into Database ");
		
		String ratingsfile = prop.getProperty("data.export.ratingsfolder");
		if (null != ratingsfile)
		{
			RatingsExport re = new RatingsExport();
			re.doExport(ratingsfile);	
		}
		
		System.out.println("Loaded ratings data into Database ");
	}
	
	public static void loadProperties(String filename) throws FileNotFoundException, IOException
	{
		prop = new Properties();
		prop.load(new FileInputStream(filename));
	}
	
	public static Connection getConnection()
	{
		Connection conn = null;
		try
		{
			conn = DriverManager.getConnection(prop.getProperty("db.connect.servername"),
				prop.getProperty("db.connect.username"), prop.getProperty("db.connect.password"));
		
		}
		catch(SQLException se)
		{
			System.out.println("Connection opening problem " + se.getMessage());
		}
		return conn;
	}
	
	public static void cleanupConnection(Statement stmt, Connection con)
	{
		try 
		{
			if (null != stmt)
			{
				stmt.close();
			}
			if (null != con)
			{
				con.close();
			}
		}
		catch(SQLException se)
		{
			System.out.println("Connection closing problem " + se.getMessage());
		}
	}
	
	public static final String MOVIES_INSERT = "INSERT INTO MOVIES (ID,YEAR,TITLE) VALUES (?,?,?)";
	public static final String PROBE_INSERT = "INSERT INTO PROBE (MOVIE_ID, CUSTOMER_ID) VALUES (?,?)";
	public static final String QUALIFYING_INSERT = "INSERT INTO QUALIFYING (MOVIE_ID, CUSTOMER_ID, DATE) VALUES (?,?,?)";
	public static final String RATINGS_INSERT = "INSERT INTO RATINGS (MOVIE_ID, CUSTOMER_ID, RATING, DATE) VALUES (?,?,?,?)";

	public static final String DATE_FORMAT = "yyyy-MM-dd";
	public static final String EMPTY_STRING = "";
	public static final String COLON = ":";
	public static final String COMMA = ",";
	
	public static final String MOVIES_ERRORS = "movies_errors.txt";
	public static final String PROBE_ERRORS = "probe_errors.txt";
	public static final String RATINGS_ERRORS = "ratings_errors.txt";
	public static final String QUALIFYING_ERRORS = "qualifying_errors.txt";
}
