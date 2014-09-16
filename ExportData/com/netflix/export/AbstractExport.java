package com.netflix.export;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public abstract class AbstractExport 
{
	BufferedReader br = null;
	BufferedWriter bw = null;
	File folder = null;
	
	public abstract void doExport(String filename) throws Exception;
	
	public void readFile(String filename) throws FileNotFoundException
	{
		br = new BufferedReader(new FileReader(filename));
	}
	
	public void readFolder(String foldername) throws FileNotFoundException
	{
		folder = new File(foldername);
	}
	
	public void writeFile(String filename) throws IOException
	{
		bw = new BufferedWriter(new FileWriter(filename, true));
	}
	
	public void logError(String error) throws IOException
	{
		bw.append(error);
		bw.append('\n');
	}
	
	public String getPath(String filepath)
	{
		return (filepath.substring(0, filepath.lastIndexOf("\\")+1));	
	}
	
	public void closeReadBuffer() throws IOException
	{
		br.close();
	}
	
	public void closeWriteBuffer() throws IOException
	{
		bw.close();
	}
}
