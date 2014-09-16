package com.netflix.export;

import static com.netflix.export.ExportMain.*;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

public class RatingsExport extends AbstractExport
{
	public void doExport(String foldername) throws IOException
	{
		Connection conn = null;
		PreparedStatement pstmt = null;
		
		try
		{
			// read files from folder
			readFolder(foldername);
			// log errors in file
			writeFile(folder + "\\" + RATINGS_ERRORS);
			
			conn = getConnection();
			pstmt = conn.prepareStatement(RATINGS_INSERT);
			
			String line = null;
			
			String[] rdata = new String[3];
			DateFormat formatter = new SimpleDateFormat(DATE_FORMAT);
			int movie_id;
			int linecnt = 0;
			
			for (File fileEntry : folder.listFiles()) 
			{
				try
				{
					readFile(folder + "\\" + fileEntry.getName());
				
					line = br.readLine();
					
					linecnt = 0;

					if (null != line && line.contains(COLON))
					{
						movie_id = Integer.parseInt(line.substring(0, line.length()-1));
		
						pstmt.setInt(1, movie_id);
						line = br.readLine();
											
						while(null != line)
						{
							linecnt++;
							rdata = line.split(COMMA);
						
							pstmt.setInt(2, Integer.parseInt(rdata[0]));
							pstmt.setInt(3, Integer.parseInt(rdata[1]));
							pstmt.setDate(4, new Date(formatter.parse(rdata[2]).getTime()));
						
							pstmt.execute();
							line = br.readLine();
						}
					}
				}
				catch(Exception ex)
				{
					logError((new StringBuffer(fileEntry.getName()).append(" : ").append(Integer.toString(linecnt)).append(line)).toString());
				}
			}
		} 
		catch(Exception  ex)
		{
			System.out.println("Problem while loading ratings data " + ex.getMessage());
		}
		finally
		{
			closeWriteBuffer();
			cleanupConnection(pstmt, conn);
		}
	}
}
