package com.netflix.export;

import static com.netflix.export.ExportMain.*;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

public class QualifyingExport extends AbstractExport
{
	public void doExport(String filename) throws IOException
	{
		Connection conn = null;
		PreparedStatement pstmt = null;
		
		try
		{
			// read input file
			readFile(filename);
			// create file for logging errors
			writeFile(getPath(filename) + QUALIFYING_ERRORS);

			conn = getConnection();
			pstmt = conn.prepareStatement(QUALIFYING_INSERT);

			String line = br.readLine();
			String[] qdata = new String[2];
			DateFormat formatter = new SimpleDateFormat(DATE_FORMAT);
			int movie_id;
			int linecnt = 0;
			
			while(null != line)
			{
				try
				{
					linecnt++;
					
					if (line.contains(COLON))
					{
						movie_id = Integer.parseInt(line.substring(0, line.length()-1));
		
						pstmt.setInt(1, movie_id);
						line = br.readLine();
						
						while(null != line && !line.contains(COLON))
						{
							qdata = line.split(COMMA);
							pstmt.setInt(2, Integer.parseInt(qdata[0]));
							pstmt.setDate(3, new Date(formatter.parse(qdata[1]).getTime()));
                    	
							pstmt.execute();
							line = br.readLine();
						}
					}
					else
					{
						line = br.readLine();
					}
				}
				catch(Exception ex)
				{
					logError((new StringBuffer(Integer.toString(linecnt)).append(" : ").append(line)).toString());
					line = br.readLine();
				}
			}
		} 
		catch(Exception  ex)
		{
			System.out.println("Problem while loading qualifying data " + ex.getMessage());
		}
		finally
		{
			closeReadBuffer();
			closeWriteBuffer();
			cleanupConnection(pstmt, conn);
		}
	}
}
