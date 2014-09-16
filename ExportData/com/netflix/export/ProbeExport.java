package com.netflix.export;

import static com.netflix.export.ExportMain.*;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;

public class ProbeExport extends AbstractExport
{
	public void doExport(String filename) throws IOException
	{
		PreparedStatement pstmt = null;
		Connection conn = null;
		
		try
		{
			// read input file
			readFile(filename);
			// create file for logging errors
			writeFile(getPath(filename) + PROBE_ERRORS);

			conn = getConnection();
			pstmt = conn.prepareStatement(PROBE_INSERT);

			String line = br.readLine();
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
							pstmt.setInt(2, Integer.parseInt(line));
                    	
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
			System.out.println("Problem while loading probe data " + ex.getMessage());
		}
		finally
		{
			closeReadBuffer();
			closeWriteBuffer();
			cleanupConnection(pstmt, conn);
		}
	}
}
