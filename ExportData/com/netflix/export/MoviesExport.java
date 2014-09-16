package com.netflix.export;

import static com.netflix.export.ExportMain.*;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;

public class MoviesExport extends AbstractExport
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
			writeFile(getPath(filename) + MOVIES_ERRORS);
			
			conn = getConnection();
			pstmt = conn.prepareStatement(MOVIES_INSERT);

			String[] moviedata = new String[3];
			String line = br.readLine();
			int linecnt = 0;
			
			while(null != line)
			{
				moviedata = line.split(COMMA);
				try
				{
					linecnt++;
					
					if ((null == moviedata[0]) || (null == moviedata[1]) || (null == moviedata[2]))
					{
						logError((new StringBuffer(Integer.toString(linecnt)).append(" : ").append(line)).toString());
					}
					else
					{
						pstmt.setInt(1, Integer.parseInt(moviedata[0]));
						pstmt.setInt(2, Integer.parseInt(moviedata[1]));
						pstmt.setString(3, moviedata[2]);
						pstmt.execute();
					}
				}
				catch(Exception ex)
				{
					logError((new StringBuffer(Integer.toString(linecnt)).append(" : ").append(line)).toString());
				}
				
			    line = br.readLine();
			}
		} 
		catch(Exception  ex)
		{
			System.out.println("Problem while loading movies data " + ex.getMessage());
		}
		finally
		{
			closeReadBuffer();
			closeWriteBuffer();
			cleanupConnection(pstmt, conn);
		}
	}
}

