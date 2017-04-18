package com.gihub.abola;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.List;
import java.util.Map;

/**
 * Created by i5-4670 on 2017/4/17.
 */
public class MySQLDataSource {

    Logger log = Logger.getLogger( this.getClass() );

    String connectionString = Settings.DB_URL+"?user="+Settings.DB_USER+"&password="+Settings.DB_PASS+"&useUnicode=true";//&characterEncoding=utf8mb4";

    Connection connection = null;

    public void connect() throws Exception {
        try{
            Class.forName( Settings.JDBC_DRIVER );
            connection = DriverManager.getConnection( connectionString );
        }
        catch(Exception e){
            log.error("Connection failed. CauseBy: " + e.getMessage()  );
            e.printStackTrace();
        }
    }
    public void execute(String sql )throws Exception{
        if ( null == connection ) {
            connect();
            execute(sql);
            return;
        }
        Statement stmt = null ;
        try {
            stmt = connection.createStatement();
        } catch (SQLException e) {
            log.error("Create statement failed CauseBy: " + e.getMessage()  );
            e.printStackTrace();
        }

        try {
            stmt.execute(sql);
        } catch (SQLException e1) {
            e1.printStackTrace();
        }

        // release resource
        try {
            stmt.close();
            stmt=null;
            //close();
            connection=null;
        } catch (Exception e) {
            // close anyway
        }
    }

    public List<Map<String, Object>> query(String sql) throws Exception{

        if ( null == connection ) {
            connect();
            return query(sql);
        }

        Statement stmt = null ;
        try {
            stmt = connection.createStatement();
        } catch (SQLException e) {
            log.error("Create statement failed CauseBy: " + e.getMessage()  );
            e.printStackTrace();
        }

        ResultSet rs = null ;
        List<Map<String, Object>> result = null;
        try {
            rs = stmt.executeQuery( sql );	// execute
            result = resultSetToListMap(rs);// transform to List<Map>
        } catch (SQLException e) {
            log.error("SQL: " + sql );
            log.error("Query failed. CauseBy: " + e.getMessage() );
            e.printStackTrace();
        }

        // release resource
        try {
            rs.close(); rs=null;
            stmt.close(); stmt=null;
            connection=null;
        } catch (Exception e) {
            // close anyway
        }


        return result;
    }

    public List<Map<String, Object>> resultSetToListMap(ResultSet rs) throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        int columns = md.getColumnCount();
        ImmutableList.Builder<Map<String, Object>> builder = ImmutableList.builder();

        while (rs.next()) {
            ImmutableMap.Builder<String, Object> mapBuilder =  ImmutableMap.builder();

            for (int i = 1; i <= columns; ++i) {
                Object v = null;
                try{
                    v = (null==rs.getObject(i)?"":rs.getObject(i));
                }
                catch(java.sql.SQLException e1){
                    try{
                        v = rs.getString(i);
                    }catch(java.sql.SQLException e2){
                        v = "";
                    }
                }
                mapBuilder.put(md.getColumnName(i), v );
            }

            builder.add( mapBuilder.build() );
        }

        return builder.build();
    }
}
