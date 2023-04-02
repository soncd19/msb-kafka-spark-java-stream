package com.msb.stream.call;

import com.google.gson.JsonObject;
import com.msb.stream.connector.DatabaseConnector;
import com.msb.stream.utils.PropertiesFileReader;
import oracle.jdbc.internal.OracleCallableStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Properties;

public class CallProduceV2 implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(CallProduceV2.class);
    private final Properties properties;
    private Connection connection = null;

    public CallProduceV2(String configFile) {
        this.properties = PropertiesFileReader.readConfig(configFile);
    }

    public String process(String pProducre,JsonObject json) throws Exception {

        JsonObject jsonData = json.getAsJsonObject("Data");
        String action = json.getAsJsonObject("UserHeader").get("MyAction").getAsString();
        String response = callProduce(pProducre,action, jsonData.toString());
        logger.info(response);
        return response;
    }

    private String callProduce(String pProducre,String pAction, String pClob) throws Exception {

        if (connection == null) {
            connection = new DatabaseConnector(properties).getConnection();
        }
        long l_err_level;

        java.sql.Clob l_clob002;
        String l_Final_Str = "";
        String l_Sql = "begin "+pProducre+"(?,?,?); end;";

        l_clob002 = null;
        l_err_level = 0L;
        OracleCallableStatement stmt = null;
        try {
            logger.info("performClob:Prepare Call at:");
            stmt = connection.prepareCall(l_Sql).unwrap(OracleCallableStatement.class);

            stmt.setString(1, pAction);
            setClobAsString(stmt, 2, pClob);
            logger.info("performClob:After setClobAsString at:");
            stmt.registerOutParameter(3, Types.CLOB);

            logger.info("performClob:Begin execute at:");                                                            

            stmt.execute();
            logger.info("performClob:End execute at:");
            l_clob002 = stmt.getClob(3);
            logger.info("performClob:Got Clob at:");

        } catch (SQLException e) {
            logger.info("performClob:my Json error:" + e + "~ at level:" + l_err_level);
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }
        logger.info("performClob:Before convertclobToString at:");
        String l_temp_str = convertclobToString(l_clob002);
        logger.info("performClob:After convertclobToString at:");
        l_Final_Str = l_temp_str;
        return l_Final_Str;
    }

    public String convertclobToString(java.sql.Clob data) throws Exception {
        final StringBuilder sb = new StringBuilder();
        try {
            final Reader reader = data.getCharacterStream();
            final BufferedReader br = new BufferedReader(reader);

            int b;
            while (-1 != (b = br.read())) {
                sb.append((char) b);
            }

            br.close();
        } catch (SQLException e) {
            logger.info("SQL. Could not convert CLOB to string" + e.toString());
            return "";
        } catch (IOException e) {

            logger.info("IO. Could not convert CLOB to string" + e.toString());
            return "";
        }

        return sb.toString();
    }

    public void setClobAsString(OracleCallableStatement ps, int paramIndex, String content) throws SQLException {

        if (content != null) {
            ps.setClob(paramIndex, new StringReader(content), content.length());
        } else {
            ps.setClob(paramIndex, (Clob) null);
        }

    }
}
