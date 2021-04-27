/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.rmj.replication.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.rmj.appdriver.GCrypt;
import org.rmj.appdriver.GProperty;
import org.rmj.appdriver.MiscUtil;
import org.rmj.appdriver.SQLUtil;
import org.rmj.lib.net.LogWrapper;
import org.rmj.lib.net.MiscReplUtil;
import org.rmj.lib.net.SFTP_DU;
import org.rmj.appdriver.agent.GRiderX;

public class Import {
    private static String SIGNATURE = "08220326";
    private static LogWrapper logwrapr = new LogWrapper("Server.Import", "Import.log");
    private static String local_dir = null;
    private static GRiderX instance = null;
    private static Connection app_con = null;
    private static String exceptnx="";
    //added for ftp_import
    private static SFTP_DU sftp;
    private static String sftpx_dir = null;
    private static boolean use_ftp=false;
    
    public static void main(String[] args) {
        ImportServer sds = null;  
        
        try {
            Socket clientSocket = new Socket("localhost", ImportServer.PORT);
            System.out.println("*** Already running!" + ImportServer.PORT);
            System.exit(1);
        }
        catch (Exception e) {
            sds = new ImportServer();
            sds.start();
        }        
        
        String path;
        if(System.getProperty("os.name").toLowerCase().contains("win")){
            path = "D:/GGC_Java_Systems";
        }
        else{
            path = "/srv/GGC_Java_Systems";
        }
        System.setProperty("sys.default.path.config", path);
        
        instance = new GRiderX("gRider");
        instance.setOnline(false);

        ResultSet rs = null;
        boolean bErr = false;

        //load host_dir here
        System.out.println("Connecting to server");
        if(!connect_server()) {
           sds.interrupt();
           return;
        }
        if(!prepareSFTPHost()) {
           sds.interrupt();
           return;
        }
        
        System.out.println("Extracting record");
        rs = extract_record();
        if(rs != null){
            try {
                //import all files that are imported from the appserver by the Upload object
               while(rs.next()){
                  
                  //kalyptus - 2017.06.14 09:44am
                  //download the file from the ftp server if config says it is using a ftp server...
                  if(use_ftp){
                     if(!download_file(rs.getString("sFileName"))){
                       bErr = true;
                       break;                        
                     }
                  }
                  
                   //untar file 
                  System.out.println("Extracting " + rs.getString("sFileName"));  
                  if(!untar_json(rs.getString("sFileName"))){
                    bErr = true;
                    break;
                  }

                   //import file content to local database...
                  System.out.println("Importing " + rs.getString("sFileName"));
                  if(!import_file(rs.getString("sFileName"))){
                       bErr = true;
                       break;
                   }
                    
                  System.out.println("Posting success for " + rs.getString("sFileName"));
                  //update local database that file was already imported...
                   if(!post_success(rs.getString("sFileName"))){
                       bErr = true;
                       break;
                   }
               }
            } catch (SQLException ex) {
               bErr = true;
                logwrapr.severe("SQLException error detected.", ex);
            } finally {
                MiscUtil.close(app_con);
            }        
            
        System.out.println("Stopping Thread: " + Calendar.getInstance().getTime());
        sds.interrupt();
        System.out.println("Thread Stopped: " + Calendar.getInstance().getTime());
        System.exit(0);        
        }
    }
    
    private static boolean post_success(String filename){
        boolean bErr = false;
        
        try {
            
            String lsSQL = "UPDATE xxxIncomingLog" +
                          " SET cTranStat = '2'" + 
                             ", dImported = " + SQLUtil.toSQL(instance.getServerDate()) +  
                          " WHERE sFileName = " + SQLUtil.toSQL(filename);
            instance.getConnection().createStatement().executeUpdate(lsSQL);              

            File file = new File(local_dir + "upload/unzipped/" + filename.substring(0, 4) + "/" + filename + ".json");
            if (file.exists()) {
                file.delete();
            }              

           file = null;
            
        } 
        catch (SQLException ex) {
            logwrapr.severe("IOException error detected.", ex);
            bErr = true;
        }
        
        
        return !bErr;
    }
    
    private static ResultSet extract_record(){
        ResultSet rs = null;
        
        try {
            String lsSQL = "SELECT sFileName" +
                          " FROM xxxIncomingLog" + 
                          " WHERE cTranStat = '1'" + 
                          " ORDER BY dReceived ASC";
            rs = instance.getConnection().createStatement().executeQuery(lsSQL);              
            System.out.println(instance.getDataSource().getConnection());
            System.out.println("Is First Row 1: " + rs.getRow());
        } 
        catch (SQLException ex) {
            logwrapr.severe("IOException error detected.", ex);
        }
        
        return rs;
    }    
    
    private static boolean import_file(String filename){
        boolean bErr = false;
        
        JSONParser parser = new JSONParser();
        
        try {     
           
            app_con.setAutoCommit(false);
            
            JSONArray a = (JSONArray) parser.parse(new InputStreamReader(new FileInputStream(local_dir + "upload/unzipped/" + filename.substring(0, 4) + "/" + filename + ".json"), "UTF-8"));
            for (Object o : a){
                JSONObject json = (JSONObject) o;
                try{    
                    
                    if(!isDuplicate((String) json.get("sTransNox"), (String) json.get("sModified"))){
                        app_con.createStatement().executeUpdate((String) json.get("sStatemnt"));

                        StringBuilder str = new StringBuilder();
                        str.append("INSERT INTO xxxReplicationLog SET");
                        str.append("  sTransNox = " + SQLUtil.toSQL((String) json.get("sTransNox")));
                        str.append(", sBranchCd = " + SQLUtil.toSQL((String) json.get("sBranchCd")));
                        str.append(", sStatemnt = " + SQLUtil.toSQL((String) json.get("sStatemnt")));
                        str.append(", sTableNme = " + SQLUtil.toSQL((String) json.get("sTableNme")));
                        str.append(", sDestinat = " + SQLUtil.toSQL((String) json.get("sDestinat")));
                        str.append(", sModified = " + SQLUtil.toSQL((String) json.get("sModified")));
                        str.append(", dEntryDte = " + SQLUtil.toSQL(SQLUtil.toDate((String) json.get("dEntryDte"), "MMM d, yyyy K:m:s")));                        
                        str.append(", dModified = " + SQLUtil.toSQL(instance.getServerDate(instance.getConnection())));
                        
                        app_con.createStatement().executeUpdate(str.toString());
                        
                        str = null;
                    }
                } catch (SQLException ex) {
                    
                    logwrapr.severe("SQLException error detected.", ex);
                    logwrapr.info((String) json.get("sStatemnt"));
                    logwrapr.info("Error Code: " + ex.getErrorCode());
                    System.out.println("Error Code: " + ex.getErrorCode());
                   
                    if(!exceptnx.contains(String.valueOf(ex.getErrorCode()))){
                        bErr = true;
                        break;                        
                    }
                    
//                    //if not duplicate continue:1022 & 1062, Out range:1690
//                    //System.out.println("Error Code: " + ex.getErrorCode());
//                    if(!(ex.getErrorCode() == 1022 || ex.getErrorCode() == 1062 || ex.getErrorCode() == 1690)){
//                        bErr = true;
//                        break;
//                    }
                }
                
                json = null;
            }
            
            a = null;
        } catch (FileNotFoundException ex) {
            logwrapr.severe("FileNotFoundException error detected.", ex);
            bErr = true;
        } catch (IOException ex) {
            logwrapr.severe("IOException error detected.", ex);
            bErr = true;
        } catch (ParseException ex) {
            logwrapr.severe("ParseException error detected.", ex);
            bErr = true;
        } catch (SQLException ex) {
            logwrapr.severe("SQLException error detected.", ex);
            bErr = true;
        }
        
        try {     
            if(!bErr)
                app_con.commit();
            else
                app_con.rollback();
        } catch (SQLException ex) {
            logwrapr.severe("SQLException error detected.", ex);
            bErr = true;
        }
        
        return !bErr;
    }
    
    private static boolean untar_json(String filename){
        boolean bErr = false;
        
        try {
            MiscReplUtil.untar(local_dir + "upload/zipped/" + filename.substring(0, 4) +  "/" + filename + ".json.tar.gz", local_dir + "upload/unzipped/" + filename.substring(0, 4) + "/");
        } catch (IOException ex) {
            logwrapr.severe("IOException error detected.", ex);
            bErr = true;
        }
        
        return !bErr;
    }    
    
    private static boolean connect_server(){
        boolean bErr = false;
        //Get configuration values
        try{
            GCrypt loEnc = new GCrypt(SIGNATURE);
            GProperty loProp = new GProperty("GhostRiderXP");
            String psDBNameXX = loProp.getConfig("IntegSys-Database");
            String psDBSrvrMn = loProp.getConfig("IntegSys-MainServer");
            String psDBUserNm = loEnc.decrypt(loProp.getConfig("IntegSys-UserName"));
            String psDBPassWD = loEnc.decrypt(loProp.getConfig("IntegSys-Password"));
            String psDBPortNo = loProp.getConfig("IntegSys-Port");

            if (psDBPortNo.equals("")){
               psDBPortNo = "3306";
            }

            app_con = MiscUtil.getConnection(psDBSrvrMn, psDBNameXX, psDBUserNm, psDBPassWD, psDBPortNo);

        }catch(Exception ex){
            logwrapr.severe("Exception error detected.", ex);
            bErr = true;
        }

        return !bErr;
    }

    private static boolean isDuplicate(String trailno, String by) {
        ResultSet rs = null;
        boolean duplicate = false;
        try {
            String lsSQL = "SELECT *" +
                          " FROM xxxReplicationLog" + 
                          " WHERE sTransNox = " + SQLUtil.toSQL(trailno) + 
                            " AND sModified = " + SQLUtil.toSQL(by);

            rs = app_con.createStatement().executeQuery(lsSQL);              
            
            duplicate = rs.next();
        } 
        catch (SQLException ex) {
            logwrapr.severe("SQLException error detected.", ex);
        }
        finally{
            MiscUtil.close(rs);
        }

        return duplicate;
    }

    //kalyptus - 2017.06.14 09:44am
    //load other configs including sftp if config indicates it will use sftp
    private static boolean prepareSFTPHost(){
        GProperty loProp = new GProperty("ReplicaXP");
        
        sftpx_dir = loProp.getConfig("sftpfldr");
        local_dir = loProp.getConfig("loclfldr");
        exceptnx = loProp.getConfig("ignoreme");
        use_ftp =  loProp.getConfig("useftp").equals("1");
        
        //this will not use the ftp so just indicate success in loading...
        if(!use_ftp) return true;
        
        GCrypt loEnc = new GCrypt(SIGNATURE);
        sftp = new SFTP_DU();
        sftp.setPort(Integer.valueOf(loProp.getConfig("sftpport")));
        sftp.setUser(loEnc.decrypt(loProp.getConfig("sftpuser")));
        sftp.setPassword(loEnc.decrypt(loProp.getConfig("sftppass")));
        
        String sFTPHost = loProp.getConfig("sftphost");
        return sftp.xConnect(sFTPHost);
    }    

    //kalyptus - 2017.06.14 09:44am
    //download the file from the ftp server before importing
    private static boolean download_file(String filename){
        boolean bErr = false;
        try {
            String downpath = local_dir + "upload/zipped/";
            String branch = filename.substring(0, 4).toUpperCase();
            File downpth = new File(downpath + branch + "/");
            if (!downpth.exists()) {
                downpth.mkdirs();
            }            
            
            //System.out.println(sftpx_dir + "upload/zipped/" + branch + "/");
            sftp.Download(sftpx_dir + "upload/zipped/" + branch + "/"
                        , local_dir + "upload/zipped/" + branch + "/"
                        , filename + ".json.tar.gz");
        } catch (Exception ex) {
            logwrapr.severe("download_file: Exception error detected.", ex);
            bErr = true;
        }

        return !bErr;
    }    
}
