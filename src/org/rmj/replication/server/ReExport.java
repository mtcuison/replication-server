/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.rmj.replication.server;

import com.google.gson.stream.JsonWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import org.apache.commons.lang3.StringUtils;
import org.json.simple.JSONObject;
import org.rmj.appdriver.GCrypt;
import org.rmj.appdriver.GProperty;
import org.rmj.appdriver.MiscUtil;
import org.rmj.appdriver.SQLUtil;
import org.rmj.appdriver.agent.GRiderX;
import org.rmj.lib.net.LogWrapper;
import org.rmj.lib.net.MiscReplUtil;
import org.rmj.lib.net.SFTP_DU;

/**
 *
 * @author sayso
 */
public class ReExport {
    private static String SIGNATURE = "08220326";
    private static LogWrapper logwrapr = new LogWrapper("ReServer.Export", "ReExport.log");
    private static String local_dir = null;
    private static GRiderX instance = null;
    private static Connection app_con = null;
    private static JSONObject json_obj = null;
    private static Date dCreatedx = null;

    //added for ftp_upload
    private static SFTP_DU sftp;
    private static String sftpx_dir = null;
    private static boolean use_ftp=false;    
    
    public static void main(String[] args) {
        String batch;
        String branch;
        String ismain;
        if(args.length == 0){
            batch = "M0011900005879";
            branch = "M029";
            ismain = "0";
        }
        else{
            batch = args[0];
            branch = args[1];
            ismain = args[2];
        }
        
        boolean bErr = false;
        
        String path;
        if(System.getProperty("os.name").toLowerCase().contains("win")){
            path = "D:/GGC_Java_Systems";
        }
        else{
            path = "/srv/GGC_Java_Systems";
        }
        System.setProperty("sys.default.path.config", path);
        
//        if(args.length == 3){
//            batch = args[0];
//            branch = args[1];
//            ismain = args[2];
//        }
        
        instance = new GRiderX("gRider");
        instance.setOnline(false);
        
        System.out.println(instance.getBranchCode());
        System.out.println(instance.getClientID());

        if(args.length == 0){
            dCreatedx = SQLUtil.toDate("2019-10-13 09:30:04", SQLUtil.FORMAT_TIMESTAMP);
        }
        else{
            setDateCreated(batch);
        }

        //create connection to the dbf server
        if(!connect_server()){
           return;
        }
        
        if(!prepareSFTPHost()) {
           return;
        }
        
        try {
            instance.beginTrans();
            //Create file from central dbf for export by branch(main=all;not main=needed by that branch only)
             json_obj = new JSONObject();

             json_obj.put("sBatchNox", batch);
             json_obj.put("sBranchCD", branch);

            if(!create_export( 
                 branch,
                 ismain,
                 "1")){
                logwrapr.severe("create_export error", "Error creating export");
                bErr = true;

            }
            else{

                String lsSQL = "UPDATE xxxOutGoingDetail" + 
                              " SET sFileName = " + SQLUtil.toSQL((String) json_obj.get("sFileName")) +
                                 ", sMD5Hashx = " + SQLUtil.toSQL((String) json_obj.get("sMD5Hashx")) + 
                                 ", sFileSize = " + SQLUtil.toSQL((String) json_obj.get("sFileSize")) + 
                                 ", cRecdStat = '1'" + 
                              " WHERE sBatchNox = " + SQLUtil.toSQL(batch) + 
                                " AND sBranchCD = " + SQLUtil.toSQL(branch);
                instance.getConnection().createStatement().executeUpdate(lsSQL);
            }
        } catch (SQLException ex) {
            logwrapr.severe("IOException error detected.", ex);
            bErr = true;
        }
        
        if(bErr)
            instance.rollbackTrans();
        else
            instance.commitTrans();
     
        MiscUtil.close(app_con);
        System.exit(0);                
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
            //local_dir = loProp.getConfig(instance.getProductID() + "-sftpfldr");
            
        }catch(Exception ex){
            logwrapr.severe("Exception error detected.", ex);
            bErr = true;
        }catch (OutOfMemoryError ex) {
            logwrapr.severe("OutOfMemoryError error detected.", ex);
            bErr = true;
        } 

        return !bErr;
    }
    
    //kalyptus - 2017.06.14 09:44am
    //load other configs including sftp if config indicates it will use sftp
    private static boolean prepareSFTPHost(){
        GProperty loProp = new GProperty("ReplicaXP");
        
        sftpx_dir = loProp.getConfig("sftpfldr");
        local_dir = loProp.getConfig("loclfldr");
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
    
    private static boolean create_export(String branch, String ismain, String isrepl){
        ResultSet rs = null;
        String filename;
        
        //Extract records to be exported
        rs = extract_record((String) json_obj.get("sBatchNox"), branch, ismain, isrepl); 
        if(rs == null) return false;
        
        //set filename;
        //remove space and colon from the value return by getTime.toString
        //filename = instance.getBranchCode() + MiscReplUtil.format(Calendar.getInstance().getTime(), "yyyyMMddHHmmss") + branch;
        filename = instance.getBranchCode() + MiscReplUtil.format(dCreatedx, "yyyyMMddHHmmss") + branch;
        
        //Save extracted record as json
        if(!write_file(rs, filename)) return false;
        
        if(!tar_json(filename)) return false;

        //kalyptus - 2017.06.14 02:36pm
        //upload file if config says using ftp
        if(use_ftp){
            if(!upload_file(filename)) return false;
        }
        
        //record info to the JSON objet obj
        json_obj.put("sFileName", filename);
        json_obj.put("sMD5Hashx", MiscReplUtil.md5Hash(local_dir + "download/zipped/" + filename.substring(filename.length() - 4) + "/" + filename + ".json.tar.gz"));
        json_obj.put("cRecdStat", "0");
        
        //Delete json file
        File file = new File(local_dir + "download/unzipped/" + filename.substring(filename.length() - 4) + "/" + filename + ".json");
        if (file.exists()) {
            file.delete();
        }          
        
        //Save size of tarred file
        file = new File(local_dir + "download/zipped/" + filename.substring(filename.length() - 4) + "/" + filename + ".json.tar.gz");
        if (file.exists()) {
            //DecimalFormat df = new DecimalFormat("##.##");
            json_obj.put("sFileSize", String.format("%.2f", (float)file.length()/1024));
        }          
        
        rs = null;
        file = null;
        
        return true;
    }
    
    private static ResultSet extract_record(String batch, String branch, String ismain, String isrepl){
        ResultSet rs = null;
        
        try {
            String lsFilter = getFilter(batch, branch);
            String lsSQL = "";
            String division = getDivision(branch);
            
            if(ismain.equals("1")){
                lsSQL = " SELECT a.*" +
                        " FROM xxxReplicationLog a" +
                        " WHERE (" + lsFilter + ")" +
                        " ORDER BY a.dModified, a.sTransNox";
            }
            else{
                lsSQL = " SELECT a.* " +
                        " FROM xxxReplicationLog a, xxxSysTable b" +
                        " WHERE (" + lsFilter + ")" +
                          " AND a.sTableNme = b.sTableNme" +
                          " AND ((b.cTableTyp = '0')" +
                             " OR (b.cTableTyp = '2' AND a.sDestinat = " + SQLUtil.toSQL(branch) + ")" +
                             " OR (b.cTableTyp = '1' AND a.sBranchCd = " + SQLUtil.toSQL(branch) + ")" +
                             " OR (b.cTableTyp = '3' AND a.sBranchCd LIKE " + SQLUtil.toSQL(branch.substring(0, 1) + "%") + ")" +
                             " OR (b.cTableTyp = '3' AND a.sBranchCd LIKE 'G%')" +
                             " OR (b.cTableTyp = '3' AND (a.sBranchCd & " + SQLUtil.toSQL(division) + " > 0 ))" +
                             " OR (a.sBranchCD = " + SQLUtil.toSQL(branch) + " AND a.sTransNox NOT LIKE " + SQLUtil.toSQL(branch + "%") + "))" +
                        " ORDER BY a.dModified, a.sTransNox";
            }
            
            rs = app_con.createStatement().executeQuery(lsSQL.toString());
            
            //Table Type
            //0 = All Branches Regardless of Division
            //1 = Branch Only     
            //2 = Destination
            //3 = Division
            
            //Divisions
            //Cd -> Val = Description
            // 0 ->  1  = Mobile Phone      
            // 1 ->  2  = Motorcyle
            // 2 ->  4  = Auto Group
            // 3 ->  8  = Hospitality
        } 
        catch (SQLException ex) {
            logwrapr.severe("IOException error detected.", ex);
        }
        catch (OutOfMemoryError ex) {
            logwrapr.severe("OutOfMemoryError error detected.", ex);
         }                
        
        return rs;
    }
    
    private static String getDivision(String branch){
       String lsSQL = "SELECT sDivsnVal " +
                     " FROM Branch_Others a" +
                        "  LEFT JOIN Division b ON a.cDivision = b.sDivsnCde" +
                     " WHERE a.sBranchCD = " + SQLUtil.toSQL(branch);
       
       ResultSet loRS = instance.executeQuery(lsSQL);
       
       String division = "0";
       try {
          if(loRS.next()){
             division = loRS.getString("sDivsnVal");
          }
       } catch (SQLException ex) {
          ex.printStackTrace();
       }
       
       return division;
    }
 
    private static boolean write_file(ResultSet rs, String filename){
        boolean bErr = false;
        
        try {
            String branch = filename.substring(filename.length() - 4).toUpperCase();
            System.out.println("Filename: " + filename);
            String destpath = local_dir + "download/unzipped/" + branch + "/";
            File destpth = new File(destpath);
            if (!destpth.exists()) {
                destpth.mkdirs();
                sftp.mkdir(sftpx_dir + "download/zipped/" + branch + "/");
            }           

            String uploadpath = local_dir + "upload/zipped/" + branch + "/";
            File uploadpth = new File(uploadpath);
            if (!uploadpth.exists()) {
                uploadpth.mkdirs();
                sftp.mkdir(sftpx_dir + "upload/zipped/" + branch + "/");
            }        

            OutputStream out = new FileOutputStream(local_dir + "download/unzipped/" + branch + "/" + filename + ".json");
            JsonWriter writer = new JsonWriter(new OutputStreamWriter(out, "UTF-8"));

            //LinkedList<Map<String, String>> link = RStoLinkList(rs);
            
            writer.beginArray();
            
            //SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            SimpleDateFormat sf = new SimpleDateFormat("MMM d, yyyy K:m:s");
            
            while(rs.next()){
               writer.beginObject();
               writer.name("sTransNox").value(rs.getString("sTransNox"));
               writer.name("sBranchCd").value(rs.getString("sBranchCd"));
               writer.name("sStatemnt").value(rs.getString("sStatemnt"));
               writer.name("sTableNme").value(rs.getString("sTableNme"));
               writer.name("sDestinat").value(rs.getString("sDestinat"));
               writer.name("sModified").value(rs.getString("sModified"));
               writer.name("dEntryDte").value(MiscReplUtil.format(rs.getTimestamp("dEntryDte"), "yyyy-MM-dd HH:mm:ss"));
               writer.name("dModified").value(MiscReplUtil.format(rs.getTimestamp("dModified"), "yyyy-MM-dd HH:mm:ss"));
               writer.endObject();
            }
            
            writer.endArray();
            writer.close();
            
            sf = null;
            destpth = null;
            uploadpth = null;
            writer = null;
            out = null;
        } catch(IOException ex){
            logwrapr.severe("IOException error detected.", ex);
            bErr = true;
        } catch (SQLException ex) {
            logwrapr.severe("SQLException error detected.", ex);
            bErr = true;
        }
        catch (OutOfMemoryError ex) {
            logwrapr.severe("OutOfMemoryError error detected.", ex);
            bErr = true;
        }                
        
        return !bErr;
    }        

    private static boolean tar_json(String filename){
        boolean bErr = false;
        
        try {
            MiscReplUtil.tar(local_dir + "download/unzipped/" + filename.substring(filename.length() - 4) + "/" + filename + ".json", local_dir + "download/zipped/" + filename.substring(filename.length() - 4) + "/");
        
        } catch (IOException ex) {
            logwrapr.severe("IOException error detected.", ex);
            bErr = true;
        }
        catch (OutOfMemoryError ex) {
            logwrapr.severe("OutOfMemoryError error detected.", ex);
         }                
        
        return !bErr;
    }
    
    private static boolean upload_file(String filename){
        boolean bErr = false;
        try {
           String branch=filename.substring(filename.length() - 4).toUpperCase();
           System.out.println(local_dir + "download/zipped/"  + "»" +  sftpx_dir + "download/zipped/" + branch + "/" + "»" + filename + ".json.tar.gz");
           
           sftp.Upload(local_dir + "download/zipped/" + branch + "/" , sftpx_dir + "download/zipped/" + branch + "/", filename + ".json.tar.gz") ;
        } catch (Exception ex) {
            logwrapr.severe("upload_file: Exception error detected.", ex);
            bErr = true;
        } catch (OutOfMemoryError ex) {
            logwrapr.severe("OutOfMemoryError error detected.", ex);
            bErr = true;
        } 
        
        return !bErr;
    }    
    
    private static String getFilter(String batch, String branch){
        StringBuilder lsFilter = new StringBuilder();
        boolean bErr = false;
        ResultSet rs = null;
        
        
        try {
            String lsSQL = "SELECT DISTINCT a.sBranchCD, a.sLogFromx, a.sLogThrux" + 
                          " FROM xxxOutGoingDetail a" + 
                              " LEFT JOIN Branch_Others b ON a.sBranchCD = b.sBranchCD" + 
                          " WHERE a.sBatchNox = " + SQLUtil.toSQL(batch) + 
                            " AND b.xBranchCD <> " + SQLUtil.toSQL(branch);
            rs = instance.getConnection().createStatement().executeQuery(lsSQL);
            
            System.out.println(lsSQL);
            
            while(rs.next()){
                lsFilter = lsFilter.append(" OR (a.sTransNox LIKE " + SQLUtil.toSQL(rs.getString("sBranchCD") + "%") + " AND a.sTransNox BETWEEN " + SQLUtil.toSQL(rs.getString("sLogFromx")) + " AND " + SQLUtil.toSQL(rs.getString("sLogThrux")) + ")");
            }
            
        } catch (SQLException ex) {
            logwrapr.severe("SQLException error detected.", ex);
            bErr = true;
        } catch (OutOfMemoryError ex) {
            logwrapr.severe("OutOfMemoryError error detected.", ex);
            bErr = true;
        } finally {
            MiscUtil.close(rs);
        }

        rs = null;
        
        if(bErr)
            return "";
        else
            return lsFilter.toString().substring(4);
    }    
    
    private static void setDateCreated(String sBatchNox){
        ResultSet loRS = null;

        dCreatedx = null;
        
        try {
            //kalyptus - 2016.05.31 09:30am
            //include xxxOutGoingMaster->dCreatedx in our query
            //kalyptus - 2017.06.12 09:18am
            //include a LIMIT 1 in the query...
            String lsSQL = "SELECT *" +
                          " FROM xxxOutGoingMaster" + 
                          " WHERE sBatchNox = " + SQLUtil.toSQL(sBatchNox);
            loRS = instance.getConnection().createStatement().executeQuery(lsSQL);
            if(loRS.next()){
                dCreatedx = loRS.getTimestamp("dCreatedx");
            }
        } catch (SQLException ex) {
            logwrapr.severe("SQLException error detected.", ex);
        }
        catch (OutOfMemoryError ex) {
            logwrapr.severe("OutOfMemoryError error detected.", ex);
         }        
        finally{
            MiscUtil.close(loRS);
        }
    }
}
