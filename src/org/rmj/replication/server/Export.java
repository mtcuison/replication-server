/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
//test: ok
package org.rmj.replication.server;

import com.google.gson.stream.JsonWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.UserPrincipal;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang3.StringUtils;
import org.json.simple.JSONObject;
import org.rmj.appdriver.GCrypt;
import org.rmj.appdriver.GProperty;
import org.rmj.appdriver.MiscUtil;
import org.rmj.appdriver.SQLUtil;
import org.rmj.lib.net.LogWrapper;
import org.rmj.lib.net.MiscReplUtil;
import org.rmj.lib.net.SFTP_DU;
import org.rmj.appdriver.agent.GRiderX;

public class Export {
    private static String SIGNATURE = "08220326";
    private static LogWrapper logwrapr = new LogWrapper("Server.Export", "Export.log");
    private static String local_dir = null;
    private static GRiderX instance = null;
    private static Connection app_con = null;
    private static JSONObject json_obj = null;
    private static Timestamp dCreatedx = null;

    //added for ftp_upload
    private static SFTP_DU sftp;
    private static String sftpx_dir = null;
    private static boolean use_ftp=false;
    
    public static void main(String[] args) {
        ExportServer sds = null;  
        
        try {
            Socket clientSocket = new Socket("localhost", ExportServer.PORT);
            System.out.println("*** Already running!" + ExportServer.PORT);
            System.exit(1);
        }
        catch (Exception e) {
            sds = new ExportServer();
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
        
        ResultSet rsBranch = null;
        String batchno = "";
        boolean bErr = false;

        System.out.println(System.getProperty("user.dir"));

        instance = new GRiderX("gRider");
        instance.setOnline(false);
        
        System.out.println(instance.getBranchCode());
        System.out.println(instance.getClientID());
        
        //create connection to the dbf server
        if(!connect_server()){
           sds.interrupt();
           return;
        }
        
        if(!prepareSFTPHost()) {
           sds.interrupt();
           return;
        }
        
        
        
        //initialize record of each branch needed to be exported
        batchno = init_export(); 
        if(batchno.equals("")) {
           sds.interrupt();
           return;
        }
        
        //Get list of branches that are authorized to import files
        rsBranch = extract_branch();
        if(rsBranch == null) {
           sds.interrupt();
           return;
        }
        
        try {
            
            //instance.beginTrans();
            
            //While (!branches.eof)
            while(rsBranch.next()){
               //Create file from central dbf for export by branch(main=all;not main=needed by that branch only)
                json_obj = new JSONObject();
                
                json_obj.put("sBatchNox", batchno);
                json_obj.put("sBranchCD", rsBranch.getString("sBranchCD"));
                
                if(!isExported(rsBranch.getString("sBranchCD"), batchno)){
                   if(!create_export( 
                        rsBranch.getString("sBranchCD"),
                        rsBranch.getString("cMainOffc"),
                        rsBranch.getString("cFTPSRepl"))){
                       bErr = true;
                       break;
                   }
                   else{

                       String lsSQL = "UPDATE xxxOutGoingDetail" + 
                                     " SET sFileName = " + SQLUtil.toSQL((String) json_obj.get("sFileName")) +
                                        ", sMD5Hashx = " + SQLUtil.toSQL((String) json_obj.get("sMD5Hashx")) + 
                                        ", sFileSize = " + SQLUtil.toSQL((String) json_obj.get("sFileSize")) + 
                                        ", cRecdStat = '1'" + 
                                     " WHERE sBatchNox = " + SQLUtil.toSQL(batchno) + 
                                       " AND sBranchCD = " + SQLUtil.toSQL(rsBranch.getString("sBranchCD"));
                       instance.getConnection().createStatement().executeUpdate(lsSQL);
                   }
                }
            }
            
            if(!bErr){
               String lsSQL = "UPDATE xxxOutGoingMaster" + 
                             " SET cTranStat = '1'" + 
                             " WHERE sBatchNox = " + SQLUtil.toSQL(batchno);
               System.out.println(lsSQL);
               instance.getConnection().createStatement().executeUpdate(lsSQL);
            }
        } catch (SQLException ex) {
            logwrapr.severe("IOException error detected.", ex);
            bErr = true;
        }
        
//        if(bErr)
//            instance.rollbackTrans();
//        else
//            instance.commitTrans();
     
        MiscUtil.close(app_con);
        
        System.out.println("Stopping Thread: " + Calendar.getInstance().getTime());
        sds.interrupt();
        System.out.println("Thread Stopped: " + Calendar.getInstance().getTime());
        System.exit(0);        
    }
    
    //create a reference for export
//    private static String init_export(){
//        String sBatchNox = "";
//
//        ResultSet loRS = null;
//
//        try {
//            //kalyptus - 2016.05.31 09:30am
//            //include xxxOutGoingMaster->dCreatedx in our query
//            //kalyptus - 2017.06.12 09:18am
//            //include a LIMIT 1 in the query...
//            String lsSQL = "SELECT a.sBranchCD, b.sLogThrux, c.sBatchNox, IFNULL(c.cTranStat, '1') cTranStat, c.dCreatedx" + 
//                          " FROM Branch a" + 
//                            " LEFT JOIN xxxOutGoingDetail b ON a.sBranchCD = b.sBranchCD" + 
//                            " LEFT JOIN xxxOutGoingMaster c ON b.sBatchNox = c.sBatchNox" +
//                                                         " AND c.cTranStat IN ('0', '1')" + 
//                          " WHERE (b.sBranchCd IS NOT NULL AND c.sBatchNox = " + SQLUtil.toSQL(getLastBatch()) + ")" + 
//                             " OR  (b.sBranchCD IS NULL AND c.sBatchNox IS NULL)" +
//                          " GROUP BY c.sBatchNox, a.sBranchCD" +                         
//                          " ORDER BY c.dCreatedx DESC";
//            
//            System.out.println(lsSQL);
//            //inform beginning of transaction
//            instance.beginTrans();
//            
//            //extract previous export record
//            loRS = instance.getConnection().createStatement().executeQuery(lsSQL);
//            
//            //let us assume that there will always be a record
//            if(loRS.next()){
//                System.out.println("Checking Row #1: " + loRS.getRow());
//                if(loRS.getString("cTranStat").equalsIgnoreCase("0")){
//                    sBatchNox = loRS.getString("sBatchNox");
//                    //kalyptus - 2016.05.31 09:30am
//                    //should have to save the date created to dCreatedx since it is needed during the 
//                    //extraction of detail...
//                    dCreatedx = loRS.getTimestamp("dCreatedx");
//                    
//                    instance.commitTrans();
//                    return sBatchNox;
//                }
//            }
//            System.out.println("Checking Row #2: " + loRS.getRow());
//            //create batch information
//            sBatchNox = MiscUtil.getNextCode("xxxOutGoingMaster", "sBatchNox", true, instance.getConnection(), instance.getBranchCode());
//            dCreatedx = instance.getServerDate();
//            
//            System.out.println("Checking Row #3: " + loRS.getRow());
//
//            lsSQL = "INSERT INTO xxxOutGoingMaster" + 
//                   " SET sBatchNox = " + SQLUtil.toSQL(sBatchNox) +
//                      ", dCreatedx = " + SQLUtil.toSQL(dCreatedx) + 
//                      ", cTranStat = '0'";
//            instance.getConnection().createStatement().executeUpdate(lsSQL);
//            
//            System.out.println("Checking Row #4: " + loRS.getRow());
//            
//            do {
//                System.out.println("Processing Row: " + loRS.getRow());
//                String sLogThrux = getLastLog(loRS.getString("sBranchCD"));
//                String sLogFromx;
//                
//                System.out.println("sLogThrux: " + sLogThrux); 
//                
//                if(loRS.getString("sLogThrux") == null || Long.valueOf(loRS.getString("sLogThrux").substring(4)) == 0){
//                   if(sLogThrux.equals("")) 
//                      sLogFromx = "";
//                   else
//                      sLogFromx = loRS.getString("sBranchCD") + StringUtils.leftPad("1", sLogThrux.length() - loRS.getString("sBranchCD").length() , "0");
//                }
//                else{
//                    sLogFromx = loRS.getString("sBranchCD") + StringUtils.leftPad(String.valueOf(Long.valueOf(loRS.getString("sLogThrux").substring(4)) + 1), loRS.getMetaData().getPrecision(1) - loRS.getString("sBranchCD").length() , "0");
//                    if(loRS.getString("sBranchCD").equals("M053")){
//                        System.out.println("From:" + sLogFromx);
//                        System.out.println("Thru:" + sLogThrux);
//                        System.out.println("Compare: sLogFromx.compareTo(sLogThrux) = " + sLogFromx.compareTo(sLogThrux) );
//                    }
//                    
//                    if(Long.valueOf(sLogFromx.substring(4)) > Long.valueOf(sLogThrux.substring(4)))
//                       sLogFromx = sLogThrux;
//                    
////                    if(sLogFromx.compareTo(sLogThrux) > 0)
////                        sLogFromx = sLogThrux;
//                    
//                     if(loRS.getString("sBranchCD").equals("M053")){
//                        System.out.println("From:" + sLogFromx);
//                        System.out.println("Thru:" + sLogThrux);
//                    }
//                   
//                }
//                String lsSQLDtl = "INSERT INTO xxxOutGoingDetail" + 
//                                 " SET sBatchNox = " + SQLUtil.toSQL(sBatchNox) + 
//                                    ", sBranchCD = " + SQLUtil.toSQL(loRS.getString("sBranchCD")) + 
//                                    ", sLogFromx = " + SQLUtil.toSQL(sLogFromx) + 
//                                    ", sLogThrux = " + SQLUtil.toSQL(sLogThrux) + 
//                                    ", sFileName = ''" + 
//                                    ", sMD5Hashx = ''" +  
//                                    ", cRecdStat = '0'"; 
//                                    
//                instance.getConnection().createStatement().executeUpdate(lsSQLDtl);
//            } while(loRS.next());
//            
//        } catch (SQLException ex) {
//            logwrapr.severe("SQLException error detected.", ex);
//            sBatchNox = "";
//        }
//        catch (OutOfMemoryError ex) {
//            logwrapr.severe("OutOfMemoryError error detected.", ex);
//            sBatchNox = "";
//         }        
//        finally{
//            MiscUtil.close(loRS);
//        }
//        
//        if(!sBatchNox.equals(""))
//            instance.commitTrans();
//        else{
//            sBatchNox = "";
//            instance.rollbackTrans();
//        }
//        return sBatchNox;
//    }

    //kalyptus - 2019.01.02 05:27pm
    //replace the logic of export here...
    private static String init_export(){
        String sBatchNox = "";

        ResultSet loRS = null;

        try {
            //kalyptus - 2016.05.31 09:30am
            //include xxxOutGoingMaster->dCreatedx in our query
            //kalyptus - 2017.06.12 09:18am
            //include a LIMIT 1 in the query...
            String lsSQL = "SELECT *" +
                          " FROM xxxOutGoingMaster" + 
                          " WHERE sBatchNox = " + SQLUtil.toSQL(getLastBatch());
            loRS = instance.getConnection().createStatement().executeQuery(lsSQL);
            if(loRS.next()){
                System.out.println("Checking Row #1: " + loRS.getRow());
                if(loRS.getString("cTranStat").equalsIgnoreCase("0")){
                    sBatchNox = loRS.getString("sBatchNox");
                    //kalyptus - 2016.05.31 09:30am
                    //should have to save the date created to dCreatedx since it is needed during the 
                    //extraction of detail...
                    dCreatedx = loRS.getTimestamp("dCreatedx");
                    
                    instance.commitTrans();
                    return sBatchNox;
                }
            }
            //Note: In case of reactivating branch, please make sure to initialize the 
            //      beginning of exportation at xxxOutgoingDetail.
            lsSQL = "SELECT a.sBranchCD, b.sLogThrux, b.sBatchNox" + 
                   " FROM Branch a" + 
                            " LEFT JOIN xxxOutGoingDetail b ON a.sBranchCD = b.sBranchCD AND b.sBatchNox = " + SQLUtil.toSQL(getLastBatch())  +
                   " WHERE a.cRecdStat = '1'";
                    
            System.out.println(lsSQL);
            //inform beginning of transaction
            instance.beginTrans();
            
            //extract previous export record
            loRS = instance.getConnection().createStatement().executeQuery(lsSQL);
            
            //create batch information
            sBatchNox = MiscUtil.getNextCode("xxxOutGoingMaster", "sBatchNox", true, instance.getConnection(), instance.getBranchCode());
            dCreatedx = instance.getServerDate();
            
            System.out.println("Checking Row #3: " + loRS.getRow());

            lsSQL = "INSERT INTO xxxOutGoingMaster" + 
                   " SET sBatchNox = " + SQLUtil.toSQL(sBatchNox) +
                      ", dCreatedx = " + SQLUtil.toSQL(dCreatedx) + 
                      ", cTranStat = '0'";
            instance.getConnection().createStatement().executeUpdate(lsSQL);
                
            System.out.println("Checking Row #4: " + loRS.getRow());
            
            while(loRS.next()) {
                System.out.println("Processing Row: " + loRS.getRow());
                String sLogThrux = getLastLog(loRS.getString("sBranchCD"));
                String sLogFromx;
                
                System.out.println("sLogThrux: " + sLogThrux); 
                
                if(loRS.getString("sLogThrux") == null || Long.valueOf(loRS.getString("sLogThrux").substring(4)) == 0){
                   if(sLogThrux.equals("")) 
                      sLogFromx = "";
                   else
                      sLogFromx = loRS.getString("sBranchCD") + StringUtils.leftPad("1", sLogThrux.length() - loRS.getString("sBranchCD").length() , "0");
                }
                else{
                    sLogFromx = loRS.getString("sBranchCD") + StringUtils.leftPad(String.valueOf(Long.valueOf(loRS.getString("sLogThrux").substring(4)) + 1), loRS.getMetaData().getPrecision(1) - loRS.getString("sBranchCD").length() , "0");
                    if(loRS.getString("sBranchCD").equals("M053")){
                        System.out.println("From:" + sLogFromx);
                        System.out.println("Thru:" + sLogThrux);
                        System.out.println("Compare: sLogFromx.compareTo(sLogThrux) = " + sLogFromx.compareTo(sLogThrux) );
                    }
                    
                    if(Long.valueOf(sLogFromx.substring(4)) > Long.valueOf(sLogThrux.substring(4)))
                       sLogFromx = sLogThrux;
                    
//                    if(sLogFromx.compareTo(sLogThrux) > 0)
//                        sLogFromx = sLogThrux;
                    
                     if(loRS.getString("sBranchCD").equals("M053")){
                        System.out.println("From:" + sLogFromx);
                        System.out.println("Thru:" + sLogThrux);
                    }
                   
                }
                String lsSQLDtl = "INSERT INTO xxxOutGoingDetail" + 
                                 " SET sBatchNox = " + SQLUtil.toSQL(sBatchNox) + 
                                    ", sBranchCD = " + SQLUtil.toSQL(loRS.getString("sBranchCD")) + 
                                    ", sLogFromx = " + SQLUtil.toSQL(sLogFromx) + 
                                    ", sLogThrux = " + SQLUtil.toSQL(sLogThrux) + 
                                    ", sFileName = ''" + 
                                    ", sMD5Hashx = ''" +  
                                    ", cRecdStat = '0'"; 
                                    
                instance.getConnection().createStatement().executeUpdate(lsSQLDtl);
            } //while(loRS.next())
            
        } catch (SQLException ex) {
            logwrapr.severe("SQLException error detected.", ex);
            sBatchNox = "";
        }
        catch (OutOfMemoryError ex) {
            logwrapr.severe("OutOfMemoryError error detected.", ex);
            sBatchNox = "";
         }        
        finally{
            MiscUtil.close(loRS);
        }
        
        if(!sBatchNox.equals(""))
            instance.commitTrans();
        else{
            sBatchNox = "";
            instance.rollbackTrans();
        }
        return sBatchNox;
    }
    
    //should determine start and end of transaction number for each branch
    private static ResultSet extract_branch(){
        ResultSet rs = null;
        try {
            String lsSQL = "SELECT a.sBranchCD, a.cMainOffc, b.cFTPSRepl" + 
                          " FROM Branch a" + 
                             " LEFT JOIN Branch_Others b ON a.sBranchCD = b.sBranchCD" + 
                          " WHERE a.cRecdStat = '1'" + 
                            " AND b.cFTPSRepl <> '0'" +  
                          " ORDER BY cMainOffc DESC, sBranchCD" ;
            
            //create statement to be use in executing queries
            rs = instance.getConnection().createStatement().executeQuery(lsSQL);
            System.out.println(rs.getRow());
        } 
        catch (SQLException ex) {
            logwrapr.severe("IOException error detected.", ex);
        }
        catch (OutOfMemoryError ex) {
            logwrapr.severe("OutOfMemoryError error detected.", ex);
         }                
        return rs;
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
    
//    private static boolean write_file(ResultSet rs, String filename){
//        boolean bErr = false;
//        try {
//            String branch = filename.substring(filename.length() - 4).toUpperCase();
//            System.out.println("Filename: " + filename);
//            String destpath = local_dir + "download/unzipped/" + branch + "/";
//            File destpth = new File(destpath);
//            if (!destpth.exists()) {
//                destpth.mkdirs();
//                sftp.mkdir(sftpx_dir + "download/zipped/" + branch + "/");
//            }           
//
//            String uploadpath = local_dir + "upload/zipped/" + branch + "/";
//            File uploadpth = new File(uploadpath);
//            if (!uploadpth.exists()) {
//                uploadpth.mkdirs();
//                sftp.mkdir(sftpx_dir + "upload/zipped/" + branch + "/");
//            }        
//
//            OutputStream out = new FileOutputStream(local_dir + "download/unzipped/" + branch + "/" + filename + ".json");
//            JsonWriter writer = new JsonWriter(new OutputStreamWriter(out, "UTF-8"));
//
//            LinkedList<Map<String, String>> link = RStoLinkList(rs);
//            
//            writer.beginArray();
//            
//            //SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//            SimpleDateFormat sf = new SimpleDateFormat("MMM d, yyyy K:m:s");
//            
//            for(Map map : link){
//               writer.beginObject();
//               writer.name("sTransNox").value((String)map.get("sTransNox"));
//               writer.name("sBranchCd").value((String)map.get("sBranchCd"));
//               writer.name("sStatemnt").value((String)map.get("sStatemnt"));
//               writer.name("sTableNme").value((String)map.get("sTableNme"));
//               writer.name("sDestinat").value((String)map.get("sDestinat"));
//               writer.name("sModified").value((String)map.get("sModified"));
//               writer.name("dEntryDte").value(sf.format((Date)map.get("dEntryDte")));
//               writer.name("dModified").value(sf.format((Date)map.get("dModified")));
//               writer.endObject();
//            }
//            
//            writer.endArray();
//            writer.close();
//            
//        } catch(IOException ex){
//            logwrapr.severe("IOException error detected.", ex);
//            bErr = true;
//        } catch (SQLException ex) {
//            logwrapr.severe("SQLException error detected.", ex);
//            bErr = true;
//        }
//        
//        return !bErr;
//    }    
    
    
    private static boolean write_file(ResultSet rs, String filename){
        boolean bErr = false;
        
        try {
            String branch = filename.substring(filename.length() - 4).toUpperCase();
            System.out.println("Filename: " + filename);
            String destpath = local_dir + "download/unzipped/" + branch + "/";
            File destpth = new File(destpath);
            if (!destpth.exists()) {
                destpth.mkdirs();
                setOwner(destpath, "greplusr");
                sftp.mkdir(sftpx_dir + "download/zipped/" + branch + "/");
            }           

            String uploadpath = local_dir + "upload/zipped/" + branch + "/";
            File uploadpth = new File(uploadpath);
            if (!uploadpth.exists()) {
                uploadpth.mkdirs();
                setOwner(uploadpath, "greplusr");
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
            
//            for(Map map : link){
//               writer.beginObject();
//               writer.name("sTransNox").value((String)map.get("sTransNox"));
//               writer.name("sBranchCd").value((String)map.get("sBranchCd"));
//               writer.name("sStatemnt").value((String)map.get("sStatemnt"));
//               writer.name("sTableNme").value((String)map.get("sTableNme"));
//               writer.name("sDestinat").value((String)map.get("sDestinat"));
//               writer.name("sModified").value((String)map.get("sModified"));
//               writer.name("dEntryDte").value(sf.format((Date)map.get("dEntryDte")));
//               writer.name("dModified").value(sf.format((Date)map.get("dModified")));
//               writer.endObject();
//            }
            
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
    
    
//    private static boolean write_file(ResultSet rs, String filename){
//        boolean bErr = false;
//        try {
//            System.out.println("Filename: " + filename);
//            String destpath = host_dir + "download/unzipped/" + filename.substring(filename.length() - 4) + "/";
//            File destpth = new File(destpath);
//            if (!destpth.exists()) {
//                destpth.mkdirs();
//            }           
//
//            String uploadpath = host_dir + "upload/zipped/" + filename.substring(filename.length() - 4) + "/";
//            File uploadpth = new File(uploadpath);
//            if (!uploadpth.exists()) {
//                uploadpth.mkdirs();
//            }        
//            
//            OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(host_dir + "download/unzipped/" + filename.substring(filename.length() - 4) + "/" + filename + ".json"),"UTF-8");
//            out.write(MiscReplUtil.RStoJSON(rs));
//            out.flush();
//            out.close();
//            
////            FileWriter file = new FileWriter(host_dir + "download/" + filename.substring(filename.length() - 4) + "/unzipped/" + filename + ".json");
////            file.write(MiscReplUtil.RStoJSON(rs));
////            file.flush();
////            file.close();                
//        } catch(IOException ex){
//            logwrapr.severe("IOException error detected.", ex);
//            bErr = true;
//        } catch (SQLException ex) {
//            logwrapr.severe("SQLException error detected.", ex);
//            bErr = true;
//        }
//        
//        return !bErr;
//    }
    
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

    private static String getLastLog(String branch){
        boolean bErr = false;
        ResultSet rs = null;
        String lastlog = "";
        try {
            String lsSQL = "SELECT sTransNox" + 
                          " FROM xxxReplicationLog" + 
                          " WHERE sTransNox LIKE " + SQLUtil.toSQL(branch + "%") + 
                          " ORDER BY sTransNox DESC LIMIT 1";
                    
            rs = app_con.createStatement().executeQuery(lsSQL);
            
            if(rs.next())
                lastlog = rs.getString("sTransNox");
            else
                lastlog = branch + "0000000000";
            
        } catch (SQLException ex) {
            logwrapr.severe("SQLException error detected.", ex);
            bErr = true;
        } catch (OutOfMemoryError ex) {
            logwrapr.severe("OutOfMemoryError error detected.", ex);
            bErr = true;
        } finally{
            MiscUtil.close(rs);
        }
        
        rs = null;
        
        if(bErr)
            return "";
        else
            return lastlog;
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
//            local_dir = loProp.getConfig(instance.getProductID() + "-sftpfldr");
            
        }catch(Exception ex){
            logwrapr.severe("Exception error detected.", ex);
            bErr = true;
        }catch (OutOfMemoryError ex) {
            logwrapr.severe("OutOfMemoryError error detected.", ex);
            bErr = true;
        } 

        return !bErr;
    }

    private  static String getLastBatch(){
        String batch = "";
        try {
            String lsQry = "SELECT sBatchNox" +
                          " FROM xxxOutGoingMaster" +
                          " ORDER BY sBatchNox DESC" +
                          " LIMIT 1";
            ResultSet lrs = instance.getConnection().createStatement().executeQuery(lsQry);

            if(lrs.next())
                batch = lrs.getString("sBatchNox");
            
            lrs = null;
            return batch;
        } catch (SQLException ex) {
            logwrapr.severe("SQLException error detected.", ex);
        }catch (OutOfMemoryError ex) {
            logwrapr.severe("OutOfMemoryError error detected.", ex);
        } 
        
        return batch;
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

    private static boolean isExported(String sBranchCD, String sBatchNox){
       String lsSQL;
       boolean lbexport = false;
        try {
           lsSQL = "SELECT sFileName" +
                  " FROM xxxOutGoingDetail" +
                  " WHERE sBatchNox = " + SQLUtil.toSQL(sBatchNox) + 
                    " AND sBranchCD = " + SQLUtil.toSQL(sBranchCD);
            ResultSet lrs = instance.getConnection().createStatement().executeQuery(lsSQL);

            if(lrs.next())
                if(!lrs.getString("sFileName").isEmpty()) 
                   lbexport = true;
            
            lrs = null;
        } catch (SQLException ex) {
            logwrapr.severe("SQLException error detected.", ex);
        } catch (OutOfMemoryError ex) {
            logwrapr.severe("OutOfMemoryError error detected.", ex);
        } 
        
        return lbexport;
    }
    
    //kalyptus - 2019.11.27 10:22am
    //set the owner of the file/directory
    private static boolean setOwner(String xpath, String owner){
        try {
            Path path = Paths.get(xpath);
            FileSystem fileSystem = path.getFileSystem();
            UserPrincipalLookupService service = fileSystem.getUserPrincipalLookupService();
            UserPrincipal userPrincipal = service.lookupPrincipalByName(owner);
            Files.setOwner(path, userPrincipal);
            return true;
        } catch (IOException ex) {
            ex.printStackTrace();
            return false;
        }
    }
}
