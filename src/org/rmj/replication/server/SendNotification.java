/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.rmj.replication.server;

import java.io.IOException;
import org.json.simple.JSONObject;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.rmj.appdriver.GRider;
import org.rmj.appdriver.MiscUtil;
import org.rmj.appdriver.SQLUtil;
import org.rmj.appdriver.agent.MsgBox;
import org.rmj.lib.net.LogWrapper;
import org.rmj.lib.net.WebClient;
//import static org.rmj.lib.net.WebClient.httpsPostJSon;

/**
 *
 * @author sayso
 */
public class SendNotification {
    private static GRider instance = null;
//    private static LogWrapper logwrapr = new LogWrapper("NMM.SendNotification", "SendNotification.log");
    private static final String FMC_URL_SEND = "https://fcm.googleapis.com/fcm/send";
    
    public static void main(String[] args) {
        //System.setProperty("sys.default.path.config", "D:/GMC_SEG_Folder_PHP/eclipse-php-workspace/GGC_Java_Systems");
        //System.setProperty("sys.default.path.config", "/srv/www/GGC_Java_Systems");

        String path;
        if(System.getProperty("os.name").toLowerCase().contains("win")){
            path = "D:/GMC_SEG_Folder_PHP/eclipse-php-workspace/GGC_Java_Systems";
        }
        else{
            path = "/srv/www/GGC_Java_Systems";
        }
        System.setProperty("sys.default.path.config", path);
        
        instance = new GRider("gRider");
        instance.setOnline(true);
        
        if(instance.getErrMsg().length() > 0){
            //logwrapr.severe(instance.getErrMsg());
            System.exit(1);
        }

        if(args.length == 0){
            sendRequest();
            System.exit(0);
        }
        
        String method = args[0];
        String transno = args[1];
        
        if(method.equalsIgnoreCase("request")){
            sendRequest(transno);
            System.exit(0);
        }
        else{
            if(args.length != 5){
                //logwrapr.severe("Invalid number of parameters detected");
                System.exit(1);
            }
            
            String rcpt_app = args[2];
            String rcpt_usr = args[3];
            String imei = args[4];
            if(updateStatus(transno, rcpt_app, rcpt_usr, imei))
            {
                System.exit(0);
            }
            else{
                System.exit(1);
            }
        }
    }
    
    private static void sendRequest(){
        try {
            //Notify other registered devices of sender...
            String lsSQL;
            lsSQL = "SELECT d.sParentxx, d.sMsgTitle, d.sMessagex, d.sImageURL, d.sMsgTypex, d.sDataSndx, d.dCreatedx, b.sAuthKeyx, c.sTokenIDx, a.*, e.sUserName" +
                    " FROM NMM_Request_Sender a" +
                    " LEFT JOIN xxxSysAuth b ON a.sAppSrcex = b.sProdctID" +
                    " LEFT JOIN App_User_Device c ON a.sCreatedx = c.sUserIDxx AND a.sAppSrcex = c.sProdctID AND a.sIMEINoxx = c.sIMEINoxx" +
                    " LEFT JOIN NMM_Request_Master d ON a.sTransNox = d.sTransNox" +
                    " LEFT JOIN App_User_Master e ON a.sCreatedx = e.sUserIDxx" +
                    " WHERE a.cSentxxxx = '0'";
            
            ResultSet loRSSender;
            System.out.println(lsSQL);
            loRSSender = instance.executeQuery(lsSQL);

            instance.beginTrans();
            
            while(loRSSender.next()){
                JSONObject data = new JSONObject();
                data.put("status", "0");
                data.put("transno", loRSSender.getString("sTransNox"));
                data.put("parent", loRSSender.getString("sParentxx"));
                data.put("stamp", SQLUtil.dateFormat(loRSSender.getDate("dCreatedx"), SQLUtil.FORMAT_TIMESTAMP));
                data.put("appsrce", loRSSender.getString("sAppSrcex"));
                data.put("srceid", loRSSender.getString("sCreatedx"));
                data.put("srcenm", loRSSender.getString("sUserName"));
                data.put("apprcpt", loRSSender.getString("sAppSrcex"));
                data.put("rcptid", loRSSender.getString("sCreatedx"));
                data.put("rcptnm", loRSSender.getString("sUserName"));
                data.put("msgmon", loRSSender.getString("sMsgTypex"));
                data.put("infox", loRSSender.getString("sDataSndx"));

                JSONObject msg = new JSONObject();
                msg.put("title", loRSSender.getString("sMsgTitle"));
                msg.put("message", loRSSender.getString("sMessagex"));
                msg.put("imgurl", loRSSender.getString("sImageURL"));
                msg.put("msg_data", data);

                JSONObject response = sendMessage(loRSSender.getString("sAuthKeyx"), loRSSender.getString("sTokenIDx"), "high", msg);

                String result = (String) response.get("result");
                if(result.equalsIgnoreCase("success")){
                    //{"result":"success","canonical_ids":0,"success":1,"failure":0,"results":[{"message_id":"0:1563345836225955%f04818c7f9fd7ecd"}],"multicast_id":4913388342359659600}
                    long success = (Long) response.get("success");
                    String stamp = SQLUtil.dateFormat(Calendar.getInstance().getTime(), SQLUtil.FORMAT_TIMESTAMP);
                    lsSQL = "UPDATE NMM_Request_Sender" + 
                           " SET cSentxxxx = " + SQLUtil.toSQL(success == 1 ? "1" : "6") + 
                              ", dSentxxxx = " + SQLUtil.toSQL(stamp) +
                           " WHERE sTransNox = " + SQLUtil.toSQL(loRSSender.getString("sTransNox")) +
                             " AND sAppSrcex = " + SQLUtil.toSQL(loRSSender.getString("sAppSrcex")) + 
                             " AND sCreatedx = " + SQLUtil.toSQL(loRSSender.getString("sCreatedx")) +
                             " AND sIMEINoxx = " + SQLUtil.toSQL(loRSSender.getString("sIMEINoxx"));
                    System.out.println(lsSQL);
                    instance.executeUpdate(lsSQL);
                }
            }
            
            //Notify the recepients
            lsSQL = "SELECT e.sAppSrcex, e.sCreatedx, f.sUserName, e.sParentxx, e.sMsgTitle, e.sMessagex, e.sImageURL, e.sMsgTypex, e.sDataSndx, e.dCreatedx, b.sAuthKeyx, c.sTokenIDx, d.sUserName sRcptName, a.*" +
                   " FROM NMM_Request_Recepient a" +
                        " LEFT JOIN xxxSysAuth b ON a.sAppRcptx = b.sProdctID" +
                        " LEFT JOIN App_User_Device c ON a.sRecpntxx = c.sUserIDxx AND a.sAppRcptx = c.sProdctID AND a.sIMEINoxx = c.sIMEINoxx" +
                        " LEFT JOIN App_User_Master d ON c.sUserIDxx = d.sUserIDxx" +
                        " LEFT JOIN NMM_Request_Master e ON a.sTransNox = e.sTransNox" +
                        " LEFT JOIN App_User_Master f ON e.sCreatedx = f.sUserIDxx" +
                   " WHERE a.cMesgStat = '0'";
            ResultSet loRSRcpt;
            System.out.println(lsSQL);
            loRSRcpt = instance.executeQuery(lsSQL);
            
            while(loRSRcpt.next()){
                JSONObject data = new JSONObject();
                data.put("status", "0");
                data.put("transno", loRSRcpt.getString("sTransNox"));
                data.put("parent", loRSRcpt.getString("sParentxx"));
                data.put("stamp", SQLUtil.dateFormat(loRSRcpt.getDate("dCreatedx"), SQLUtil.FORMAT_TIMESTAMP));
                data.put("appsrce", loRSRcpt.getString("sAppSrcex"));
                data.put("srceid", loRSRcpt.getString("sCreatedx"));
                data.put("srcenm", loRSRcpt.getString("sUserName"));
                data.put("apprcpt", loRSRcpt.getString("sAppRcptx"));
                data.put("rcptid", loRSRcpt.getString("sRecpntxx"));
                data.put("rcptnm", loRSRcpt.getString("sUserName"));
                data.put("msgmon", loRSRcpt.getString("sMsgTypex"));
                data.put("infox", loRSRcpt.getString("sDataSndx"));

                JSONObject msg = new JSONObject();
                msg.put("title", loRSRcpt.getString("sMsgTitle"));
                msg.put("message", loRSRcpt.getString("sMessagex"));
                msg.put("imgurl", loRSRcpt.getString("sImageURL"));
                msg.put("msg_data", data);

                JSONObject response = sendMessage(loRSRcpt.getString("sAuthKeyx"), loRSRcpt.getString("sTokenIDx"), "high", msg);
                String result = (String) response.get("result");
                
                System.out.println(response.toJSONString());
                if(result.equalsIgnoreCase("success")){
                    //{"result":"success","canonical_ids":0,"success":1,"failure":0,"results":[{"message_id":"0:1563345836225955%f04818c7f9fd7ecd"}],"multicast_id":4913388342359659600}
                    //MsgBox.showOk();

                    long success = (Long) response.get("success");
                    String stamp = SQLUtil.dateFormat(Calendar.getInstance().getTime(), SQLUtil.FORMAT_TIMESTAMP);
                    lsSQL = "UPDATE NMM_Request_Recepient" + 
                           " SET cMesgStat = " + SQLUtil.toSQL(success == 1 ? "1" : "6") + 
                              ", dSentxxxx = " + SQLUtil.toSQL(stamp) +
                              ", dLastUpdt = " + SQLUtil.toSQL(stamp) +
                           " WHERE sTransNox = " + SQLUtil.toSQL(loRSRcpt.getString("sTransNox")) +
                             " AND sAppRcptx = " + SQLUtil.toSQL(loRSRcpt.getString("sAppRcptx")) + 
                             " AND sRecpntxx = " + SQLUtil.toSQL(loRSRcpt.getString("sRecpntxx")) +
                             " AND sIMEINoxx = " + SQLUtil.toSQL(loRSRcpt.getString("sIMEINoxx"));
                    System.out.println(lsSQL);
                    instance.executeUpdate(lsSQL);
                }
            }
            
            System.out.println("Before commit");
            instance.commitTrans();
            System.out.println("After Commit");
            
        } catch (SQLException ex) {
            ex.printStackTrace();
            //logwrapr.severe(ex.getMessage());
            instance.rollbackTrans();
        }
    }
    
    private static boolean sendRequest(String transno){
        try {
            String lsSQL;
            
            //Load the information regarding the message...
            lsSQL = "SELECT IFNULL(b.sUserName, c.sSysMonDs) sUserName, a.*" +
                    " FROM NMM_Request_Master a" +
                        " LEFT JOIN App_User_Master b ON a.sCreatedx = b.sUserIDxx" +
                        " LEFT JOIN NMM_SysMon_List c ON a.sCreatedx = c.sSysMonID" +
                    " WHERE a.sTransNox = " + SQLUtil.toSQL(transno);
            ResultSet loRSMaster;
            loRSMaster = instance.executeQuery(lsSQL);

            if(!loRSMaster.next()){
                return false;
            }

            //usually there are no sender if notification came from the system...
            if(!loRSMaster.getString("sAppSrcex").equalsIgnoreCase("SYSTEM")){
                //load the list of other devices used by sender
                lsSQL = "SELECT b.sAuthKeyx, c.sTokenIDx, a.*" +
                       " FROM NMM_Request_Sender a" +
                            " LEFT JOIN xxxSysAuth b ON a.sAppSrcex = b.sProdctID" +
                            " LEFT JOIN App_User_Device c ON a.sCreatedx = c.sUserIDxx AND a.sAppSrcex = c.sProdctID AND a.sIMEINoxx = c.sIMEINoxx" +
                       " WHERE a.sTransNox = " + SQLUtil.toSQL(transno) +
                         " AND a.cSentxxxx = '0'";
                ResultSet loRSSender;
                loRSSender = instance.executeQuery(lsSQL);

                instance.beginTrans();

                while(loRSSender.next()){
                    JSONObject data = new JSONObject();
                    data.put("status", "0");
                    data.put("transno", loRSMaster.getString("sTransNox"));
                    data.put("parent", loRSMaster.getString("sParentxx"));
                    data.put("stamp", SQLUtil.dateFormat(loRSMaster.getDate("dCreatedx"), SQLUtil.FORMAT_TIMESTAMP));
                    data.put("appsrce", loRSMaster.getString("sAppSrcex"));
                    data.put("srceid", loRSMaster.getString("sCreatedx"));
                    data.put("srcenm", loRSMaster.getString("sUserName"));
                    data.put("apprcpt", loRSMaster.getString("sAppSrcex"));
                    data.put("rcptid", loRSMaster.getString("sCreatedx"));
                    data.put("rcptnm", loRSMaster.getString("sUserName"));
                    data.put("msgmon", loRSMaster.getString("sMsgTypex"));
                    data.put("infox", loRSMaster.getString("sDataSndx"));

                    JSONObject msg = new JSONObject();
                    msg.put("title", loRSMaster.getString("sMsgTitle"));
                    msg.put("message", loRSMaster.getString("sMessagex"));
                    msg.put("imgurl", loRSMaster.getString("sImageURL"));
                    msg.put("msg_data", data);

                    JSONObject response = sendMessage(loRSSender.getString("sAuthKeyx"), loRSSender.getString("sTokenIDx"), "high", msg);

                    String result = (String) response.get("result");
                    if(result.equalsIgnoreCase("success")){
                        //{"result":"success","canonical_ids":0,"success":1,"failure":0,"results":[{"message_id":"0:1563345836225955%f04818c7f9fd7ecd"}],"multicast_id":4913388342359659600}
                        String stamp = SQLUtil.dateFormat(Calendar.getInstance().getTime(), SQLUtil.FORMAT_TIMESTAMP);
                        long success = (Long) response.get("success");
                        lsSQL = "UPDATE NMM_Request_Sender" + 
                               " SET cSentxxxx = " + SQLUtil.toSQL(success == 1 ? "1" : "6") + 
                                  ", dSentxxxx = " + SQLUtil.toSQL(stamp) +
                               " WHERE sTransNox = " + SQLUtil.toSQL(loRSMaster.getString("sTransNox")) +
                                 " AND sAppSrcex = " + SQLUtil.toSQL(loRSSender.getString("sAppSrcex")) + 
                                 " AND sCreatedx = " + SQLUtil.toSQL(loRSSender.getString("sCreatedx")) +
                                 " AND sIMEINoxx = " + SQLUtil.toSQL(loRSSender.getString("sIMEINoxx"));
                        instance.executeUpdate(lsSQL);
                    }
                }
            }
            
            //load the list of of recepients
            lsSQL = "SELECT b.sAuthKeyx, c.sTokenIDx, d.sUserName, a.*" +
                   " FROM NMM_Request_Recepient a" +
                        " LEFT JOIN xxxSysAuth b ON a.sAppRcptx = b.sProdctID" + 
                        " LEFT JOIN App_User_Device c ON a.sRecpntxx = c.sUserIDxx AND a.sAppRcptx = c.sProdctID AND a.sIMEINoxx = c.sIMEINoxx" +
                        " LEFT JOIN App_User_Master d ON c.sUserIDxx = d.sUserIDxx" +
                   " WHERE a.sTransNox = " + SQLUtil.toSQL(transno) +
                     " AND a.cMesgStat = '0'";
            ResultSet loRSRcpt;
            loRSRcpt = instance.executeQuery(lsSQL);
            
            while(loRSRcpt.next()){
                JSONObject data = new JSONObject();
                data.put("status", "0");
                data.put("transno", loRSMaster.getString("sTransNox"));
                data.put("parent", loRSMaster.getString("sParentxx"));
                data.put("stamp", SQLUtil.dateFormat(loRSMaster.getDate("dCreatedx"), SQLUtil.FORMAT_TIMESTAMP));
                data.put("appsrce", loRSMaster.getString("sAppSrcex"));
                data.put("srceid", loRSMaster.getString("sCreatedx"));
                data.put("srcenm", loRSMaster.getString("sUserName"));
                data.put("apprcpt", loRSRcpt.getString("sAppRcptx"));
                data.put("rcptid", loRSRcpt.getString("sRecpntxx"));
                data.put("rcptnm", loRSRcpt.getString("sUserName"));
                data.put("msgmon", loRSMaster.getString("sMsgTypex"));
                data.put("infox", loRSMaster.getString("sDataSndx"));

                JSONObject msg = new JSONObject();
                msg.put("title", loRSMaster.getString("sMsgTitle"));
                msg.put("message", loRSMaster.getString("sMessagex"));
                msg.put("imgurl", loRSMaster.getString("sImageURL"));
                msg.put("msg_data", data);

                JSONObject response = sendMessage(loRSRcpt.getString("sAuthKeyx"), loRSRcpt.getString("sTokenIDx"), "high", msg);
                String result = (String) response.get("result");
                if(result.equalsIgnoreCase("success")){
                    //{"result":"success","canonical_ids":0,"success":1,"failure":0,"results":[{"message_id":"0:1563345836225955%f04818c7f9fd7ecd"}],"multicast_id":4913388342359659600}
                    long success = (Long) response.get("success");
                    String stamp = SQLUtil.dateFormat(Calendar.getInstance().getTime(), SQLUtil.FORMAT_TIMESTAMP);
                    
                    lsSQL = "UPDATE NMM_Request_Recepient" + 
                           " SET cMesgStat = " + SQLUtil.toSQL(success == 1 ? "1" : "6") + 
                              ", dSentxxxx = " + SQLUtil.toSQL(stamp) +
                              ", dLastUpdt = " + SQLUtil.toSQL(stamp) +
                           " WHERE sTransNox = " + SQLUtil.toSQL(loRSMaster.getString("sTransNox")) +
                             " AND sAppRcptx = " + SQLUtil.toSQL(loRSRcpt.getString("sAppRcptx")) + 
                             " AND sRecpntxx = " + SQLUtil.toSQL(loRSRcpt.getString("sRecpntxx")) +
                             " AND sIMEINoxx = " + SQLUtil.toSQL(loRSRcpt.getString("sIMEINoxx"));
                    instance.executeUpdate(lsSQL);
                }
            }

            instance.commitTrans();
            
            return true;
        } catch (SQLException ex) {
            //logwrapr.severe(ex.getMessage());
            instance.rollbackTrans();
            return false;
        }
    }   
    
    private static boolean updateStatus(String transno, String rcpt_app, String rcpt_user, String imei){
        try {
            String lsSQL;
            
            //Load the information regarding the RECEPIENTS sending the STATUS UPDATE...
            //Since NOTIFICATION of STATUS UPDATE is filtered in the PHP, assumed this STATUS UPDATE 
            //as the first STATUS UPDATE from the RECEPIENT
            lsSQL = "SELECT b.sUserName, a.*" +
                    " FROM NMM_Request_Recepient a" +
                        " LEFT JOIN App_User_Master b ON a.sRecpntxx = b.sUserIDxx" +
                    " WHERE a.sTransNox = " + SQLUtil.toSQL(transno) + 
                      " AND a.sAppRcptx = " + SQLUtil.toSQL(rcpt_app) + 
                      " AND a.sRecpntxx = " + SQLUtil.toSQL(rcpt_user) +
                      " AND a.sIMEINoxx = " + SQLUtil.toSQL(imei);
            ResultSet loRSMaster;
            loRSMaster = instance.executeQuery(lsSQL);
            
            if(!loRSMaster.next()){
                return false;
            }

            String status = loRSMaster.getString("cMesgStat");
            String stamp = SQLUtil.dateFormat(loRSMaster.getDate("dLastUpdt"), SQLUtil.FORMAT_TIMESTAMP);
            
            //load the list of other devices used by the ORIGINATOR OF THE MESSAGE
            lsSQL = "SELECT b.sAuthKeyx, c.sTokenIDx, d.sUserName, a.*" +
                   " FROM NMM_Request_Sender a" +
                        " LEFT JOIN xxxSysAuth b ON a.sAppSrcex = b.sProdctID" +
                        " LEFT JOIN App_User_Device c ON a.sCreatedx = c.sUserIDxx AND a.sAppSrcex = c.sProdctID AND a.sIMEINoxx = c.sIMEINoxx" +
                        " LEFT JOIN App_User_Master d ON c.sUserIDxx = d.sUserIDxx" +
                   " WHERE a.sTransNox = " + SQLUtil.toSQL(transno);
            ResultSet loRSSender;
            loRSSender = instance.executeQuery(lsSQL);

            instance.beginTrans();

            while(loRSSender.next()){
                JSONObject data = new JSONObject();
                data.put("status", status);
                data.put("transno", loRSMaster.getString("sTransNox"));
                data.put("stamp", stamp);
                data.put("appsrce", loRSMaster.getString("sAppRcptx"));
                data.put("srceid", loRSMaster.getString("sRecpntxx"));
                data.put("srcenm", loRSMaster.getString("sUserName"));
                data.put("apprcpt", loRSSender.getString("sAppSrcex"));
                data.put("rcptid", loRSSender.getString("sCreatedx"));
                data.put("rcptnm", loRSSender.getString("sUserName"));

                JSONObject msg = new JSONObject();
                msg.put("title", "Notification Update");
                msg.put("message", "");
                msg.put("imgurl", null);
                msg.put("msg_data", data);

                JSONObject response = sendMessage(loRSSender.getString("sAuthKeyx"), loRSSender.getString("sTokenIDx"), "high", msg);

                String result = (String) response.get("result");
                if(!result.equalsIgnoreCase("success")){
                    //logwrapr.severe("Error sending STATUS UPDATE:" + loRSMaster.getString("sTransNox") + "," + loRSSender.getString("sAppRcptx") + " " + response.toJSONString());
                    instance.rollbackTrans();
                    return false;
                }
            }
            
            //load the list of of recepients
            lsSQL = "SELECT b.sAuthKeyx, c.sTokenIDx, d.sUserName, a.*" +
                   " FROM NMM_Request_Recepient a" +
                        " LEFT JOIN xxxSysAuth b ON a.sAppRcptx = b.sProdctID" + 
                        " LEFT JOIN App_User_Device c ON a.sRecpntxx = c.sUserIDxx AND a.sAppRcptx = c.sProdctID AND a.sIMEINoxx = c.sIMEINoxx" +
                        " LEFT JOIN App_User_Master d ON c.sUserIDxx = d.sUserIDxx" +
                   " WHERE a.sTransNox = " + SQLUtil.toSQL(transno) +
                      " AND NOT (a.sAppRcptx = " + SQLUtil.toSQL(rcpt_app) + 
                      " AND a.sRecpntxx = " + SQLUtil.toSQL(rcpt_user) +
                      " AND a.sIMEINoxx = " + SQLUtil.toSQL(imei) + ")";
            ResultSet loRSRcpt;
            loRSRcpt = instance.executeQuery(lsSQL);
            
            while(loRSRcpt.next()){
                JSONObject data = new JSONObject();
                data.put("status", status);
                data.put("transno", loRSMaster.getString("sTransNox"));
                data.put("stamp", stamp);
                data.put("appsrce", loRSMaster.getString("sAppRcptx"));
                data.put("srceid", loRSMaster.getString("sRecpntxx"));
                data.put("srcenm", loRSMaster.getString("sUserName"));
                data.put("apprcpt", loRSRcpt.getString("sAppRcptx"));
                data.put("rcptid", loRSRcpt.getString("sRecpntxx"));
                data.put("rcptnm", loRSRcpt.getString("sUserName"));

                JSONObject msg = new JSONObject();
                msg.put("title", "Notification Update");
                msg.put("message", "");
                msg.put("imgurl", null);
                msg.put("msg_data", data);
                
                JSONObject response = sendMessage(loRSRcpt.getString("sAuthKeyx"), loRSRcpt.getString("sTokenIDx"), "high", msg);
                String result = (String) response.get("result");
                if(!result.equalsIgnoreCase("success")){
                    //logwrapr.severe("Error sending STATUS UPDATE:" + loRSMaster.getString("sTransNox") + "," + loRSRcpt.getString("sAppRcptx") + " " + response.toJSONString());
                    instance.rollbackTrans();
                    return false;
                }
            }
            
            instance.commitTrans();
            
            return true;
        } catch (SQLException ex) {
            //logwrapr.severe(ex.getMessage());
            instance.rollbackTrans();
            return false;
        }
    }
    
    private static JSONObject sendMessage(String auth_key, String to_key, String priority, JSONObject msg_data){
        System.out.println("sendMessage");
        try {
            Map<String, String> headers =
                    new HashMap<String, String>();
            headers.put("Accept", "application/json");
            headers.put("Content-Type", "application/json");
            headers.put("Authorization", "key=" + auth_key);

            //Create the parameters needed by the API
            JSONObject param = new JSONObject();
            param.put("to", to_key);
            param.put("priority", priority);
            param.put("data", msg_data);

            JSONParser oParser = new JSONParser();
            JSONObject json_obj = null;
            
            //String response = WebClient.httpPostJSon(sURL, param.toJSONString(), (HashMap<String, String>) headers);
            String response = WebClient.httpsPostJSon(FMC_URL_SEND, param.toJSONString(), (HashMap<String, String>) headers);
            
            //System.out.println("response:" + response);
            
            if(response == null){
                JSONObject err_detl = new JSONObject();
                err_detl.put("message", System.getProperty("store.error.info"));
                err_detl.put("code", System.getProperty("store.error.code"));
                JSONObject err_mstr = new JSONObject();
                err_mstr.put("result", "ERROR");
                err_mstr.put("error", err_detl);
                return err_mstr;
            }
            json_obj = (JSONObject) oParser.parse(response);
            json_obj.put("result", "success");
            
            //System.out.println(json_obj.toJSONString());
            //System.out.println(response);
            return json_obj;
        } catch (IOException ex) {
            JSONObject err_detl = new JSONObject();
            if(System.getProperty("store.error.code").length() == 0){
                err_detl.put("message", "IO Exception: " + ex.getMessage());
                err_detl.put("code", "250");
            }
            else{
                err_detl.put("message", System.getProperty("store.error.info"));
                err_detl.put("code", System.getProperty("store.error.code"));
            }
            JSONObject err_mstr = new JSONObject();
            err_mstr.put("result", "ERROR");
            err_mstr.put("error", err_detl);
            return err_mstr;
        } catch (ParseException ex) {
            JSONObject err_detl = new JSONObject();
            err_detl.put("message", "Parse Exception: " + ex.getMessage());
            err_detl.put("code", ex.getErrorType());
            JSONObject err_mstr = new JSONObject();
            err_mstr.put("result", "ERROR");
            err_mstr.put("error", err_detl);
            return err_mstr;
        }    
    }
}

