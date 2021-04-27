/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.rmj.replication.server;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.rmj.appdriver.GRider;
import org.rmj.appdriver.SQLUtil;
import org.rmj.lib.net.LogWrapper;

/**
 *
 * @author sayso
 */
public class ExtractPaymentFromPartner {
    private static GRider instance = null;
    private static LogWrapper logwrapr = new LogWrapper("Server.ExtractPaymentFromPartner", "ExtractPaymentFromPartner.log");
    private static String cebuana = "GGC_BM001";
    
    public static void main(String args[]){
        String path;
        if(System.getProperty("os.name").toLowerCase().contains("win")){
            path = "D:/GGC_Java_Systems";
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

        captureCebuanaData();
        
    }
    
    private static void captureCebuanaData(){
        String sql = "SELECT sTransNox, sPayloadx" + 
                    " FROM XAPITrans" + 
                    " WHERE sXAPICode = 'GAP10002'" + 
                      " AND sClientID = " + SQLUtil.toSQL(cebuana) +
                      " AND cTranStat = '0' AND dCaptured IS NULL;";
        ResultSet rs = instance.executeQuery(sql);
        
        try {
            JSONParser oParser = new JSONParser();
            JSONObject json_obj = null;
            while(rs.next()){
                json_obj = (JSONObject) oParser.parse(rs.getString("sPayloadx"));
                System.out.println("++++++++++++++++++++++++++++++");
                System.out.println((String)json_obj.get("branch"));
                System.out.println((String)json_obj.get("referno"));
                System.out.println((String)json_obj.get("datetime"));
                System.out.println((String)json_obj.get("account"));
                System.out.println((String)json_obj.get("name"));
                System.out.println((String)json_obj.get("address"));
                System.out.println((String)json_obj.get("mobile"));
                System.out.println(Double.parseDouble((String)json_obj.get("amount")));
                
                if(!updateMobileNo((String)json_obj.get("account"), (String)json_obj.get("mobile"))){
                    System.exit(0);
                }
            }
        } catch (SQLException ex) {
            logwrapr.severe("IOException error detected.", ex);
        } catch (ParseException ex) {
            logwrapr.severe("ParseException error detected.", ex);
        }
    }
    
    private static boolean updateMobileNo(String acctno, String mobile){
        String sql = "SELECT b.sClientID, b.sMobileNo, c.sMobileNo xMobileNo" +
                    " FROM MC_AR_Master a" +
                        " LEFT JOIN Client_Master b ON a.sClientID = b.sClientID" +
                        " LEFT JOIN Client_Mobile c ON b.sClientID = c.sClientID AND b.sMobileNo = c.sMobileNo" +
                    " WHERE a.sAcctNmbr = " + SQLUtil.toSQL(acctno);
        ResultSet rs = instance.executeQuery(sql);
        try {
            if(!rs.next()){
                return false;
            }
        } catch (SQLException ex) {
            Logger.getLogger(ExtractPaymentFromPartner.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        
        //if xmobile is not existing then
           // add xmobile from Client_mobile setting it as primary
        //else
           // if xmobile is not primary set it as the primary
        //
        
        //if mobile is different from mobile from cebuana
            //set the mobile from cebuana as primary mobile
        //
        
        return true;
    }
}


