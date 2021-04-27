
import org.rmj.replication.server.SendNotification;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author mac
 */
public class testSendRequestNotification {
    public static void main(String [] args){
        String [] lxArgs = new String[2];
        lxArgs[0] = "request";
        lxArgs[1] = "MX0121000000003";
        
        SendNotification.main(lxArgs);
    }
}
