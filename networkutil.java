import java.net.*;

public class networkutil{

    public static void main(String args[]) throws SocketException{

        System.out.println(getLocAddr());
    }

    public static String getLocAddr(){

        String localAddress=null;

        try{

            InetAddress localhost= InetAddress.getLocalHost();
            InetAddress[] allAddresses= InetAddress.getAllByName(localhost.getHostName());

            for (InetAddress address : allAddresses){
                if(address instanceof Inet4Address && !address.isLoopbackAddress()){
                    localAddress=address.getHostAddress();
                    break;                
                }
            

            }
        } catch(Exception e){
            e.printStackTrace();
        }

        return localAddress;
    }
}