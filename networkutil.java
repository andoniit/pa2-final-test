import java.net.*;

public class networkutil{

    public static void main(String args[]) throws SocketException{

        System.out.println(getLocalAddress());
    }

    public static String getLocalAddress(){

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