package com.salesforce.apollo.protocols;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Collections;
import java.util.Enumeration;

import static java.lang.System.out;

/**
 * @author hal.hildebrand
 **/
public class ListNIFs {
    public static void main(String[] args) throws SocketException {
        Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();

        for (NetworkInterface netIf : Collections.list(nets)) {
            out.printf("Display name: %s\n", netIf.getDisplayName());
            out.printf("Name: %s\n", netIf.getName());
            var addresses = netIf.getInterfaceAddresses();
            for (var add : addresses) {
                out.printf("Address: %s\n", add);
            }
            displaySubInterfaces(netIf);
            out.print("\n");
        }
    }

    static void displaySubInterfaces(NetworkInterface netIf) throws SocketException {
        Enumeration<NetworkInterface> subIfs = netIf.getSubInterfaces();
        for (NetworkInterface subIf : Collections.list(subIfs)) {
            out.printf("\tSub Interface Display name: %s\n", subIf.getDisplayName());
            out.printf("\tSub Interface Name: %s\n", subIf.getName());
            Enumeration<InetAddress> inetAddresses = subIf.getInetAddresses();
            for (InetAddress add : Collections.list(inetAddresses)) {
                out.printf("\t Sub Interface Address: %s\n", add.getHostAddress());
            }
        }
    }
}
