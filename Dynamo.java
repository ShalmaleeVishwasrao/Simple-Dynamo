package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.inputmethodservice.Keyboard;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Formatter;

import static edu.buffalo.cse.cse486586.simpledynamo.Tasks.*;
import static edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoProvider.*;

/**
 * Created by shalm on 4/30/2016.
 */
public class Dynamo {

    static String[] ring = new String[5];
    static String first;
    static String last;
    static String predecessor;
    static String replica1;
    static String replica2;

    static String queryingPort = null;
    static int queried = 0;
    static String allKeyValues = "";
    static String queriedKeyValue = null;
    static boolean responseReceived = false;


    static void initializeDynamo(Context context){
        Log.i(TAG,"Initializing Dynamo...");
        //get all ports
        initializeNetwork();

        //for correct ring
        try {
            String[] temp = new String[5];
            for (int i = 0; i < RemotePort.length; i++) {
                temp[i] = getHash(RemotePort[i]);
            }

            Arrays.sort(temp);

            Log.i(TAG, "Got all hash values of ports and sorted them");
            Log.i(TAG,"Length of temp array is : " + temp.length);
            for (int i = 0; i < temp.length; i++){
                Log.i(TAG,temp[i]);
            }

            for (int i = 0; i < temp.length; i++) {
                for (int j = 0; j < RemotePort.length; j++) {
                    if (temp[i].equals(getHash(RemotePort[j]))) {
                        ring[i] = RemotePort[j];
                        break;
                    }
                }
            }

        Log.i(TAG,"Printing for debugging purposes");
        Log.i(TAG, "Length of ring array is : " + ring.length);
        for (int i = 0; i < ring.length; i++){
            Log.i(TAG,"Node : " + ring[i]);
        }
            //Arrays.sort(ring);

            first = ring[0];
            last = ring[ring.length - 1];

            for (int i = 0; i < ring.length; i++) {
                if (ring[i].equals(getMyPort(context))) {
                    if (i == 0) {
                        predecessor = ring[ring.length - 1];
                        replica1 = ring[i + 1];
                        replica2 = ring[i + 2];
                    } else if (i == ring.length - 1) {
                        predecessor = ring[i - 1];
                        replica1 = ring[0];
                        replica2 = ring[1];
                    } else {
                        predecessor = ring[i - 1];
                        replica1 = ring[i + 1];
                        replica2 = ring[i + 2];
                    }
                }
            }
        }catch (Exception e){
            Log.e(TAG,"Some problem");
            e.printStackTrace();
        }
    }


    static String getMyPort(Context context){
        TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        return myPort;
    }

    static String getHash(String port){
        String hashedPort = null;
        try {
            port = Integer.parseInt(port)/2+ "";
            hashedPort = genHash(port);
            return hashedPort;
        }catch (NoSuchAlgorithmException e){
            Log.e(TAG,"Error while hashing port");
            e.printStackTrace();
        }
        return hashedPort;
    }

    static String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }


    static void handleRequest(SimpleDynamoProvider provider, Context context, String message){

        Log.i(TAG, "In handleRequest");
        String[] message_details = message.split("\\|");

        if(message_details[0].equals("NEW_MESSAGE")){
            Log.i(TAG,"Forwarded msg to insert key " + message_details[1] + " has been received by " + getMyPort(context));

            //insert into correct node
            String key = message_details[1];
            String value = message_details[2];
            KeyValueMap.put(key, value);
            Log.i(TAG, "Key inserted into host : " + getMyPort(context));

            //forward to replica 1
            String msg_rep1 = "REPLICATE_1" + "|" + key + "|" + value;
            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msg_rep1, replica1);
            Log.i(TAG, "Key forwarded for first replication to : " + replica1);
        }

        else if(message_details[0].equals("REPLICATE_1")){
            Log.i(TAG,"Replication 1 for key " + message_details[1] + " has been received by " + getMyPort(context));

            //insert into correct node
            String key = message_details[1];
            String value = message_details[2];
            KeyValueMap.put(key, value);
            Log.i(TAG, "Key inserted into replica 1 : " + getMyPort(context));

            //forward to replica 2
            String msg_rep2 = "REPLICATE_2" + "|" + key + "|" + value;
            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msg_rep2, replica2);
            Log.i(TAG, "Key forwarded for second replication to : " + replica2);
        }

        else if(message_details[0].equals("REPLICATE_2")){
            Log.i(TAG, "Replication 2 for key " + message_details[1] + " has been received by " + getMyPort(context));

            //insert into correct node
            String key = message_details[1];
            String value = message_details[2];
            KeyValueMap.put(key, value);
            Log.i(TAG, "Key inserted into replica 2 : " + getMyPort(context));
        }

        else if(message_details[0].equals("QUERY")){
            Log.i(TAG,"Forwarded message received is of type QUERY");
            //forwardedQuery = true;
            String key_to_query = message_details[1];
            queryingPort = message_details[2];

            if(key_to_query.equals("@")) {
                Log.i(TAG, "Local dump request received by " + getMyPort(context));
                //got the key and value from node into cursor
                if (!KeyValueMap.isEmpty()) {
                    Cursor cursor = provider.query(Uri.EMPTY, null, key_to_query, null, null);

                    //now to send response back to querying port, we need to build string from the cursor and send it
                    if (cursor.getCount() != 0) {
                        Log.i(TAG, "KeyValueMap of " + getMyPort(context) + "has : " + cursor.getCount() + " rows");

                        cursor.moveToFirst();

                        String key;
                        String value;
                        String allKV = "";

                        do {
                            key = cursor.getString(cursor.getColumnIndex("key"));
                            value = cursor.getString(cursor.getColumnIndex("value"));
                            String keyvalue = key + ":" + value;
                            allKV = allKV + keyvalue + "#";
                        } while (cursor.moveToNext());

                        String msg_query_response = "QUERY_RESPONSE" + "|" + key_to_query + "|" + allKV;
                        new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msg_query_response, queryingPort);
                        Log.i(TAG, "Port " + getMyPort(context) + " has sent all its data to " + queryingPort);
                        cursor.close();
                    }
                }
                else{
                    Log.i(TAG,"KeyValueMap of " + getMyPort(context) + "is empty. Therefore, sending nothing");
                    String msg_query_response = "QUERY_RESPONSE" + "|" + key_to_query + "|" + "#";
                    new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msg_query_response, queryingPort);
                }
                return;
            }

            //specific key queried
            else{
                /*Cursor cursor = provider.query(Uri.EMPTY, null, key_to_query, null, null);
                //if key is present in this node
                if(cursor.getCount() != 0) {
                    Log.i(TAG, "Key present on this node. Returning to original node");
                    cursor.moveToFirst();
                    String keyvalue = null;
                    do {
                        String key = cursor.getString(cursor.getColumnIndex("key"));
                        String value = cursor.getString(cursor.getColumnIndex("value"));
                        keyvalue = key + ":" + value;
                        Log.i(TAG, "Cursor returned contains : " + keyvalue);
                    } while (cursor.moveToNext());
                    */

                String keyvalue = KeyValueMap.get(key_to_query);

                    String msg_query_response = "QUERY_RESPONSE" + "|" + key_to_query + "|" + keyvalue;
                    new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msg_query_response, queryingPort);
                    //cursor.close();
                //}
            }
        }

        else if(message_details[0].equals("QUERY_RESPONSE")){
            Log.i(TAG,"Query response received");
            String queried_key = message_details[1];

            if(queried_key.equals("@")) {
                Log.i(TAG,"Received local dump");
                String allKV = message_details[2];
                queried++;
                allKeyValues = allKeyValues + allKV;
            }
            else{
                Log.i(TAG,"Specific key case");
                queriedKeyValue = message_details[2];
                responseReceived = true;
                //String keyvalue[] = message_details[2].split(":");
                //MatrixCursor cursor = new MatrixCursor(new String[]{keyvalue[0],keyvalue[1]});
                //provider.returnQueryCursor(cursor);
            }
        }

        else if(message_details[0].equals("DELETE")){
            Log.i(TAG,"Delete request received");
            String key_to_delete = message_details[1];
            String receivedNode = message_details[2];

            //if request is to delete from host
            if(receivedNode.equals("H")) {
                //delete from host and forward request to replica 1
                provider.delete(Uri.EMPTY, key_to_delete, null);
                String msg_delete = "DELETE" + "|" + key_to_delete + "|" + "R1";
                new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msg_delete, replica1);
            }
            else if(receivedNode.equals("R1")) {
                //delete from host and forward request to replica 1
                provider.delete(Uri.EMPTY, key_to_delete, null);
                String msg_delete = "DELETE" + "|" + key_to_delete + "|" + "R2";
                new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msg_delete, replica1);
            }
            else if(receivedNode.equals("R2")) {
                //this is the last replica for the key, delete from here
                provider.delete(Uri.EMPTY, key_to_delete, null);
            }
            else if(receivedNode.equals("L")){
                if(KeyValueMap.containsKey(key_to_delete)){
                    KeyValueMap.remove(key_to_delete);
                }
            }
            else if(receivedNode.equals("@")){
                KeyValueMap.clear();
            }
        }
    }
}
