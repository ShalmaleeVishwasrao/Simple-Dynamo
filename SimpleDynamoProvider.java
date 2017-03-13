package edu.buffalo.cse.cse486586.simpledynamo;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;

import static edu.buffalo.cse.cse486586.simpledynamo.Dynamo.*;
import static edu.buffalo.cse.cse486586.simpledynamo.Tasks.*;

public class SimpleDynamoProvider extends ContentProvider {

    static ConcurrentMap<String,String> KeyValueMap = new ConcurrentHashMap<String, String>();


	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
        //if delete all local
        if(selection.equals("@")){
            Log.i(TAG,"Deleting all keys on this node");

            for (Map.Entry e :KeyValueMap.entrySet()) {
                String msg_delete = "DELETE" + "|" + e.getKey() + "|" + "L";
                for (String node : ring) {
                    new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,msg_delete,node);
                }
            }
            KeyValueMap.clear();
        }

        //if delete all global
        else if(selection.equals("*")){
            KeyValueMap.clear();

            for (String node : ring) {
                if(!node.equals(getMyPort(getContext()))){
                    String msg_delete_all = "DELETE" + "|" + "@";
                    new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,msg_delete_all,node);
                }
            }
        }

        //if delete specific key
        else{
            Log.i(TAG,"Deleting key : " + selection);
            String key = selection;
            try {
                //check if this is the node where the key should be stored
                //if yes delete key and send request to replicas
                //if no, find the correct node for the key, forward request to it, and then to replicas

                String deleteNode = "";

                //if key is greater than last node or less than first node
                if(genHash(key).compareTo(getHash(last)) > 0 || genHash(key).compareTo(getHash(first)) < 0){
                    //correct node is first node
                    deleteNode = first;
                }
                else {
                    for (int i = 0; i < ring.length; i++) {
                        if (genHash(key).compareTo(getHash(ring[i])) > 0) {
                            continue;
                        } else {
                            //correct node found
                            deleteNode = ring[i];
                        }
                    }
                }

                if(deleteNode.equals(getMyPort(getContext()))){
                    KeyValueMap.remove(key);
                    //send delete command to replica1
                    String msg_delete = "DELETE" + "|" + key + "|" + "R1";
                    new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,msg_delete,replica1);
                    return 0;
                }

                //send delete request to correct delete node
                String msg_delete = "DELETE" + "|" + key + "|" + "H";
                new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,msg_delete,replica1);

            }catch (Exception e){
                Log.e(TAG, "Hashing error during delete");
                e.printStackTrace();
            }
        }
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub

        Log.i(TAG,"Inside insert");


        String key = null;
        String value = null;

        //get the key and value to be inserted
        if(values.containsKey("key")){
            key = values.getAsString("key");
        }
        if(values.containsKey("value")) {
            value = values.getAsString("value");
        }

        Log.i(TAG, "Key to be inserted is : " + key);
        Log.i(TAG, "Value to be inserted is : " + value);

        try {
            //find the correct node for the key
            //insert if self = correct node and forward to replica1

            String insertNode = "";

            //if key is greater than last node or less than first node
            if(genHash(key).compareTo(getHash(last)) > 0 || genHash(key).compareTo(getHash(first)) < 0){
                //correct node is first node
                insertNode = first;
                //new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msg_new, correctNode);
            }
            else {
                for (int i = 0; i < ring.length; i++) {
                    if (genHash(key).compareTo(getHash(ring[i])) > 0) {
                        continue;
                    } else {
                        //correct node found
                        insertNode = ring[i];
                        //new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msg_new, correctNode);
                    }
                }
            }

            Log.i(TAG, "Correct node for insertion is : " + insertNode);

            //if correct node is self, no need to forward
            if(insertNode.equals(getMyPort(getContext()))){
                //insert into map
                KeyValueMap.put(key,value);
                Log.i(TAG,"Key inserted into host : " + insertNode);

                //forward to replica 1
                String msg_rep1 = "REPLICATE_1" + "|" + key + "|" + value;
                new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msg_rep1, replica1);
                Log.i(TAG, "Key forwarded for first replication to : " + replica1);
            }
            //correct node is not self
            else{
                //forward to correct node
                String msg_new = "NEW_MESSAGE" + "|" + key + "|" + value;
                new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msg_new, insertNode);
                Log.i(TAG, "Key forwarded for insertion to : " + insertNode);
            }

        }catch (Exception e){
            Log.e(TAG, "Hashing error during insert");
            e.printStackTrace();
        }

        return uri;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub

        Log.i(TAG, "In onCreate()");
        Context context = getContext();
		initializeDynamo(context);

        Log.i(TAG, "App instance created on port : " + getMyPort(getContext()));

        //server starts listening as soon as app instance is created
        new ServerTask(this, getContext()).executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        Log.i(TAG, "Server Task has been started...");

		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
        Log.i(TAG,"Inside query");

        Log.i(TAG,"Key queried is : " + selection);

        MatrixCursor queryCursor = new MatrixCursor(new String[]{"key","value"});

        //if query all local
        if(selection.equalsIgnoreCase("@")){
            Log.i(TAG, "Local data queried from port : " + getMyPort(getContext()));
            //get all keys in that node
            try {
                if(!KeyValueMap.isEmpty()) {
                    for (Map.Entry<String, String> entry : KeyValueMap.entrySet()) {
                        queryCursor.addRow(new String[]{entry.getKey(), entry.getValue()});
                    }
                }
                else{
                    Log.i(TAG,"Node " + getMyPort(getContext()) + " does not have any data");
                }
                return queryCursor;
            }catch (Exception e){
                Log.e(TAG,"Hashing error while querying local keys");
            }
        }

        //if query all global
        else if(selection.equalsIgnoreCase("*")){
            Log.i(TAG, "Global data queried");
            Log.i(TAG,"Get local data of all");
            String msg_query_all = "QUERY" + "|" + "@" + "|" + getMyPort(getContext());
            for (String node : ring) {
                if(!node.equals(getMyPort(getContext()))){
                    Log.i(TAG, "Sending local dump request to : " + node);
                    new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msg_query_all, node);
                }
            }

            if (queried < ring.length-1) {
                try {
                    while (queried != ring.length-1) {
                        //wait till response is received
                        Log.i(TAG, "Waiting for response...");
                        Thread.sleep(5000);
                    }
                } catch (InterruptedException e) {
                    Log.e(TAG, "Error while sleeping");
                }

                Log.i(TAG, "Responses received");
                //Log.i(TAG, "Response is : " + queriedKeyValue);

                //adding own data
                Log.i(TAG,"Adding own data");
                String localData = "";
                for (Map.Entry<String,String> entry : KeyValueMap.entrySet()) {
                    localData += entry.getKey() + ":" + entry.getValue() + "#";
                }

                Log.i(TAG,"Combining data");
                allKeyValues += localData;

                Log.i(TAG,"Forming cursor");
                Log.i(TAG,"All pairs : " + allKeyValues);
                String pairs[] = allKeyValues.split("#");
                Log.i(TAG,"Number of key value pairs retrieved : " + pairs.length);
                for(String pair : pairs){
                    Log.i(TAG, pair);
                    if(pair.contains(":")) {
                        String keyvalue[] = pair.split(":");
                        queryCursor.addRow(new String[]{keyvalue[0], keyvalue[1]});
                        Log.i(TAG, "Key : " + keyvalue[0] + "Value : " + keyvalue[1]);
                    }
                }

                Log.i(TAG,"Received " + queryCursor.getCount() + " rows");

                //print entire cursor
                Log.i(TAG,"Printing cursor");
                while (queryCursor.moveToNext()){
                    Log.i(TAG,queryCursor.getString(queryCursor.getColumnIndex("key")));
                }

                queried = 0;
                allKeyValues = "";
                //responseReceived = false;
                //forwardedQuery = false;
                Log.i(TAG, "Flags have been reset");

                return queryCursor;
            }
        }

        //if query specific key
        else {
            String key = selection;
            Log.i(TAG,"Specific key queried : " + key);

            //find the correct node for the key,
            //retrieve value from its second replica
            //return it back to querying port

            try {
                String queryNode = "";
                //if key is greater than last node or less than first node
                if (genHash(key).compareTo(getHash(last)) > 0 || genHash(key).compareTo(getHash(first)) < 0) {
                    //correct node is first node
                    //therefore retrieve data from its 2nd replica
                    queryNode = ring[2];
                } else {
                    for (int i = 0; i < ring.length; i++) {
                        if (genHash(key).compareTo(getHash(ring[i])) > 0) {
                            continue;
                        } else {
                            //correct node found is ring[i]
                            //setting queryNode = its second replica
                            queryNode = ring[i + 2];
                        }
                    }
                }

                //if correct node is self, no need to forward, just return value
                if(queryNode.equals(getMyPort(getContext()))){
                    Log.i(TAG,"Replica 2 for key " + key + " is " + getMyPort(getContext()));
                    queryCursor.addRow(new String[]{key, KeyValueMap.get(key)});
                    return queryCursor;
                }

                //sending request to queryNode for retrieval
                String msg_query = "QUERY" + "|" + key + "|" + getMyPort(getContext());
                new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msg_query, queryNode);

                while(!responseReceived){
                    //wait till response is received
                    Log.i(TAG, "Waiting for response...");
                    Thread.sleep(500);
                }

                Log.i(TAG, "Response received");
                Log.i(TAG, "Response is : " + queriedKeyValue);

                String keyvalue[] = queriedKeyValue.split(":");
                Log.i(TAG, "Size of key value is : " + keyvalue.length);
                queryCursor.addRow(new String[]{keyvalue[0], keyvalue[1]});

                queriedKeyValue = null;
                responseReceived = false;
                //forwardedQuery = false;
                Log.i(TAG, "Flags have been reset");

            }catch (InterruptedException e) {
                Log.e(TAG, "Error while sleeping");
            }catch (Exception e){
                Log.e(TAG,"Error while querying specific key");
            }

        }

        return queryCursor;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
