package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.os.AsyncTask;
import android.util.Log;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

import static edu.buffalo.cse.cse486586.simpledynamo.Dynamo.*;

/**
 * Created by shalm on 4/21/2016.
 */
public class Tasks {

    static final String TAG = SimpleDynamoActivity.class.getSimpleName();
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final int SERVER_PORT = 10000;

    static ServerSocket serverSocket = null;
    static String RemotePort[] = null;

    public static void initializeNetwork(){
        Log.i(TAG,"Initializing Network...");
        RemotePort = new String[5];
        RemotePort[0] = REMOTE_PORT0;
        RemotePort[1] = REMOTE_PORT1;
        RemotePort[2] = REMOTE_PORT2;
        RemotePort[3] = REMOTE_PORT3;
        RemotePort[4] = REMOTE_PORT4;

        try{
            serverSocket = new ServerSocket(SERVER_PORT);
        }catch (IOException e){
            Log.e(TAG, "Error creating Server Socket");
            e.printStackTrace();
        }
    }


    /***
     * ServerTask is an AsyncTask that should handle incoming messages.
     */
    public static class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        SimpleDynamoProvider sdprovider;
        Context context;

        //constructor
        ServerTask(SimpleDynamoProvider sp,Context c){
            context = c;
            sdprovider = sp;
        }

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */

            while (true) {
                try {
                    //reading message from client
                    Log.i(TAG, "Waiting for client...");

                    //create a socket for connection
                    Socket socket = serverSocket.accept();

                    //read the message sent by client
                    BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String message = br.readLine();

                    //send message to other client if not empty
                    if(message != null){
                        Log.i(TAG, "Server received : " + message);
                        publishProgress(message);
                    }

                    br.close();
                    socket.close();
                } catch (IOException e) {
                    Log.e(TAG, "Server socket cannot receive messages");
                }

            }

            //return null;
        }

        protected void onProgressUpdate(String...strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            String message = strings[0].trim();
            //pass to another function for handling

            Log.i(TAG,"Handling requests on Progress Update");
            handleRequest(sdprovider,context,message);

        }
    }

    public static class ClientTask extends AsyncTask<String, Void, Void>{

        @Override
        protected Void doInBackground(String... msgs) {
            try {
                String msgToSend = msgs[0];
                String remotePort = msgs[1];

                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));

                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                bw.write(msgToSend);
                bw.close();
                socket.close();
            } catch (UnknownHostException e) {
                Log.e(TAG, "UnknownHostException in ClientTask");
                e.printStackTrace();
            } catch (IOException e) {
                Log.e(TAG, "IOException in ClientTask");
                e.printStackTrace();
            }

            return null;
        }
    }
}
