package edu.buffalo.cse.cse486586.groupmessenger1;

import edu.buffalo.cse.cse486586.groupmessenger1.R;

import android.app.Activity;
import android.content.ContentValues;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.view.KeyEvent;
import android.view.View;
import android.view.View.OnKeyListener;
import android.text.method.ScrollingMovementMethod;
import android.view.Menu;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;
import android.content.Context;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 * 
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {

    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";

    static final String[] remotePorts = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
    static final int SERVER_PORT = 10000;
    static int countKey = 0; /* counter for keys */


    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        /*Calculate the port numbers for the AVD */
        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));


        try{
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        }catch (IOException e){
            Log.e(TAG, "Can't create a ServerSocket");
        }

        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        
        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));
        
        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */


        final EditText editText = (EditText) findViewById(R.id.editText1);
        final Button bSendText = (Button) findViewById(R.id.button4);

        bSendText.setOnClickListener(new Button.OnClickListener(){
            @Override
            public void onClick(View v) {
                String msg = editText.getText().toString() + "\n";
                editText.setText(""); // This is one way to reset the input box.
                TextView localTextView = (TextView) findViewById(R.id.textView1);
                localTextView.append("\t" + msg); // This is one way to display a string.
                TextView remoteTextView = (TextView) findViewById(R.id.textView1);
                remoteTextView.append("\n");

                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
            }
        });
    }

    /* Server Task */
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             *
             * References:
             * 1. https://docs.oracle.com/javase/tutorial/networking/sockets/index.html ( for the PrintWriter and BufferedReader )
             * 2. https://developer.android.com/reference/android/os/AsyncTask.html ( for basic understanding of the AsyncTask and publishProgress() )
             */
            try
            {
                while(true) {
                    /* 1. Accept Client Socket */
                    Socket clientSocket = serverSocket.accept();
                    /* Log.i(TAG, "Server/Socket Accepted ["+clientSocket.getPort()+"]"); */

                    /* 2. DONOT send message if socket not connected */
                    if (!clientSocket.isConnected()) {
                        continue;
                    }

                    /* 3. Create a Input Stream */
                    BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    /* Read incoming data from client */
                    String incoming;
                    /* 4. a. Read the data from the input stream
                     * 4. b. Push the data
                    */
                    if((incoming = in.readLine())!= null)
                    {
                        /* Log.i(TAG, "Server/Message : "+incoming); */
                        publishProgress(incoming);
                    }

                    /* @CHECK : if socket needs to be closed */
                    /* @CHECK : if input stream needs to be closed */
                    /* @CHECK : how to manage multiple messages : Accept sockets in a while(True) */
                }
            } catch (UnknownHostException e) {
                Log.e(TAG, "ServerTask UnknownHostException");
            }
            catch (IOException e1) {
                e1.printStackTrace();
            }

            return null;
        }

        /* Ref: From OnPTestClickListener */
        private Uri buildUri(String scheme, String authority) {
            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.authority(authority);
            uriBuilder.scheme(scheme);
            return uriBuilder.build();
        }

        protected void onProgressUpdate(String...strings) {
            /*
             * The following code displays what is received in doInBackground().
             *
             * Ref: From PA1 : SimpleMessengerActivity.java
             */
            String strReceived = strings[0].trim();
            TextView remoteTextView = (TextView) findViewById(R.id.textView1);
            remoteTextView.append(strReceived + "\t\n");
            TextView localTextView = (TextView) findViewById(R.id.textView1);
            localTextView.append("\n");


            /* Ref: From OnPTestClickListener */
            final Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger1.provider");
            final ContentValues mContentValues = new ContentValues();

            // creating the content values //
            mContentValues.put(MessageReaderDbHelper.COLUMN_NAME_KEY, Integer.toString(countKey));
            mContentValues.put(MessageReaderDbHelper.COLUMN_NAME_VALUE, strReceived);
            countKey++;

            try{
                getContentResolver().insert(mUri, mContentValues);
            }catch (Exception e) {
                Log.e(TAG,"Unable to push to own database!");
            }
        }   // end of progressUpdate //

    }   // end of ServerTask //


    /* ClientTask */
    private class ClientTask extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {
            try {
                String remotePort;
                for (int i = 0; i < 5; i++) {
                     remotePort= remotePorts[i];
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));

                    String msgToSend = msgs[0];

                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    out.println(msgToSend);
                }

            } catch (UnknownHostException e) {
                Log.e(TAG, "Client Task UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }
            return null;
        }
    }   // end of ClientTask


    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }
}
