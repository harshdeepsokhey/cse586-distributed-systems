package edu.buffalo.cse.cse486586.simpledht;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static android.content.Context.TELEPHONY_SERVICE;

// comparator //
class CustomComparator implements Comparator<String> {
    @Override
    public int compare(String lhs, String rhs)
    {
        return lhs.compareTo(rhs);
    }
}

public class SimpleDhtProvider extends ContentProvider {

    // hashMap : contains all the <key,value> pairs //
    static ConcurrentHashMap<String, String> hashMap;

    //
    static Map<String , String> queryMap = new ConcurrentHashMap<String , String>();


    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static final String TAGC = ServerTask.class.getSimpleName();
    static final String TAGS = ClientTask.class.getSimpleName();

    static Uri myUri = null;
    
    // self node info
    static String myPort = null;
    static String myId = null;
   
    // successor info
    String successorPort = null;
    String successorId = null;

    //predecessor info
    String predecessorPort = null;
    String predecessorId = null;
    String endNode = null;

    // nodeMap <port, id> //
    private HashMap<Integer, Integer> nodeMap = new HashMap<Integer, Integer>();

    // nodeHashMap <hashOfId , port> //
    private HashMap<String, Integer> nodeHashMap = new HashMap<String, Integer>();

    private HashMap<String, String> nodePortMap = new HashMap<String, String>();

    // list of all nodes //
    List<String> nodes = new ArrayList<String>();

    static int  node1_port = 11108; // GOD node //


    /* MSGTYPE:
    1. JOIN REQUEST : sent by newly powered up nodes to the ring with GOD node
    2. JOIN RESPONSE: sent by GOD node to other nodes // to be removed
    3. INSERT: insert a <key,value> pair
    4. QUERY REQUEST: query a <key,value> , "*" : query all ; "@" : query self ; KS: query only key-specific
    5. QUERY RESPONSE: return result of the query
    6. UPDATE NODES : change predecessor/successor when new AVDs join the ring or some AVDs die!
    7. UPDATE ENTRIES: updates entries corresponding to the nodes
    8. DELETE: delete a <key, value> , "*" : delete all; "@" : delete self ; "KS" : delete only key-specific

    @TODO: for query and delete: handle request forwarding // DONE //
    */

    /* @Ref: https://docs.oracle.com/javase/tutorial/java/javaOO/enum.html */
    public enum msgType {
        req_join(1), rep_join(2),
        update_nodes(3),
        insert(4),
        req_query(5), rep_query(6),
        update_entries(7),
        delete(8);

        private int value;

        msgType(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {


        /*
        int type = msgType.delete.getValue();
        String key = selection;
        String value = successorPort;

        Message msg = new Message(type,key, value);
        */
        
        Log.i(TAG,"DELETE: type="+Integer.toString(msgType.delete.getValue())+" ,key="+selection+" ,value="+successorPort);

        // Case 1:  DELETE SELF KEY-VALUE PAIRS : delete self only//
        if(selection.equals("@"))
        {
            hashMap.clear();
        }
        // Case 2: DELETE ALL KEY-VALUE PAIRS : delete self and forward the delete request to successorPort //
        else if(selection.equals("*"))
        {
            hashMap.clear();
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(msgType.delete.getValue()),selection,successorPort, myPort);
        }
        // Case 3: KEY-SPECIFIC: delete only the key-value for the given key in the hashMap //

        else
        {
            // Case 3a: KEY IN SELF //
            if(hashMap.containsKey(selection))
            {
                hashMap.remove(selection);
            }
            // Case 3b: KEY NOT IN SELF, FORWARD REQUEST TO SUCCESSOR //
            else
            {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(msgType.delete.getValue()),selection,successorPort,myPort);
            }
        }

        return 0;
    }   // END OF DELETE

/**************************************************************************************************************************/

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

/**************************************************************************************************************************/
    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub

        /* Message Format : "INSERT;KEY-VALUE" */

        // @Ref: from PA 2 //
        String key = (String) values.get("key");
        String value = (String) values.get("value");

        Log.i(TAG,"Content Values : key="+ key + ",value="+ value);

        String hash = "";

        try {
            /* generate hash for key */
            hash = genHash(key);

        } catch (NoSuchAlgorithmException nsae) {
            nsae.printStackTrace();
        }

        boolean isValid = validateHash(hash);

        Log.i(TAG, " INSERT METHOD: isValid=" +isValid+" ,hash= "+ hash);

        /* Case 1: INSERT IN SELF NODE */
        if (isValid == true)
        {
            Log.i(TAG, "INSERT/SELF Node isValid: "+isValid);
            /* check duplication of key
            *!!! ALWAYS DELETE AND ADD , NEVER UPDATE IN PLACE !!!
            * */
            if(hashMap.containsKey(values.get("key")))
            {
                Log.i(TAG,"Duplicate Key!! Update key!"+ values.get("key"));
                hashMap.remove(values.get("key"));
            }

            // update/add <key, value> pair: remove and insert //
            hashMap.put(values.get("key").toString(), values.get("value").toString());
            Log.i(TAG," INSERT: Value put successfully! key="+key+ " ,value="+value);

        }
        // CASE 2: FORWARD TO SUCCESSOR NODE //
        else
        {
            // You are not the rightful node, hence forward to the successor node
            /*
            int type = msgType.insert.getValue();
            String k = (String)values.get("key");
            String v = (String)values.get("value");
            Message msg = new Message(type,key, value);
            */

            String insertMsg = msgType.insert.getValue()+";"+(String)values.get("key")+"-"+(String)values.get("value");
            Log.i(TAG, "INSERT/NOT SELF : isValid" +isValid+ ", Forward to Successor Port :"+successorPort+ ", str="+insertMsg);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(msgType.insert.getValue()), insertMsg, successorPort, myPort);
        }

        return uri;
    }

    @Override
    public boolean onCreate() {
        /* @Ref : from previous PAs */
        hashMap = new ConcurrentHashMap<String, String>();
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        myUri = buildUri("content","content://edu.buffalo.cse.cse486586.simpledht.provider");

        /* on first bootup */
        successorPort = myPort;     successorId = myId;
        predecessorPort = myPort;   predecessorId = myId;

        try
        {
            ServerSocket serverSocket = new ServerSocket(10000);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        }catch (IOException ie)
        {
            Log.e(TAG, "Error: Server Socket Creation Failed!");
            return false;
        }
        
        Log.i(TAG, "SERVER / Info : Started!");

        try
        {
            myId = genHash(portStr);
        } catch(NoSuchAlgorithmException nsae)
        {
            nsae.printStackTrace();
        }
        
        Log.i(TAG,"SERVER / Info : myId="+myId+" ,myPort="+myPort+" ,portStr="+portStr);
        
        try
        {
            // bind all hashes for nodes to port number //
            /*nodeHashMap.put(genHash(node1_id), node1_port);
            nodeHashMap.put(genHash(node2_id), node2_port);
            nodeHashMap.put(genHash(node3_id), node3_port);
            nodeHashMap.put(genHash(node4_id), node4_port);
            nodeHashMap.put(genHash(node5_id), node5_port);
            */

            // hash(id) -> port //
            nodeHashMap.put(genHash("5554"), 11108);
            nodeHashMap.put(genHash("5556"), 11112);
            nodeHashMap.put(genHash("5558"), 11116);
            nodeHashMap.put(genHash("5560"), 11120);
            nodeHashMap.put(genHash("5562"), 11124);

            // update current node's successor and predecessor //
            successorId = genHash(portStr);
            successorPort = myPort;

            predecessorId = genHash(portStr);
            predecessorPort = myPort;

            endNode = successorId;

            // add node to list //
            nodes.add(myId);

            Log.i(TAG,"NODE INFO: successor<port, id> = [ "+successorPort+" ,"+successorId+" ]," +
                                "predecessor<port, id> = [ "+predecessorPort+ " ,"+ predecessorId+"]");

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        // bind port and id //
        /*nodeMap.put(node1_port, Integer.parseInt(node1_id));
        nodeMap.put(node2_port, Integer.parseInt(node2_id));
        nodeMap.put(node3_port, Integer.parseInt(node3_id));
        nodeMap.put(node4_port, Integer.parseInt(node4_id));
        nodeMap.put(node5_port, Integer.parseInt(node5_id));
        */

        // port -> id //
        nodeMap.put(11108, 5554);
        nodeMap.put(11112, 5556);
        nodeMap.put(11116, 5558);
        nodeMap.put(11120, 5560);
        nodeMap.put(11124, 5562);

        // IF NOT GOD NODE, ASK TO JOIN //
        if(Integer.parseInt(myPort) != 11108)
        {
            Log.i(TAG, "NOT GOD");
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(msgType.req_join.getValue()), myPort);
        }

        return false;
    }
    
    // Validate Hash of the Node  //
    public boolean validateHash(String hash)
    {
        boolean result = false;
        // IF ONLY ONE NODE //
        if(myId.equals(successorId) && myId.equals(predecessorId))
        {
            result = true;
        }

        else if(myId.compareTo(predecessorId) > 0 && hash.compareTo(myId) <=0 && hash.compareTo(predecessorId) > 0)
        {
            result = true;
        }
        else if(myId.compareTo(predecessorId) < 0 && (hash.compareTo(predecessorId) > 0 || hash.compareTo(myId)<=0))
        {
            result = true;
        }
        return result;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        //@Ref: https://developer.android.com/reference/android/database/MatrixCursor.html#MatrixCursor(java.lang.String[],%20int) //
        MatrixCursor mc = new MatrixCursor(new String[]{"key","value"});

        // CASE 1: QUERY SELF //
        if(selection.equals("@"))
        {
            // read key-value from hashMap and add to head of mc //
            for(String a: hashMap.keySet())
            {
                mc.moveToFirst();
                mc.addRow(new Object[]{a, hashMap.get(a)});
                Log.i(TAG,"QUERY Method/ @: node="+a+" , value="+hashMap.get(a));
            }
        }
        // CASE 2: QUERY ALL //
        else if(selection.equals("*"))
        {
            // start reading from top //
            mc.moveToFirst();
            for(String b: hashMap.keySet())
            {
                mc.addRow(new Object[]{b, hashMap.get(b)});
                Log.i(TAG,"QUERY Method/ *: node="+b+" , value="+hashMap.get(b));
            }

            Log.i(TAG, "QUERY Method/ *: myPort= "+myPort+", successor= "+successorPort);

            // case 2a: handle forward request //
            if(!myPort.equals(successorPort))
            {
                // get query results from the successor port //
                String resp = getResponseMsg(selection);
                Log.i(TAG,"QUERY Method/ Forward : response="+resp+ " ,myPort="+myPort+" ,successorPort="+successorPort);

                // format : <key1>-<value1>:<key2>-<value2>: .. :<keyN>-<valueN> //
                String[] tok = resp.split(":");
                
                for(String s: tok)
                {
                    Log.i(TAG,"QUERY Method/ * : Forward : tokens="+s);
                    if(!s.contains("-"))
                    {
                        continue;
                    }

                    String[] keyvalue = s.split("-");
                    //String kk = keyvalue[0];
                    //String vv = keyvalue[1];
                    mc.addRow(new Object[]{keyvalue[0], keyvalue[1]});
                    Log.i(TAG, "QUERY Method/ * : Forward : key="+keyvalue[0]+" ,value=" +keyvalue[1]);
                }

                queryMap.remove(selection);
            }
        }
        /* CASE 3: KEY-SPECIFIC QUERY */
        else
        {
            String ksKey = selection;
            Log.i(TAG, "QUERY Method/ KS: key="+ksKey);
            mc.moveToFirst();
            String ksValue = hashMap.get(ksKey);
            Log.i(TAG, "QUERY Method/ KS: key="+ksKey +" , value="+ksValue);

            // CASE 3a: QUERY KEY IN SELF NODE //
            if(ksValue != null)
            {
                mc.addRow(new Object[]{ksKey, ksValue});
                Log.i(TAG,"QUERY Method/ KS: key="+ksKey+" ,value="+ksValue);
                return mc;
            }
            // CASE 3b: QUERIED KEY NOT IN SELF NODE, FORWARD REQUEST //
            else if(successorPort != myPort && ksValue == null)
            {
                // Fetches response from successor node //
                // format: <key1>-<value1>;<key2>-<value2>; .. ;<keyN>-<valueN>
                String fwdResp = getResponseMsg(selection);
                Log.i(TAG, "QUERY Method/ KS: Forward: response="+ fwdResp);
                String[] tok = fwdResp.split(";");
                for(String t: tok)
                {
                    String [] keyvalues = t.split("-");
                    mc.addRow(new Object[]{keyvalues[0],keyvalues[1]});
                }
                queryMap.remove(selection);
                return mc;
            }
        }

        return mc;
    }

    // FETCHES RESPONSE FOR FORWARD REQUESTS //
    /*
    * Create socket with successor
    * Forward the request message
    * Wait for the stream to write to the queryMap
    * Read the queryMap and deliver the response to the original node
    *
    */
    private String getResponseMsg(String ... selection)
    {
        Socket socket = null;
        String response = "NACK";
        String qKey = selection[0];
        String request = "";

        try
        {
            socket = new Socket(InetAddress.getByAddress(new byte[]{10,0,2,2}), Integer.parseInt(successorPort));
            Log.i(TAG,"SOCKET Created :socket="+socket);
            DataInputStream incoming = new DataInputStream(socket.getInputStream());
            DataOutputStream outgoing = new DataOutputStream(socket.getOutputStream());

            // format : REQ-QUERY;SENDER_PORT;KEY_TO_BE_QUERIED //
            request = msgType.req_query.getValue()+";"+myPort+";"+qKey;

            outgoing.writeUTF(request);
            Log.i(TAG,"REQUEST sent : key="+request+" , values="+successorPort);

            do {
                response = incoming.readUTF();
                Log.i(TAG,"Received Acknowledgemnet : response="+response);
            }while(response.equals("NACK"));

            // WAIT FOR THE PIPE TO RECEIVE THE MESSAGE RESPONSE //
            do
            {
                Log.i(TAG, "STATUS="+queryMap.containsKey(qKey));
                Thread.sleep(150);
            }while(!queryMap.containsKey(qKey));
            
            Log.i(TAG, "Thread is NOW Awake, fetch the query response from the queryMap");
            
            if(incoming != null)
            {
                incoming.close();
            }

            if(outgoing != null)
            {
                outgoing.close();
            }

            socket.close();
            Log.i(TAG, "Socket ("+socket+") is CLOSED! ");
            
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        String qResponse= queryMap.get(qKey);
        Log.i(TAG, "COLLECT RESPONSE : Response Received: response="+qResponse+" for key="+qKey);

        return qResponse;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
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

    /* @Ref: from previous Programming Assignments */
    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    /*************************************************************************************************/
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {

            ServerSocket serverSocket = sockets[0];
            Socket ss = null;

            do   // check if it can be optimized //
            {
                try {
                    /* accept incoming connection */
                    ss = serverSocket.accept();

                    /* create 2 stream for input and output */
                    DataInputStream incoming = new DataInputStream(ss.getInputStream());
                    DataOutputStream outgoing = new DataOutputStream(ss.getOutputStream());

                    String rawStr = null;
                    String response = null;

                    while (null != (rawStr = incoming.readUTF())) {

                        Log.i("SERVER","RAW_STRING: "+rawStr);

                        String[] tok = rawStr.split(";");

                        // messageType //
                        int type = Integer.parseInt(tok[0]);
                        
                        Log.i("SERVER", "incoming message: type= "+type+" ,rawStr="+rawStr);

/**************************************************************************************************/
                        // CASE 1: JOIN REQUEST //
                        if(type == msgType.req_join.getValue())
                        {
                            // FORMAT: JOIN_REQUEST;PORT;HASH //
                            String key = tok[1];
                            String value = tok[2];

                            // JOIN IN ORDER //
                            nodes.add(value);
                            Collections.sort(nodes);

                            Log.i("SERVER", "JOIN REQUEST: key="+Integer.parseInt(key)+" ,nodes="+nodes.toString());

                            int currPos = nodes.indexOf(value);
                            endNode = nodes.get(nodes.size() - 1);

                            Log.i(TAG, "Added node "+endNode+ "at position "+ currPos);

                            //Calculate the predecessor and successor based on current node and size //
                            int preNode = (currPos -1)% nodes.size();
                            int sucNode = (currPos +1)% nodes.size();

                            Log.i("SERVER","preNode= "+preNode+" ,sucNode="+sucNode);

                            if (preNode < 0)
                            {
                                preNode = nodes.size() -1;
                            }
                            if (sucNode < 0)
                            {
                                sucNode = nodes.size() -1;
                            }

                            // get corresponding mappings //
                            int prePort = nodeHashMap.get(nodes.get(preNode));
                            int sucPort = nodeHashMap.get(nodes.get(sucNode));

                            Log.i("SERVER","JOIN: prePort="+prePort+" ,sucPort="+sucPort);


                            // returns response message as : //
                            // format : RESPONSE_JOIN;predecessorPort;successorPort //

                            response = msgType.rep_join.getValue() + ";" + prePort + ";"+sucPort;
                            outgoing.writeUTF(response);
                            Log.i(TAG, "JOIN RESPONSE sent! response = "+response);

                            // update successors or predecessors for the nodes //
                            String preUpdate = msgType.update_nodes.getValue()+";S;"+key; // updates successor
                            String sucUpdate = msgType.update_nodes.getValue()+";P;"+key; // updates predecessor

                            Log.i("SERVER","predecessor update="+preUpdate+" ,successor update="+sucUpdate);

                            publishProgress(Integer.toString(msgType.update_nodes.getValue()),
                                            preUpdate ,Integer.toString(prePort),
                                            sucUpdate ,Integer.toString(sucPort));


                        }
/**************************************************************************************************/
                        // 2. UPDATE NODES //
                        else if(type == msgType.update_nodes.getValue())
                        {
                            /*
                            * Message Format:
                            * update_nodes;S;<port> : update the successor
                            * update_nodes;P;<port> : update the predecessor
                            */

                            // send ACK to client //
                            outgoing.writeUTF("ACK");
                            Log.i("SERVER", "UPDATE NODES");
                            
                            String nodeType = tok[1]; // S or P //
                            
                            int portValue = Integer.parseInt(tok[2]);
                            
                            Log.i("SERVER", "UPDATE NODE /info: type="+nodeType+" , value="+portValue);

                            if(nodeType.equals("S")) // update successor //
                            {
                                //update successorPort from msg field //
                                successorPort = Integer.toString(portValue);
                                
                                // generate hash from the port and id mapping in NodeMap //
                                successorId = genHash(Integer.toString(nodeMap.get(portValue)));
                            }
                            else if (nodeType.equals("P")) // update predecessor //
                            {
                                String oldPredecessorId = predecessorId;
                                
                                ArrayList<String> listKey = new ArrayList<String>();
                                ArrayList<String> listValue = new ArrayList<String>();

                                Log.i("SERVER", "BLAH BLAH NODE MAP: isEmpty ="+nodeMap.isEmpty());
                                
                                predecessorId = genHash(Integer.toString(nodeMap.get(portValue)));
                                Log.i(TAG,"BLAH BLAH : predecessorId: +"+predecessorId);
                                
                                predecessorPort = Integer.toString(portValue);
                                Log.i(TAG,"BLAH BLAH : predecessorPort: +"+predecessorPort);
                                
                                //Log.i("SERVER","Check hashMap.size()="+hashMap.size());
                                
                                if(!hashMap.isEmpty())
                                {
                                   for(String hmk: hashMap.keySet())
                                   {
                                       if(hmk.compareTo(predecessorId) <= 0)
                                       {
                                           listValue.add(hashMap.get(hmk));
                                           listKey.add(hmk);
                                       }
                                   }
                                    
                                    // find new home for hashMap elements //
                                   for(int i=0; i < listKey.size(); i++)
                                   {
                                       hashMap.remove(listKey.get(i));
                                   }

                                   String updateReq = msgType.update_entries.getValue()+";"+listValue.size()+";";

                                   for(int j=0; j < listValue.size(); j++)
                                   {
                                       updateReq = updateReq + listKey.get(j)+ "-" + listValue.get(j)+":";
                                   }

                                   updateReq = updateReq.substring(0, updateReq.length()-1);
                                   
                                   Log.i("SERVER","UPDATE NODES/ : PART HT ="+updateReq+" ,beforePreId = "+oldPredecessorId+ " ,afterPreId="+predecessorId);

                                   publishProgress(Integer.toString(msgType.update_entries.getValue()),updateReq, nodePortMap.get(oldPredecessorId));
                                }
                            }
                            else
                            {
                                    Log.e("SERVER","UPDATE / UNKNOWN NODE TYPE! ");
                            }
                        }
/**************************************************************************************************/
                        /* 3. UPDATE ENTRIES FOR NODES
                        *
                        *
                        *
                        * *****  DONOT TOUCH THIS CODE !! IT WILL BREAK EVERY THING DOWN! ******
                        */
                        else if (type == msgType.update_entries.getValue())
                        {
                            outgoing.writeUTF("ACK");
                            
                            String str = tok[2];
                            Log.i("SERVER","UPDATE_ENTRIES: str="+str);
                            String[] subTok = str.split(":");

                            for(String st: subTok)
                            {
                                Log.i("SERVER","key="+st);
                                String[] subSubTok = st.split("-");

                                String sstkey = subSubTok[0];
                                hashMap.put(sstkey, subSubTok[1]);
                            }

                        }
/**************************************************************************************************/
                        /* 4. INSERT */
                        else if (type == msgType.insert.getValue())
                        {
                            outgoing.writeUTF("ACK");
                            String str = tok[1];
                            String[] subTok = str.split("-");

                            String skey = subTok[0];
                            String svalue = subTok[1];


                            //String hashPort = genHash(Integer.toString(nodeMap.get(Integer.parseInt(GOD_PORT))));

                            Log.i(TAG,"INSERT / startNode: "+node1_port+" " +
                                                    ", successor: [ " + successorPort + " , "+successorId + " ]"+
                                                    ", predecessor: [ " + predecessorPort + ", "+predecessorId +"]");

                            //String temp = genHash(Integer.toString(nodeMap.get(11108)));
                            //boolean temp1 = myId.equals(temp);
                            //int temp2 = key.compareTo(predecessorId);
                            //Log.i(TAG,"blah blah blah : a="+temp1+" ,b="+temp2);

                            String hk = genHash(skey);
                            boolean isValidHash = validateHash(hk);

                            // if isValidHash == true, then msg was for the self node else forward to the appropriate node //
                            if(isValidHash){
                                hashMap.put(skey, svalue);
                                Log.i("SERVER", "INSERT/SELF: key= "+ skey + ", value= "+svalue);
                            }else{
                                publishProgress(Integer.toString(msgType.insert.getValue()),rawStr, successorPort, myPort);
                                Log.i("SERVER", "INSERT/FORWARD: dest_node= "+ successorPort +", source_node= "+ myPort +", hashKey=" + hk);
                            }
                        }
/**************************************************************************************************/
                        // 7. RESPONSE QUERY //
                        else if(type == msgType.rep_query.getValue())
                        {
                            outgoing.writeUTF("ACK");
                            String fromPort = tok[1];
                            String toPort = tok[2];
                            String resp = tok[3];

                            Log.i(TAG, "QUERY /RESPONSE: req="+fromPort+" ,port="+myPort+" ,response="+resp);
                            if(myPort.equals(fromPort)){
                                queryMap.put(toPort, resp);
                                Log.i(TAG, "Pushing response to queryMap!");
                            }
                        }
/**************************************************************************************************/
                        // 6. REQUEST QUERY //
                        else if(type == msgType.req_query.getValue())
                        {
                            outgoing.writeUTF("ACK");
                            String req = tok[1];
                            String sel = tok[2];

                            String resp = null;

                            // check if response is included or blank //
                            if (tok.length > 3){
                                resp = tok[3];
                            }

                            boolean addToReq = false;

                            //Case 1: add all query responses//
                            if(sel.equals("*"))
                            {
                                resp = resp + ":";

                                addToReq = true;
                                for(String hmk2: hashMap.keySet())
                                {
                                    //format-> key-value:key-value:key-value: .. :key-value //
                                    resp = resp + hmk2 + "-" +hashMap.get(hmk2)+":";
                                }

                                resp = resp.substring(0, resp.length()-1);

                            }
                            else // key specific //
                            {
                                // single key //
                                if(hashMap.get(sel) != null)
                                {
                                    resp = sel + "-" + hashMap.get(sel);
                                }
                                else
                                {
                                    addToReq = true;
                                }
                            }

                            Log.i("SERVER","QUERY_REQUEST: req="+req+" , successorPort="+successorPort+" ,asreq="+addToReq);


                            if(req.equals(successorPort) && addToReq == true)
                            {
                                addToReq = false;
                            }

                            if(addToReq)
                            {
                                publishProgress(Integer.toString(msgType.req_query.getValue()), resp, req, sel, myPort);
                                Log.i(TAG, "QUERY_REQUEST: sending response="+resp);
                            }else
                            {
                                publishProgress(Integer.toString(msgType.rep_query.getValue()), resp, req, sel, myPort);
                                Log.i(TAG, "QUERY_RESPONSE: sending response="+resp);
                            }
                        }
/**************************************************************************************************/
                        // 4. DELETE  //
                        else if (type == msgType.delete.getValue())
                        {
                            outgoing.writeUTF("ACK");
                            String dkey = tok[1];
                            boolean toBeFwd = false; // check if forward the request //

                            //CASE 1: REMOVE ALL AND FORWARD FOR OTHER REMOVALS //
                            if(dkey.equals("*"))
                            {
                                hashMap.clear();
                                toBeFwd = true;
                            }
                            else
                            {   //CASE 2A: KEY SPECIFIC //
                                if(hashMap.containsKey(dkey))
                                {
                                    hashMap.remove(dkey);
                                }
                                // CASE 2B: KEY SPECIFIC, BUT KEY NOT FOUND IN SELF NODE //
                                else
                                {
                                    toBeFwd = true;
                                }
                                
                            }  
                            if(toBeFwd)
                            {
                                publishProgress(Integer.toString(msgType.delete.getValue()), dkey, successorPort, myPort);
                            }
                           
                        }
/**************************************************************************************************/
                    }

                    if (outgoing != null) {outgoing.close();}
                    if (incoming != null) {incoming.close();}


                }catch(EOFException eofe){
                }catch(Exception e)
                {
                    e.printStackTrace();
                }
                try{
                    ss.close();
                }catch(IOException ioe)
                {
                    ioe.printStackTrace();
                }
                
            }while(ss.isConnected());

            return null;
        }

        protected void onProgressUpdate(String... values) {
            // msg type //
            int type = Integer.parseInt(values[0]);
            //String key = values[1];
            //String value = values[2];
/*            // create new Message object //
            Message msg = new Message();

            // update the Message object //
            msg.setType(type);
            msg.setKey(values[0]);
            msg.setValue(values[1]);
*/
            // 1. new node wants to join
            if(type == msgType.req_join.getValue())
            {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,  Integer.toString(msgType.req_join.getValue()), myPort);
            }

            // 2. once new node has joined, all predecessors and successors need to be updated
            else if(type == msgType.update_nodes.getValue())
            {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,  Integer.toString(msgType.update_nodes.getValue()), values[1], values[2], myPort);

                //key = values[3];
                //value = values[4];
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,   Integer.toString(msgType.update_nodes.getValue()),values[3], values[4], myPort);
            }

            // 3. update all entries corresponding to the nodes and the newly joined node //
            else if(type == msgType.update_entries.getValue())
            {
                // if hashMap is empty, then no entries so far //
                if(!hashMap.isEmpty())
                {
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,  Integer.toString(msgType.update_entries.getValue()),values[1], values[2], myPort);
                }
            }
            // 4. insert values //
            else if(type == msgType.insert.getValue())
            {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,  Integer.toString(msgType.insert.getValue()), values[1] , values[2], myPort);

            }
            // 6,7. Handle query requests and response
            else if(type == msgType.req_query.getValue())
            {
                //String response = values[3];
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,  Integer.toString(msgType.req_query.getValue()), values[1] , values[2], values[3], myPort);
            }
            // 7. what he said above!
            else if(type == msgType.rep_query.getValue())
            {
                //String response = values[3];
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,  Integer.toString(msgType.rep_query.getValue()), values[1] , values[2], values[3], myPort);
            }
            // 5. delete values //
            else if(type == msgType.delete.getValue())
            {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,  Integer.toString(msgType.delete.getValue()), values[1] , values[2], myPort);

            }
            else
            {
                // default case
                Log.e(TAG, "UNKNOWN MSGTYPE!");
            }

            return;
        }

    } /* end of ServerTask class */




    /** Client Task **/
    private class ClientTask extends AsyncTask<String, Void , Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            String remotePort = null;
            String request = null;

            Socket socket = null;
            int mtype = Integer.parseInt(msgs[0]);
/***********************************************************************************************************/
            // 1. JOIN REQUESTS
            if(mtype == msgType.req_join.getValue())
            {
                // REMEMBER 11108 IS GOD NODE //
                remotePort = Integer.toString(11108);
                request = Integer.toString(msgType.req_join.getValue())+";"+myPort+";"+myId;
                Log.i("CLIENT","JOIN/REQUEST: request="+request+" , remotePort="+remotePort);
            }
/***********************************************************************************************************/
            // 2. UPDATE //
            else if(mtype == msgType.update_nodes.getValue() || mtype == msgType.update_entries.getValue())
            {
                request = msgs[1];
                remotePort = msgs[2];

                Log.i("CLIENT","UPDATE/NODE,ENTRY: request="+request+" , remotePort="+remotePort);
            }
/***********************************************************************************************************/
            // 3. INSERT //
            else if(mtype == msgType.insert.getValue())
            {
                request = msgs[1];
                remotePort = msgs[2];
                Log.i("CLIENT","INSERT: request="+request+" , remotePort="+remotePort);
            }
/***********************************************************************************************************/
            // 4. QUERY REQUEST, to be circulated through successor //
            else if(mtype == msgType.req_query.getValue())
            {
                request = msgType.req_query.getValue()+";"+msgs[2]+";"+msgs[3]+";"+msgs[1];
                remotePort = successorPort;
                //Log.i("CLIENT","REQUEST msgs = "+msgs[2] +"-->"+msgs[3]+"-->"+msgs[1]);
                Log.i("CLIENT","QUERY/REQUEST: request="+request+" , remotePort="+remotePort);
            }
/***********************************************************************************************************/
            // 5. QUERY RESPONSE //
            else if(mtype == msgType.rep_query.getValue())
            {
                request = msgType.rep_query.getValue()+";"+msgs[2]+";"+msgs[3]+";"+msgs[1];
                remotePort = msgs[2];
                //Log.i("CLIENT","RESPONSE msgs = "+msgs[2] +"-->"+msgs[3]+"-->"+msgs[1]);
                Log.i("CLIENT","QUERY/RESPONSE: request="+request+" , remotePort="+remotePort);
            }
/***********************************************************************************************************/
            // 6. DELETE //
            else if(mtype == msgType.delete.getValue())
            {
                request = msgs[1];
                remotePort = msgs[2];
                Log.i("CLIENT","DELETE: request="+request+" , remotePort="+remotePort);
            }
            else{
                Log.e("CLIENT","UNKNOWN MSGTYPE!");
            }

            Log.i("CLIENT","FINAL : request="+request+" , reemotePort="+remotePort);
            
            try {
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
                Log.i("CLIENT","Socket Created:"+socket);

                DataInputStream incomingClient = new DataInputStream(socket.getInputStream());
                DataOutputStream outgoingClient = new DataOutputStream(socket.getOutputStream());

                String reply = "NACK";

                // send the request //
                // format: type;request;remotePort //
                outgoingClient.writeUTF(request);
                
                Log.i("CLIENT","REQUEST SENT: request="+request);


                // received ACKs , hold till ACK is not received //
                do
                {
                    reply = incomingClient.readUTF();
                    Log.i("CLIENT","REPLY received, reply="+reply);
                }while(reply.equals("NACK"));

                // if newly joined nodes was success, update current nodes successor and predecessor info //
                if(mtype == msgType.req_join.getValue())
                {
                    Log.i("CLIENT","JOIN_REQUEST: reply="+reply);
                    if(!reply.equals("NACK"))
                    {
                        String[] jtok = reply.split(";");

                        predecessorPort = jtok[1];
                        successorPort = jtok[2];

                        predecessorId = genHash(Integer.toString(nodeMap.get(Integer.parseInt(predecessorPort))));
                        successorId = genHash(Integer.toString(nodeMap.get(Integer.parseInt(successorPort))));

                        Log.i("CLIENT", "NODE UPDATE: successor<port,id>=[ "+successorPort+" , "+successorId+" ] ,predecessor<port,id>=[ "+predecessorPort+" , "+predecessorId+" ]");
                    }
                }
                else
                {
                    Log.i("CLIENT","ACK: type="+mtype);
                }

                if(outgoingClient!= null)
                {
                    outgoingClient.close();
                }

                if(incomingClient!= null)
                {
                    incomingClient.close();
                }
                
                
            } catch (Exception e) {
                e.printStackTrace();
                try{
                    socket.close();
                }catch(IOException ie){
                  ie.printStackTrace();
                }
            }
            return null;
        }
    }
}
