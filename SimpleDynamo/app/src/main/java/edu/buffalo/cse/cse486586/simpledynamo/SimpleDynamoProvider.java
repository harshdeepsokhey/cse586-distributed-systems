/*
* @filename: SimpleDynamoProvider.java

* @description: 
** Contains methods for implementing the concurrent addition, deletion and query of nodes 
** Implements Selection Quorum Stratergy 

* @author: Harshdeep Sokhey <hsokhey@buffalo.edu>
*/

package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.opengl.Matrix;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static android.content.Context.INPUT_METHOD_SERVICE;
import static android.content.Context.TELEPHONY_SERVICE;


// comparator //
class TSComparator implements Comparable<TSComparator> {

    public String value;
    public long ts;

    @Override
    public int compareTo(TSComparator another) {
        int diff = (int)(this.ts - another.ts);
        return diff;
    }
}


public class SimpleDynamoProvider extends ContentProvider {

    /*@Ref: From the PA 3: SimpleDht */

    static Uri myUri = null;
    String myPort = null;
    String myId = null;

    // nodeMap <port, id> //
    private HashMap<Integer, Integer> nodeMap = new HashMap<Integer, Integer>();

    // nodeHashMap <hashOfId , port> //
    private HashMap<String, Integer> nodeHashMap = new HashMap<String, Integer>();

    static final String TAG = SimpleDynamoProvider.class.getSimpleName();
    static final String TAGC = ServerTask.class.getSimpleName();
    static final String TAGS = ClientTask.class.getSimpleName();

    private SimpleDynamoDBHelper db = null;

    static Lock lock = new ReentrantLock();

    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";

    static final String[] remotePorts = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};

    int TIMEOUT = 1000;

    Integer replicaA = 0;
    Integer replicaB = 0;


    public enum msgType {
        req_join(1),
        insert(2),
        req_query(3), rep_query(4),
        delete(5),
        replicate(6);

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

        Log.i(TAG,"DELETE: type="+Integer.toString(msgType.delete.getValue())+" ,key="+selection);

        // Case 1:  DELETE SELF KEY-VALUE PAIRS : delete self only//
        if(selection.equals("@"))
        {
            // hashMap.clear();
            db.dbDelete(selection);
        }
        // Case 2: DELETE ALL KEY-VALUE PAIRS : delete self and forward the delete request to successorPort //
        else if(selection.equals("*"))
        {
            //hashMap.clear();
            db.dbDelete(selection);
            for(int id=0; id < remotePorts.length;id++)
            {
                if(remotePorts[id].equals(myPort))
                    continue;

                msgSenderDelete(selection,remotePorts[id]);
            }
        }
        // Case 3: KEY-SPECIFIC: delete only the key-value for the given key in the hashMap //
        else
        {
            //key specific logic
            db.dbDelete(selection);

        }
		return 0;
	}

/*****************************************************************************************************/
	public void msgSenderDelete(String selection, String remotePort)
    {

        String request = Integer.toString(msgType.delete.getValue()) +";"+selection;
        DataInputStream incomingDel = null;
        DataOutputStream outgoingDel= null;
        Socket socket = null;


        try
        {
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
            socket.setSoTimeout(1000);

            incomingDel = new DataInputStream(socket.getInputStream());
            outgoingDel = new DataOutputStream(socket.getOutputStream());

            String response = "NACK";

            outgoingDel.writeUTF(request);


            Log.i(TAG,"REQUEST / DELETE sent : key="+request+" , value="+remotePort);


            do {
                response = incomingDel.readUTF();
                Log.i(TAG,"Waiting for ACK : status:"+response);
            }while(response.equals("NACK"));


            socket.close();

            if(incomingDel!= null) incomingDel.close();
            if(outgoingDel!= null) outgoingDel.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /*****************************************************************************************************/
    /* MessageSenders */
    public void msgSenderInsert(String ... msg) throws IOException {
        DataInputStream incomingIns = null;
        DataOutputStream outgoingIns = null;
        Socket socket = null;

        String request = msg[0];
        String remotePort = msg[1];

        try {
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
            socket.setSoTimeout(1000);

            incomingIns = new DataInputStream(socket.getInputStream());
            outgoingIns = new DataOutputStream(socket.getOutputStream());

            String response = "NACK";

            outgoingIns.writeUTF(request);

            Log.i(TAG,"REQUEST / DELETE sent : key="+request+" , value="+remotePort);

            do {
                response = incomingIns.readUTF();
                Log.i(TAG,"Wait for ACK : status="+response);
            }while(response.equals("NACK"));

            socket.close();

            if(incomingIns!= null) incomingIns.close();
            if(outgoingIns!= null) outgoingIns.close();

        } catch (Exception e) {
            if(incomingIns!= null) incomingIns.close();
            if(outgoingIns!= null) outgoingIns.close();
            socket.close();

        }

    }


	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}


    public String getPredecessorToCurrentNode(String key)
    {
        List<String> nodeHashList = new ArrayList<String>();
        for(String k: nodeHashMap.keySet())
        {
            nodeHashList.add(k);
        }

        nodeHashList.add(key);
        Collections.sort(nodeHashList);

        int keyIndex = 0;
        for(int i=0; i< nodeHashList.size(); i++)
        {
            if(nodeHashList.get(i).equals(key))
            {
                keyIndex = i;
                break;
            }
        }

        // for start node , wrap around //
        if((keyIndex - 1) < 0)
        {
            keyIndex = (nodeHashList.size()+ keyIndex -1)% nodeHashList.size();
        }

        return (nodeHashList.get(keyIndex));
    }


    public String getKeyIndex(String key)
    {
        List<String> nodeHashList = new ArrayList<String>();
        for(String k: nodeHashMap.keySet())
        {
            nodeHashList.add(k);
        }

        nodeHashList.add(key);

        Collections.sort(nodeHashList);

        int index = -1;
        for(int i =0; i < nodeHashList.size(); i++)
        {
            if(nodeHashList.get(i).equals(key))
            {
                index = i;
                break;
            }
        }

        String result = (nodeHashList.get((index+1)%nodeHashList.size())+";"+nodeHashList.get((index+2)%nodeHashList.size())+";"+nodeHashList.get((index+3)%nodeHashList.size()));
        return result;
    }


    @Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		String key = (String) values.get("key");
		String value = (String) values.get("value");

        Log.i(TAG,"Content Values : key="+ key + ",value="+ value);

        String hash = null;
        String keyIndex = null;

        try
        {
            /* generate hash for key */
            hash = genHash(key);
            keyIndex = getKeyIndex(hash);
        } catch (NoSuchAlgorithmException nsae) {
            nsae.printStackTrace();
        }

        Log.i(TAG, "hashed value of the key: key="+hash);
        String[] idxTok = keyIndex.split(";");

        Integer toPort = nodeHashMap.get(idxTok[0]);
        long ts = System.currentTimeMillis();


        String[] nodes ={   Integer.toString(nodeHashMap.get(idxTok[0])),   // current
                            Integer.toString(nodeHashMap.get(idxTok[1])),   // replica # 1
                            Integer.toString(nodeHashMap.get(idxTok[2]))};  // replica # 2


        String request = Integer.toString(msgType.insert.getValue())+";"+key+"-"+value+"-"+ts;

        for(int i=0; i < nodes.length; i++)
        {
            if(nodes[i].equals(myPort))
            {
                dbInsertWrapper(key, value, ts);
                Log.i(TAG, "key-value inserted to itself!");
            }
            else
            {
                try {
                    msgSenderInsert(request, nodes[i]);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                Log.i(TAG, "key-value inserted to node:"+nodes[i]);
            }
        }
        return null;
    }



	@Override
	public boolean onCreate() {

        /*@Ref: From the previous assignments */
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr)*2));
        myUri = buildUri("content","content://edu.buffalo.cse.cse486586.simpledynamo.provider");

        try
        {
            ServerSocket serverSocket = new ServerSocket(10000);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

        }catch (Exception e)
        {
            Log.e(TAG, "Error: Server Socket Creation Failed!");
            return false;
        }
        Log.i(TAG, "SERVER / Info: Started!");


            // hash(id) -> portNo
        try {
            nodeHashMap.put(genHash("5554"), 11108);
            nodeHashMap.put(genHash("5556"), 11112);
            nodeHashMap.put(genHash("5558"), 11116);
            nodeHashMap.put(genHash("5560"), 11120);
            nodeHashMap.put(genHash("5562"), 11124);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

            // port -> id
            nodeMap.put(11108, 5554);
            nodeMap.put(11112, 5556);
            nodeMap.put(11116, 5558);
            nodeMap.put(11120, 5560);
            nodeMap.put(11124, 5562);

        try
        {
            myId = genHash(Integer.toString(nodeMap.get(Integer.parseInt(myPort))));
        } catch (NoSuchAlgorithmException nsae) {
            nsae.printStackTrace();
        }
            db = new SimpleDynamoDBHelper(getContext());

            if(db.doesExist())
            {
                Log.i(TAG, "ALREADY EXISTS!");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(msgType.req_join.getValue()), myPort);
            }

            String nodeIndex = getNodeIndex(myId);
            String[] nIdTok = nodeIndex.split(";");
            replicaA = nodeHashMap.get(nIdTok[0]);
            replicaB = nodeHashMap.get(nIdTok[1]);

            Log.i(TAG, "onCreate / currentNode:"+myId+", nextNode:"+replicaA+", nextNextNode:"+replicaB);



        return false;
	}

    public String getNodeIndex(String key) {
        List<String> nodeHashList = new ArrayList<String>();
        for(String k: nodeHashMap.keySet())
        {
            nodeHashList.add(k);
        }

        Collections.sort(nodeHashList);
        int nodeIdx = -1;
        for(int i=0; i < nodeHashList.size(); i++)
        {
            if((nodeHashList.get(i)).equals(key))
            {
                nodeIdx = i;
                break;
            }
        }

        String result = (nodeHashList.get((nodeIdx+1)%nodeHashMap.size()))+";"+(nodeHashList.get((nodeIdx+2)%nodeHashMap.size()))+";"+(nodeHashList.get((nodeIdx+3)%nodeHashMap.size()));

        return result;
    }

    @Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {

        MatrixCursor curs = null;

        if(selection.equals("@"))
        {
            Log.i(TAG, "QUERY / SOME : selection="+selection);
            curs = (MatrixCursor) queryHandler(selection);

        }
        else if(selection.equals("*"))
        {
            Log.i(TAG, "QUERY / ALL : selection="+selection);

            curs = (MatrixCursor) queryHandler(selection);

            for(int i=0; i < remotePorts.length; i++)
            {
                if(Integer.parseInt(remotePorts[i]) == Integer.parseInt(myPort)) continue;


                String response = queryALLHandler(selection, remotePorts[i]);

                if(response == null || response.equals("") || response.equals("NACK")) continue;

                String[] tok = response.split(";");

                for(int j=0; j < tok.length; j++)
                {
                    String[] subTok = tok[j].split("-");
                    String pk = subTok[0];
                    String pv = subTok[1];
                    long pTs = Long.parseLong(subTok[2]);

                    curs.addRow(new Object[]{pk, pv});

                    try
                    {
                        if (validateHash(genHash(pk), genHash(myPort)))
                        {
                            dbInsertWrapper(pk, pv, pTs);
                        }
                    } catch (NoSuchAlgorithmException e) {
                        e.printStackTrace();
                    }

                    //dbUpdateWrapper(pk, pv,pTs);
                }
            }
        }
        else
        {
            //key specific
            Log.i(TAG, "QUERY / KEY SPECIFIC");
            String response2 ="";

            try
            {
                response2 = getKeyIndex(genHash(selection));
            } catch (NoSuchAlgorithmException e)
            {
                e.printStackTrace();
            }
            String[] kstok = response2.split(";");

            String[] ksreplicaNodes = {Integer.toString(nodeHashMap.get(kstok[0])),
                                    Integer.toString(nodeHashMap.get(kstok[1])),
                                    Integer.toString(nodeHashMap.get(kstok[2]))};

            String ksv = null;
            long ksts = 0;

            for(int rId = 0; rId < ksreplicaNodes.length; rId++)
            {
                if(ksreplicaNodes[rId].equals(myPort))
                {
                    Cursor ksc = dbQueryWrapper(selection);
                    if(ksc != null && ksc.getCount()>0)
                    {
                        ksc.moveToFirst();
                        ksv = ksc.getString(ksc.getColumnIndex("value"));
                        ksts = ksc.getLong(ksc.getColumnIndex("ts"));
                    }
                }
                else
                {
                    String response3 = queryALLHandler(selection, ksreplicaNodes[rId]);

                    if(response3 == null || response3.equals("NACK") || response3.equals("")) continue;

                    String[] rTok = response3.split("-");
                    long rTs = Long.parseLong(rTok[2]);
                    if(ksts <= rTs)
                    {
                        ksv = rTok[1];
                        ksts = rTs;
                    }

                }
            }

            curs = new MatrixCursor(new String[]{"key", "value"});
            curs.moveToFirst();
            curs.addRow(new Object[]{selection, ksv});
        }

        Log.i(TAG,"blah blah ="+curs.getCount());
        return curs;
	}


    public String queryALLHandler(String selection, String remotePort) {

        Socket socket = null;
        String response5 = "NACK";
        String request=null;

        try {
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
            socket.setSoTimeout(1000);

            DataInputStream incomingQ = new DataInputStream(socket.getInputStream());
            DataOutputStream outgoingQ = new DataOutputStream(socket.getOutputStream());

            request = Integer.toString(msgType.req_query.getValue())+";"+myPort+";"+selection;

            Log.i(TAG, "QUERY ALL HANDLER : request="+request+ ", remotePort="+remotePort);
            outgoingQ.writeUTF(request);

            do
            {
                response5 = incomingQ.readUTF();
                Log.i(TAG, "Wait on response: status:"+response5);
            }while(response5.equals("NACK"));

            if(incomingQ != null) incomingQ.close();
            if(outgoingQ != null) outgoingQ.close();

            socket.close();

        } catch (Exception e) {
            e.printStackTrace();
            Log.i(TAG, "Exception="+e);
            response5 = null;
        }

        return response5;
    }


    public Cursor queryHandler(String selection)
    {
        // snapshot-1
        HashMap<String, TSComparator> ss1Map = new HashMap<String, TSComparator>();
        Cursor curs = dbQueryWrapper("*");

        if (curs!=null && curs.getCount()>0)
        {
            curs.moveToFirst();
            do
            {
                TSComparator tsc = new TSComparator();
                String qhK = curs.getString(curs.getColumnIndex("key"));
                tsc.value = curs.getString(curs.getColumnIndex("value"));
                tsc.ts = curs.getLong(curs.getColumnIndex("ts"));

                ss1Map.put(qhK, tsc);

            }while(curs.moveToNext());
        }

        String[] qhreplicaNodes = {Integer.toString(replicaA), Integer.toString(replicaB)};

        if(selection.equals("@"))
        {
            selection = "*";
        }

        for(int kid=0; kid < qhreplicaNodes.length; kid++)
        {
            Log.i(TAG,"QUERY/ @ : Polling: selection:"+selection+", replica:"+qhreplicaNodes[kid]+",status:"+1);

            // polling //
            String response4 = pollingHandler(selection, qhreplicaNodes[kid], 1);
            Log.i(TAG, "hehehe ="+response4);



            // snap shot 2 //
            HashMap<String, TSComparator> ss2Map = new HashMap<String, TSComparator>();
            if(response4.equals("NACK") || response4.equals("")) continue;

            String [] qhtok = response4.split(";");

            for(int i=0; i < qhtok.length; i++)
            {
                TSComparator tsc1 = new TSComparator();

                String [] qhsubTok = qhtok[i].split("-");
                String stkey = qhsubTok[0];
                tsc1.value = qhsubTok[1];
                tsc1.ts = Long.parseLong(qhsubTok[2]);

                ss2Map.put(stkey, tsc1);
            }

            Log.i(TAG,"ss1Map :"+ss1Map.keySet().toString());
            Log.i(TAG,"ss2Map :"+ss2Map.keySet().toString());

            HashMap<String, TSComparator> temp = new HashMap<String, TSComparator>();
            for(String keyss2: ss2Map.keySet())
            {
                long ts1 = 0;
                long ts2 = 0;
                try
                {
                    if(ss1Map.containsKey(keyss2))
                    {
                        ts1 = ss1Map.get(keyss2).ts;
                    }

                    ts2 = ss2Map.get(keyss2).ts;

                    if((validateHash(genHash(keyss2), myId)) && (ts1 <= ts2))
                    {
                        temp.put(keyss2, ss2Map.get(keyss2));
                        dbInsertWrapper(keyss2, ss2Map.get(keyss2).value, ts2);
                    }
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
            }

            for(String keyT: temp.keySet())
            {
                ss1Map.remove(keyT);
                ss1Map.put(keyT, temp.get(keyT));

            }
        }

        MatrixCursor mc1 = new MatrixCursor(new String[]{"key", "value"});
        for(String keyss1: ss1Map.keySet())
        {
            mc1.moveToFirst();
            mc1.addRow(new Object[]{keyss1, ss1Map.get(keyss1).value});
            dbInsertWrapper(keyss1, ss1Map.get(keyss1).value, ss1Map.get(keyss1).ts);
        }

        return mc1;
    }

    // status = poll status
    private String pollingHandler(String selection, String replicaNode, int status)
    {
        Socket socket =null;
        String response = "NACK";
        String qKey = selection;
        String request="";
        boolean isCrashed = false;

        try
        {
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(replicaNode));
            socket.setSoTimeout(1000);

            DataInputStream incomingP = new DataInputStream(socket.getInputStream());
            DataOutputStream outgoingP = new DataOutputStream(socket.getOutputStream());

            request = Integer.toString(msgType.req_query.getValue())+";"+myPort+";"+selection+";"+status;

            response = "NACK";
            outgoingP.writeUTF(request);

            do
            {
                response = incomingP.readUTF();
                Log.i(TAG, "Wait on response: status:"+response);
            }while(response.equals("NACK"));

            if(incomingP != null) incomingP.close();
            if(outgoingP != null) outgoingP.close();

            socket.close();

        } catch (SocketTimeoutException ste) {
            isCrashed = true;
        } catch (Exception e)
        {
            e.printStackTrace();
        }

        return response;
    }

    @Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException
    {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    /* @Ref: from previous Programming Assignments */
    private Uri buildUri(String scheme, String authority)
    {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    /*************************************************************************************************/
    public boolean validateHash(String key, String value)
    {
        List<String> nodeHashList = new ArrayList<String>();
        for(String k: nodeHashMap.keySet())
        {
            nodeHashList.add(k);
        }

        nodeHashList.add(key);
        Collections.sort(nodeHashList);

        // check if key matches
        int keyFoundAtIdx = 0;
        for(int i=0;i< nodeHashList.size(); i++)
        {
            if(nodeHashList.get(i).equals(key))
            {
                keyFoundAtIdx = i;
                break;
            }
        }

        // check if value matches
        boolean result = false;
        for(int j=1; j < 4; j++)
        {
            if(nodeHashList.get((keyFoundAtIdx+j)%nodeHashList.size()).equals(value))
            {
                result = true;
                break;
            }
        }

        return result;
    }




    /*************************************************************************************************/
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets)
        {
            ServerSocket serverSocket = sockets[0];
            Socket ss = null;

            do
            {
                try {
                    /* accept incoming connection */
                    ss = serverSocket.accept();

                    /* create 2 stream for input and output */
                    DataInputStream incoming = new DataInputStream(ss.getInputStream());
                    DataOutputStream outgoing = new DataOutputStream(ss.getOutputStream());

                    String rawstr = null;

                    while(null != (rawstr = incoming.readUTF()))
                    {
                        Log.i(TAGS, "RAW_STRING: "+rawstr);
                        String[] tok = rawstr.split(";");

                        // messageType //
                        int type = Integer.parseInt(tok[0]);

                        Log.i(TAGS, "Incoming message: type="+type+", rawStr="+rawstr);

/****************************************************************************************************/
                        // CASE 1: JOIN REQUEST //
                        if( type == msgType.req_join.getValue())
                        {
                            Log.i(TAGS,"JOIN");
                            String key = tok[1];
                            String value = tok[2];

                            List<String> nodeList = new ArrayList<String>();

                            for(String nodeKey : nodeHashMap.keySet())
                            {
                                nodeList.add(nodeKey);
                            }

                            nodeList.add(value);
                            Collections.sort(nodeList);

                            int index = 0;
                            for(int iter=0; iter < nodeList.size(); iter++)
                            {
                                if(nodeList.get(iter).equals(value))
                                {
                                    index = iter;
                                    break;
                                }
                            }

                            boolean prevFound = false;

                            int prevA = index -1;
                            if(prevA < 0) { prevA = nodeList.size() + prevA; }

                            int prevB = index -2;
                            if(prevB < 0) { prevB = nodeList.size() + prevB; }

                            // check if the AVD is the previous nodes are associated with the current node
                            if(myId.equals(nodeList.get(prevA % nodeList.size())) || myId.equals(nodeList.get(prevB % nodeList.size())))
                            {
                                prevFound = true;
                            }

                            String predecssorPort = getPredecessorToCurrentNode(myId);
                            String response = "";

                            Cursor fromDb = dbQueryWrapper("*");

                            if(fromDb != null && fromDb.getCount()>0)
                            {
                                fromDb.moveToFirst();
                                do
                                {
                                    String kk = fromDb.getString(fromDb.getColumnIndex("key"));
                                    String vv = fromDb.getString(fromDb.getColumnIndex("value"));
                                    long ts = fromDb.getLong(fromDb.getColumnIndex("ts"));

                                    boolean isValidHash = validateHash(genHash(kk), value);

                                    if(isValidHash)
                                    {
                                        response = response + kk + "-"+ vv +"-"+ts+";";
                                    }

                                }while(fromDb.moveToNext());

                                if(response.length() > 0)
                                {
                                    response = response.substring(0, response.length() -1);
                                }

                            }

                            // check if it can be initialized above!
                            if(0 == response.length())
                            {
                                response = "NACK";
                            }

                            outgoing.writeUTF(response);

                        }
/****************************************************************************************************/
                        else if(type == msgType.insert.getValue())
                        {
                            //outgoing.writeUTF("ACK");
                            String str = tok[1];
                            String[] subTok = str.split("-");
                            String key = subTok[0];
                            String value = subTok[1];
                            long ts = Long.parseLong(subTok[2]);

                            Log.i(TAGS, "SERVER /INSERT : key="+key+" ,value="+value+" ,timestamp="+ts);
                            dbInsertWrapper(key, value, ts);
                            outgoing.writeUTF("ACK");
                        }
/****************************************************************************************************/
                        else if(type == msgType.rep_query.getValue())
                        {
                            outgoing.writeUTF("ACK");
                            String fromPort = tok[1];
                            String toPort = tok[2];
                            String response = tok[3];
                            Log.i(TAGS, "QUERY RESPONSE status=Received , fromPort="+fromPort+" , toPort="+toPort+", response="+response);

                            if(myPort.equals(fromPort))
                            {
                                Log.i(TAGS, "QUERY RESPONSE status=Accepted!");
                            }
                        }
/****************************************************************************************************/
                        else if(type == msgType.req_query.getValue())
                        {
                            // outgoing.writeUTF("ACK");
                            String req = tok[1];
                            String sel = tok[2];
                            Cursor q1Rec = null;
                            Cursor q2Rec = null;
                            String resp = "";

                            if(sel.equals("*"))
                            {
                                q1Rec = dbQueryWrapper(sel);
                                Log.i(TAGS,"QUERY REQUEST / ALL : qRecCount="+q1Rec.getCount());
                                if((q1Rec != null) && (q1Rec.getCount() > 0))
                                {
                                    q1Rec.moveToFirst();
                                    do
                                    {
                                        String kk = q1Rec.getString(q1Rec.getColumnIndex("key"));
                                        String vv = q1Rec.getString(q1Rec.getColumnIndex("value"));
                                        long tts = q1Rec.getLong(q1Rec.getColumnIndex("ts"));

                                        resp = resp + kk +"-"+vv+"-"+tts+";";
                                    }while(q1Rec.moveToNext());
                                    resp = resp.substring(0, resp.length() -1);
                                }
                            }
                            else
                            {
                                q2Rec = dbQueryWrapper(sel);
                                Log.i(TAGS,"QUERY REQUEST / KEY-SPECIFIC: qRecCount="+q2Rec.getCount());

                                if((q2Rec != null) && (q2Rec.getCount()>0))
                                {
                                    q2Rec.moveToFirst();
                                    String kk = q2Rec.getString(q2Rec.getColumnIndex("key"));
                                    String vv = q2Rec.getString(q2Rec.getColumnIndex("value"));
                                    long tts = q2Rec.getLong(q2Rec.getColumnIndex("ts"));

                                    resp = kk+"-"+vv+"-"+tts;
                                }
                            }
                            Log.i(TAGS, "QUERY_REQUEST / SENT : response="+resp);
                            outgoing.writeUTF(resp);
                        }
/****************************************************************************************************/
                        else if(type == msgType.delete.getValue())
                        {
                            outgoing.writeUTF("ACK");
                            String dKey = tok[1];
                            db.dbDelete(dKey);

                        }
/****************************************************************************************************/
                        else if(type == msgType.replicate.getValue())
                        {
                            outgoing.writeUTF("ACK");
                            String rKey = tok[1];
                            Log.i(TAGS, "REPLICATE / key="+rKey);
                            String[] subTok = rKey.split(";");
                            String[] subSubTok = subTok[0].split("-");

                            String sstKey = subSubTok[0];
                            String sstValue = subSubTok[1];
                            long sstTs = Long.parseLong(subSubTok[2]);

                            dbInsertWrapper(sstKey, sstValue, sstTs);
                        }
/****************************************************************************************************/
                    }


                    if (outgoing != null) {outgoing.close();}
                    if (incoming != null) {incoming.close();}


                } catch (EOFException eofe) {
                    eofe.printStackTrace();

                } catch (IOException e) {
                    try {
                        ss.close();
                    } catch (IOException e1) {
                        e1.printStackTrace();
                    }
                    e.printStackTrace();
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }

            }while(ss.isConnected());

            return null;
        }


        protected void onProgressUpdate(String... values)
        {
            int type = Integer.parseInt(values[0]);

            if(type == msgType.req_join.getValue())
            {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,  Integer.toString(msgType.req_join.getValue()), myPort);
            }

            else if(type == msgType.insert.getValue())
            {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,  Integer.toString(msgType.insert.getValue()), values[1], values[2], myPort);
            }

            else if(type == msgType.req_query.getValue())
            {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,  Integer.toString(msgType.req_query.getValue()), values[1], values[2], values[3], myPort);
            }

            else if(type == msgType.rep_query.getValue())
            {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,  Integer.toString(msgType.rep_query.getValue()), values[1], values[2], values[3], myPort);
            }

            else if(type == msgType.delete.getValue())
            {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,  Integer.toString(msgType.delete.getValue()), values[1], values[2], myPort);
            }

            else if(type == msgType.replicate.getValue())
            {
                String[] ports = values[1].split(";");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,  Integer.toString(msgType.replicate.getValue()), ports[0], ports[1], ports[2], myPort);
            }

            else
            {
                // default case
                Log.e(TAGS, "UNKNOWN MSGTYPE!");
            }

            return;
        }

    }
    /*************************************************************************************************/
    public void dbInsertWrapper(String key, String value, long ts)
    {
        lock.lock();
        try
        {
            db.dbInsert(key,value,ts);
            Log.i(TAG, "dbInsert: key="+key+" ,value="+value+" ,ts="+ts);
        }finally {
            lock.unlock();
        }

    }

    public Cursor dbQueryWrapper(String key)
    {
        lock.lock();
        Cursor rec = null;
        try
        {
            rec = db.dbQuery(key);
            Log.i(TAG,"dbQuery: record count="+rec.getCount());

        }finally {
            lock.unlock();
        }

        return rec;

    }


    /*************************************************************************************************/
    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs)
        {
            //int remotePort = 0;
            String request = null;
            Socket socket = null;

            List<Integer> portList = new ArrayList<Integer>();

            try
            {
                int mtype = Integer.parseInt(msgs[0]);
/***************************************************************************************************/
                if (mtype == msgType.req_join.getValue())
                {
                    for (int id = 0; id < remotePorts.length; id++)
                    {
                        if (Integer.parseInt(remotePorts[id]) == Integer.parseInt(myPort))
                        {
                            continue;
                        }

                        portList.add(Integer.parseInt(remotePorts[id]));
                    }

                    request = Integer.toString(msgType.req_join.getValue()) + ";" + myPort + ";" + myId;
                    Log.i(TAGC, "JOIN/REQUEST : request=" + request);
                }
                /***************************************************************************************************/
                else if (mtype == msgType.insert.getValue()) {
                    request = Integer.toString(msgType.insert.getValue()) + ";" + msgs[1];
                    //remotePort = Integer.parseInt(msgs[2]);
                    portList.add(Integer.parseInt(msgs[2]));
                    Log.i(TAGC, "INSERT: request=" + request + " ,remotePort=" + msgs[2]);
                }
                /***************************************************************************************************/
                else if (mtype == msgType.req_query.getValue()) {
                    request = Integer.toString(msgType.req_query.getValue()) + ";" + msgs[2] + ";" + msgs[3] + ";" + msgs[1];
                    Log.i(TAGC, "QUERY/REQUEST: request=" + request);
                }
                /***************************************************************************************************/
                else if (mtype == msgType.delete.getValue()) {
                    //remotePort = Integer.parseInt(msgs[2]);
                    request = Integer.toString(msgType.delete.getValue()) + ";" + msgs[1];
                    portList.add(Integer.parseInt(msgs[2]));
                    Log.i(TAGC, "DELETE: request=" + request + " , remotePort=" + msgs[2]);
                }

                /****************************************************************************************************/
                else if (mtype == msgType.replicate.getValue()) {
                    //remotePort = Integer.parseInt(msgs[2]);
                    request = Integer.toString(msgType.replicate.getValue()) + ";" + msgs[1];
                    portList.add(Integer.parseInt(msgs[2]));
                    if (!(msgs[2].equals(msgs[3]))) {
                        portList.add(Integer.parseInt(msgs[3]));
                    }
                    Log.i(TAGC, "REPLICATE: request=" + request + " , remotePort=" + msgs[2] + ", replicas=" + msgs[3]);
                } else {
                    Log.e(TAGC, "UNKNOWN MSGTYPE!");
                }
                /****************************************************************************************************/
                //Log.i(TAGC, "FINAL : request=" + request + " , remotePort=" + msgs[2]);
                for (int x = 0; x < portList.size(); x++) {
                    String rp = Integer.toString(portList.get(x));

                    try
                    {
                        Log.i(TAGC,"remote port:"+rp+", type:"+mtype);
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(rp));
                        socket.setSoTimeout(1000);

                        Log.i(TAGC, "Socket Created:" + socket);

                        DataInputStream incomingClient = new DataInputStream(socket.getInputStream());
                        DataOutputStream outgoingClient = new DataOutputStream(socket.getOutputStream());

                        String response = "NACK";

                        outgoingClient.writeUTF(request);

                        Log.i(TAGC, "REQUEST SENT: request=" + request);

                        // received ACKs , hold till ACK is not received //
                        do
                        {
                            response = incomingClient.readUTF();
                            Log.i(TAGC, "Waiting for response, status=" + response);
                        } while (response.equals("NACK"));

                        // if newly joined nodes was success, update current nodes successor and predecessor info //
                        if (mtype == msgType.req_join.getValue()) {

                            Log.i("CLIENT", "JOIN_REQUEST: reply=" + response);
                            // check if other conditions are ever hit!
                            if (!response.equals("NACK") || !response.equals("") || response != null)
                            {
                                // @todo: put into a handler //
                                joinHandler(response);
                            } else
                            {
                                Log.e(TAGC, "Got NACK response: response: " + response);
                            }

                        } // from mtype client blah blah //
                        else
                        {
                            Log.i(TAGC, " Got ACK : type=" + mtype);
                        }

                        if (incomingClient != null) incomingClient.close();
                        if (outgoingClient != null) outgoingClient.close();

                        socket.close();

                    } catch (EOFException eofe1) {

                    } catch (SocketTimeoutException ste1) {
                        Log.e(TAGC, "Socket Timeout");
                    }


                } // for loop close //

            } catch (Exception e)
            {
                e.printStackTrace();
                try {
                    socket.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }

            return null;
        }

    }

    public void joinHandler(String response) {
        String[] tok = response.split(";");

        for (int i = 0; i < tok.length; i++)
        {
            String[] subTok = tok[i].split("-");
            String k = subTok[0];
            String v = subTok[1];
            long ts = Long.parseLong(subTok[2]);

            dbInsertWrapper(k, v, ts);
        }
    }

}