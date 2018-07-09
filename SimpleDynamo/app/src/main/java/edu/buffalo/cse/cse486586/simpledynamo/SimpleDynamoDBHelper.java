/*
* @filename: SimpleDynamoDBHelper.java

* @description:
** Contains methods for managing the database

* @author: Harshdeep Sokhey <hsokhey@buffalo.edu>
*/
package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;

public class SimpleDynamoDBHelper extends SQLiteOpenHelper{

    // constants 
    private static final String DATABASE_NAME = "Dynamo.db";
    private static final String TABLE_NAME = "DynamoDB";

    // queries 
    private static final String SQL_CREATE_ENTRIES = "CREATE TABLE " + TABLE_NAME + "(key TEXT PRIMARY KEY, value TEXT, ts BIGINT)";
    private static final String SQL_DELETE_ENTRIES = "DROP TABLE IS EXISTS '" + TABLE_NAME + "'";

    // constructor
    public SimpleDynamoDBHelper(Context context)
    {
        super(context, DATABASE_NAME, null, 1);
    }


    // methods 

    /* Check if entry exists */
    public boolean doesExist()
    {
        Cursor c= this.getReadableDatabase().rawQuery("select * from "+TABLE_NAME, null);
        return (c.getCount() > 0);
    }

    @Override
    /* Create DB on load */
    public void onCreate(SQLiteDatabase db) {
        Log.i("SQLDB","SQL DB created");
        db.execSQL(SQL_CREATE_ENTRIES);
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        this.dbDelete("*");
        db.execSQL(SQL_DELETE_ENTRIES);
        onCreate(db);
    }

    /* Insert Key-Value Pair into the DB */
    public void dbInsert(String key, String value, long ts)
    {
        String query = "select * from "+TABLE_NAME+" where key='"+key+"'";
        Cursor cursor = this.getReadableDatabase().rawQuery(query, null);
        long currentTs = 0;
        if(cursor.getCount() > 0)
        {
            cursor.moveToFirst();
            currentTs = cursor.getLong(cursor.getColumnIndex("ts"));

        }

        Log.i("SQLDB", "DB_INSERT / currentTs="+currentTs+" , ts="+ts);

        if(currentTs < ts )
        {
            Log.i("SQLDB", "REPLACE VALUES!");
            ContentValues cv = new ContentValues();
            cv.put("key",key);
            cv.put("value",value);
            cv.put("ts",ts);
            this.getWritableDatabase().insertWithOnConflict(TABLE_NAME, null, cv, SQLiteDatabase.CONFLICT_REPLACE);
        }

        Log.i("SQLDB", "DB_INSERT / query: key="+key+" ,value="+value+" ,ts="+ts);
        Cursor cr = this.getReadableDatabase().rawQuery("select * from "+TABLE_NAME+" where key='"+key+"'", null);
        if(cr.getCount() > 0)
        {
            cr.moveToFirst();
            Log.i("SQLDB","DB_INSERT / value="+value);
        }

    }

    /* Delete key-value from db based on key */
    public void dbDelete(String key)
    {
        if(key.equals("*") || key.equals("@"))
        {
            this.getWritableDatabase().delete(TABLE_NAME, null, null);
            Log.i("SQLDB", "DB DELETE / STAR or AND");
            this.dbQuery(key);
        }else
        {
            this.getWritableDatabase().delete(TABLE_NAME, "key = '"+key+"'", null);
            Log.i("SQLDB", "DB DELETE / KEY WISE: key="+key);
            this.dbQuery(key);
        }

    }

    /* Query database based on key */
    public Cursor dbQuery(String key)
    {
        Cursor ret = null;
        ret = this.getReadableDatabase().rawQuery("select * from "+TABLE_NAME+" where key='"+key+"'", null);
        if(key.equals("*"))
        {
            ret = this.getReadableDatabase().rawQuery("select * from "+TABLE_NAME, null);
        }

        Log.i("SQLDB", "record/ query: count"+ret.getCount());
        return ret;
    }

    public void onDowngrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        onUpgrade(db, oldVersion, newVersion);
    }

}
