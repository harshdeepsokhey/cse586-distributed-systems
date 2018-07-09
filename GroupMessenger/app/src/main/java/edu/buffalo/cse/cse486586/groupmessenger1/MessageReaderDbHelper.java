package edu.buffalo.cse.cse486586.groupmessenger1;

import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.provider.BaseColumns;
import android.content.Context;

/**
 * Created by sukhoi on 2/22/2018.
 *
 * References :
 *
 *
 */


public class MessageReaderDbHelper extends SQLiteOpenHelper {


    public static final int DATABASE_VERSION = 1;
    public static final String DATABASE_NAME = "MessageReader.db";
    public static final String TABLE_NAME = "entry";
    public static final String _ID = "id";
    public static final String COLUMN_NAME_KEY = "key";
    public static final String COLUMN_NAME_VALUE = "value";

    /*Create a Database using a SQL Helper */
    private static final String SQL_CREATE_ENTRIES =
                    "CREATE TABLE " + TABLE_NAME + " (" +
                    _ID + " INTEGER PRIMARY KEY," +
                    COLUMN_NAME_KEY + " TEXT, " +
                    COLUMN_NAME_VALUE + " TEXT)";

    private static final String SQL_DELETE_ENTRIES =
            "DROP TABLE IF EXISTS " + TABLE_NAME;

    public MessageReaderDbHelper(Context context) {
        super(context, DATABASE_NAME, null, DATABASE_VERSION);
    }

    public void onCreate(SQLiteDatabase db) {
        db.execSQL(SQL_CREATE_ENTRIES);
    }

    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        db.execSQL(SQL_DELETE_ENTRIES);
        onCreate(db);
    }

    public void onDowngrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        onUpgrade(db, oldVersion, newVersion);
    }

}


