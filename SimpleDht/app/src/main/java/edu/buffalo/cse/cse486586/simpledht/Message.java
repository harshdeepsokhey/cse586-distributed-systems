package edu.buffalo.cse.cse486586.simpledht;

public class Message {
    private int type;
    private String key;
    private String value;

    Message(int type, String key, String value){
        this.type = type;
        this.key = key;
        this.value = value;
    }

    public Message() {
        this.type = 0;
        this.key = null;
        this.value = null;
    }

    public int getType() {
        return this.type;
    }

    public String getKey() {
        return this.key;
    }

    public String getValue(){
        return this.value;
    }

    public void setType(int type) {
        this.type = type;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
