package com.example.model;

import com.google.gson.annotations.SerializedName;

public class Message {
    @SerializedName("id")
    private int id;
    @SerializedName("message")
    private String message;

    public Message(int id, String message) {
        this.id = id;
        this.message = message;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}