package com.nyble.rest.resources;

public class Response {
    private String status;
    private String errorMessage;

    public static Response success(){
        Response r = new Response();
        r.status = "SUCCESS";
        r.errorMessage = null;
        return r;
    }

    public static Response fail(String message){
        Response r = new Response();
        r.status = "FAILED";
        r.errorMessage = message;
        return r;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }
}