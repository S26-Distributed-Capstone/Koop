package edu.yu.cs.com4020.com.koop;

import io.javalin.Javalin;

public class Main {
    public static void main(String[] args) {
        var app = Javalin.create(/*config*/)
            .get("/", ctx -> ctx.result("This is the first thing "))
            .start(7070);
        
    }
}