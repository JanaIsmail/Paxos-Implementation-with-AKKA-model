package com.example;

import java.util.ArrayList;

import akka.actor.ActorRef;

public class Members {
	
	public ArrayList<ActorRef> members;
	
	public Members(ArrayList<ActorRef> m) {
		members = m;
	}
}