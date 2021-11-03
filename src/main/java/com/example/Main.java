package com.example;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import java.util.Random;
import java.util.ArrayList;

import java.util.Collections;

public class Main {
    
    public static void main(String[] args) {
	
	final int N = 100;
	final int f = 49;
	final long tle = 1500;
	
	final ActorSystem system = ActorSystem.create("system");
//	final LoggingAdapter log = Logging.getLogger(system, "main");
	
    final ArrayList<ActorRef> members = new ArrayList<ActorRef>();
    
    try {
 
		for(int x = 0; x < N; x += 1) {
		     members.add(system.actorOf(Process.createProcess(x, N), Integer.toString(x)));
		}    
	
		// give a view of all the processes
	    for(int x = 0; x < N; x += 1) {
	    	members.get(x).tell(new Members(members), ActorRef.noSender());
		}
	    
	    // select f faulty processes
	    Collections.shuffle(members);
	    for(int x = 0; x < f; x += 1) {
	    	members.get(x).tell(new Crash(Math.random()), ActorRef.noSender());
	    }
	    
	    final long startTime = System.currentTimeMillis();
	    
	    // launch processes
	    Random r = new Random();
	    for(int x = 0; x < N; x += 1) {
	    	members.get(x).tell(new Launch(r.nextInt(2)), ActorRef.noSender());
	    	members.get(x).tell(new StartTime(startTime), ActorRef.noSender());
	    }
	    
    	// leader election
	    Thread.sleep(tle);
	    int leader_id = (int)(Math.random() * (N - f)) + f; // random id between f and N
//	    log.info("p" + members.get(leader_id).path().name() + " is now the leader");
	    for(int x = f; x < N; x += 1) {
	    	if(x != leader_id) members.get(x).tell(new Hold(), ActorRef.noSender());
	    }
	    
    } catch (Exception ioe) {} 
    
    finally { 
	    try {
			waitBeforeTerminate();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			system.terminate();
		}
    } 
  }
    
	public static void waitBeforeTerminate() throws InterruptedException {
		Thread.sleep(6000);
	}
    
}
