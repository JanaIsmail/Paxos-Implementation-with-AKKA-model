package com.example;

import akka.actor.Props;
import akka.actor.UntypedAbstractActor;

import java.util.ArrayList;
import java.util.HashMap;

import akka.actor.ActorRef;
import akka.event.Logging;
import akka.event.LoggingAdapter;

public class Process extends UntypedAbstractActor {
	
	
	private class State {
		  private final int est;
		  private final int ballot;

		  public State(int e, int b) {
		    est = e;
		    ballot = b;
		  }
	}
	
	private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
	private ArrayList<ActorRef> members;
	private int id; // process id
	private int N; // total number of processes
	private int ballot; // ballot number
	private int proposal; // proposal value
	private int readballot; // read ballot number
	private int imposeballot; // impose ballot number
	private int estimate;  // estimate proposed value
	private int nbReplies_phase1;
	private int nbReplies_phase2;
	private HashMap<Integer, State> states; // states
	private HashMap<Integer, Integer> acks; // acks
	private int proposed_value; // the initially proposed value
	private boolean decided; // set to true once the process decided
	private boolean faulty_prone_mode;
	private boolean silent_mode;
	private boolean hold;
	private double crash_probability;
	private long startTime;

	public Process(int i, int n) {
		N = n; id = i;
		decided = false;
		ballot = id - N; readballot = 0; imposeballot = id - N;
		estimate = -1; proposal = -1; nbReplies_phase1 = 0; nbReplies_phase2 = 0;
		faulty_prone_mode = false;
		silent_mode = false;
		hold = false;
		crash_probability = 0;
		startTime = 0;
		members = new ArrayList<ActorRef>();
		states = new HashMap<Integer, State>();
		acks = new HashMap<Integer, Integer>();
	}
	
	public static Props createProcess(int i, int n) {
		return Props.create(Process.class, () -> {
			return new Process(i, n);
		});
	}
	
	
	@Override
	public void onReceive(Object message) throws Throwable {
		if(!silent_mode && faulty_prone_mode) {
			if (Math.random() < crash_probability) {
				silent_mode = true;
//				log.info("p" + Integer.toString(id) + " is silent");
			}
		}
		else if(silent_mode) return; // do not react to the message
		
		else if(message instanceof Members) {
			Members m = (Members) message;
			receiveMembers(m);
		}
		
		else if(message instanceof Crash) {
			Crash m = (Crash) message;
			crash(m);
		}
		
		else if(message instanceof Hold) {
			hold = true;
//			log.info("p" + Integer.toString(id) + " receives hold message");
		}
		
		else if(message instanceof Launch) {
			Launch m = (Launch) message;
			launch(m);
		}
		
		else if(message instanceof StartTime) {
			StartTime m = (StartTime) message;
			startTime = m.startTime;
		}
		
		else if(message instanceof Read) {
			Read m = (Read) message;
			read(m);
		}
		
		else if(message instanceof Abort) {
			Abort m = (Abort) message;
			abort(m);
		}
		
		else if(message instanceof Gather) {
			Gather m = (Gather) message;
			gather(m);
		}
		
		else if(message instanceof Impose) {
			Impose m = (Impose) message;
			impose(m);
		}
		
		else if(message instanceof Ack) {
			Ack m = (Ack) message;
			ack(m);
		}
		
		else if(message instanceof Decide) {
			Decide m = (Decide) message;
			decide(m);
		}	
	}
	
	public void receiveMembers(Members m) {
		members = m.members;
	}
	
	public void crash(Crash c) {
//		log.info("p" + Integer.toString(id) + " is faulty prone");
		faulty_prone_mode = true;
		crash_probability = c.crash_probability;
	}
	
	public void launch(Launch s) {		
//		log.info("p" + Integer.toString(id) + " launched");
		proposed_value = s.proposal;
		propose(s.proposal);
//		if(!hold && !decided) getSelf().tell(new Launch(proposed_value), getSelf()); // porpose again
		//getSelf().tell(new Launch(proposed_value), getSelf());
	}
	
	public void propose(int p) {		
		ballot += N; proposal = p; acks.put(ballot, 0);
		nbReplies_phase1 = 0; nbReplies_phase2 = 0; 
		log.info("p" + Integer.toString(id) + " proposes " + Integer.toString(p));
		states = new HashMap<Integer, State>();
		
		Read rd = new Read(ballot);
		for(ActorRef a : members) {
			a.tell(rd, getSelf());
		}
	}
	
	
	public void read(Read r) {
//		log.info("p" + Integer.toString(id) + " received read " + Integer.toString(r.ballot) + " from " + "p" + getSender().path().name());
		int b = r.ballot;
		if (readballot > b || imposeballot > b) {
//			log.info("p" + Integer.toString(id) + " sends abort " + Integer.toString(r.ballot) + " to " + "p" + getSender().path().name());
			Abort ab = new Abort(b);
			ab.first_phase_abort = true;
			getSender().tell(ab, getSelf());
		}
		
		else {
//			log.info("p" + Integer.toString(id) + " sends gather " + Integer.toString(r.ballot) + " to " + "p" + getSender().path().name());
			readballot = b;
			getSender().tell(new Gather(id, b, imposeballot, estimate), getSelf());
		}
	}
	
	public void abort(Abort a) {
		if(a.first_phase_abort) nbReplies_phase1++;
		if(!a.first_phase_abort) nbReplies_phase2++;
		boolean propose_again = ((nbReplies_phase1++ > N/2 && a.first_phase_abort) || (nbReplies_phase2++ > N/2 && !a.first_phase_abort));
		if(!hold && !decided && propose_again) getSelf().tell(new Launch(proposed_value), getSelf()); // porpose again
		if(nbReplies_phase1++ > N/2) nbReplies_phase1 = 0;
		if(nbReplies_phase2++ > N/2) nbReplies_phase2 = 0;
//		log.info("p" + Integer.toString(id) + " received abort " + Integer.toString(a.ballot) + " from " + "p" + getSender().path().name());
		return;
	}
	
	public void gather(Gather g) {
//		log.info("p" + Integer.toString(id) + " received gather " + Integer.toString(g.ballot) + " from " + "p" + getSender().path().name());
		states.put(g.id, new State(g.estimate, g.ballot));
		nbReplies_phase1++;
		// check if it got replies from the majority
		if(states.size() > N/2) {
			int max_est_ballot = 0;
			int est = 0;
			for(int i : states.keySet()) {
				int b = states.get(i).ballot;
				if(b > max_est_ballot) {
					max_est_ballot = b;
					est = states.get(i).est;
				}
			}
			
			if(max_est_ballot > 0 && est > -1) proposal = est;
			
			states = new HashMap<Integer, State>();
			
//			log.info("p" + Integer.toString(id) + " sends impose to all ");
			Impose imp = new Impose(ballot, proposal);
			for(ActorRef a : members) {
				a.tell(imp, getSelf());
			}
		}
	}
	
	public void impose(Impose i) {
//		log.info("p" + Integer.toString(id) + " received impose " + Integer.toString(i.ballot) + " from " + "p" + getSender().path().name());
		int b = i.ballot;
		if(readballot > b || imposeballot > b) {
//			log.info("p" + Integer.toString(id) + " sends abort " + Integer.toString(i.ballot) + " to " + "p" + getSender().path().name());
			getSender().tell(new Abort(b), getSelf());
		}
		
		else {
//			log.info("p" + Integer.toString(id) + " sends ack " + Integer.toString(i.ballot) + " to " + "p" + getSender().path().name());
			estimate = i.proposal;
			imposeballot = b;
			getSender().tell(new Ack(b), getSelf());
		}
	}
	
	public void ack(Ack ack) {
//		log.info("p" + Integer.toString(id) + " received ack " + Integer.toString(ack.ballot) + " from " + "p" + getSender().path().name());
		nbReplies_phase2++;
		int nb = acks.get(ack.ballot) + 1;
		acks.put(ack.ballot, nb);
//		log.info("p" + Integer.toString(id) + " nb ack " + Integer.toString(ack.ballot) + " = " + Integer.toString(nb));
		for(int i : acks.keySet()) {
			if (acks.get(i) > N/2) {
				Decide dec = new Decide(proposal);
				for(ActorRef a : members) {
	//				log.info("p" + Integer.toString(id) + " sends decide " + Integer.toString(ack.ballot) + " to " + "p" + a.path().name());
					a.tell(dec, getSelf());
				}
				break;
			}
		}
	}
	
	public void decide(Decide d) {
		if(!decided) {
			long endTime = System.currentTimeMillis();		
			log.info("p" + Integer.toString(id) + " decides " + Double.toString(d.proposal) + " after " + Long.toString(endTime - startTime) + "ms");
			for(ActorRef a : members) {
//				log.info("p" + Integer.toString(id) + " sends decide " + Integer.toString(d.proposal) + " to " + "p" + a.path().name());
				a.tell(d, getSelf());
			}
		}
		decided = true;
	}
}
