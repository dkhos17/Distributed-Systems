 package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//
import "fmt"
import "sync"
import "time"
import "sync/atomic"
import "../labrpc"
import "math/rand"

import "bytes"
import "../labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	Snapshot []byte
}

type LogEntry struct {
	Term int
	Index int
	Command interface{}
}

type AppendEntryArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int

	PrevLogTerm int
	Entries []LogEntry

	LeaderCommit int
}

type AppendEntryReply struct {
	Term int
	Success bool
	NextIndex int	// the optimization that backs up nextIndex by more than one entry at a time.
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	/* At any given time each server is in one of three states */
	state string 		// leader, follower, or candidate
	heartbeat time.Time
	votes int

	/* Persistent state - servers*/
	currentTerm int
	votedFor int
	log	[]LogEntry

	/* Volatile state - servers*/
	commitIndex int
	lastApplied int

	/* Volatile state - leaders*/
	nextIndex []int	
	matchIndex []int

	bstep int // step to find reply.NextIndex by bsearch
	notify chan bool // notify - some logs can be commited
	apply chan ApplyMsg // save - given client chanel in raft make
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here (2A).
	return rf.currentTerm, rf.state == "leader"
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	// fmt.Println("saved", rf.me, "->", rf.currentTerm, rf.votedFor, rf.log)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		fmt.Println("error", currentTerm, votedFor, log)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		// fmt.Println("read", rf.me, "->", rf.currentTerm, rf.votedFor, rf.log)
	}
}

func (rf *Raft) snapshotLog(LastIncludedIndex int, LastIncludedTerm int) (int, int){
	entry := LogEntry{Index: LastIncludedIndex, Term: LastIncludedTerm}
	for i := len(rf.log)-1; i >= 0; i-- {
		if rf.log[i].Index == LastIncludedIndex && rf.log[i].Term == LastIncludedTerm {
			rf.log = append([]LogEntry{entry}, rf.log[i+1:]...)
			return LastIncludedIndex, LastIncludedIndex
		}
	}

	rf.log = []LogEntry{entry}
	return LastIncludedIndex, LastIncludedIndex
}

func (rf *Raft) applySnapshot(snapshot []byte) {
	rf.apply <- ApplyMsg{Snapshot: snapshot}
}

// order of elements - LastIncludedIndex, LastIncludedTerm, kv.data, kv.been
func (rf *Raft) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var LastIncludedIndex int
	var LastIncludedTerm int
	if d.Decode(&LastIncludedIndex) != nil || d.Decode(&LastIncludedTerm) != nil {
		fmt.Println("error", LastIncludedIndex, LastIncludedTerm)
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()
	
		rf.commitIndex, rf.lastApplied = rf.snapshotLog(LastIncludedIndex, LastIncludedTerm)
		go rf.applySnapshot(data)
		// fmt.Println("read", rf.me, "->", LastIncludedIndex, LastIncludedTerm, rf.currentTerm)
	}
}

// to check maxraftstate in kvraft
func (rf *Raft) RaftStateSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) GetLastIncludedIndex() int {
	if len(rf.log) == 0 {
		return 0
	}
	return rf.log[0].Index
}

func (rf *Raft) GetLastIncludedTerm() int {
	if len(rf.log) == 0 {
		return 0
	}
	return rf.log[0].Term
}

type SnapshotArgs struct {
	Term int 					// leader’s term
	LeaderId int 				// so follower can redirect clients
	LastIncludedIndex int 		// the snapshot replaces all entries up through and including this index
	LastIncludedTerm int 		// term of lastIncludedIndex
	// Offset int					// byte offset where chunk is positioned in the snapshot file
	Data[] byte					// raw bytes of the snapshot chunk, starting at offset
	// Done bool 					// true if this is the last chunk
}

type SnapshotReply struct {
	Term int 			// currentTerm, for leader to update itself
	NextIndex int
}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int 			// candidate’s term
	CandidateID int		// candidate requesting vote
	LastLogIndex int	// index of candidate’s last log entry
	LastLogTerm int		// term of candidate’s last log entry 
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int 			// currentTerm, for candidate to update itself
	VoteGranted bool	// true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.becameFollower(args.Term)
		rf.heartbeat = time.Now()
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && rf.up_to_date(args) {
		// fmt.Println("-----------------",rf.me,"voted to", args.CandidateID, "in term", rf.currentTerm, "-----------------")
		rf.state = "follower"
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
		defer rf.persist()
	}
}
/* 
If the logs have last entries with different terms, then
the log with the later term is more up-to-date. If the logs
end with the same term, then whichever log is longer is
more up-to-date.
*/
func (rf *Raft) up_to_date(candidate *RequestVoteArgs) bool {
	voter := rf.log[len(rf.log)-1]

	if candidate.LastLogTerm != voter.Term {
		return candidate.LastLogTerm > voter.Term
	}
	return candidate.LastLogIndex >= voter.Index
}


func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
		
	reply.Success = false
	reply.NextIndex = rf.log[len(rf.log)-1].Index + 1

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.becameFollower(args.Term)
	}
	rf.heartbeat = time.Now()

	if rf.log[len(rf.log)-1].Index < args.PrevLogIndex {
		return
	}

	LastIncludedIndex := rf.GetLastIncludedIndex()
	args.PrevLogIndex = args.PrevLogIndex - LastIncludedIndex
	if args.PrevLogIndex >= 0 && args.PrevLogIndex < len(rf.log) {
		if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm  {
			reply.NextIndex = args.PrevLogIndex - rf.bstep
			if reply.NextIndex < 1 {reply.NextIndex = 1}
			rf.bstep *= 2
			return
		}
	
		rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
		reply.NextIndex = rf.log[len(rf.log)-1].Index + 1
		reply.Success = true
		rf.bstep = 1
		defer rf.persist()
	}
	
	// fmt.Println("rf.logs", rf.log)
	
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < rf.log[len(rf.log)-1].Index {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = rf.log[len(rf.log)-1].Index
		}
		defer func() { rf.notify <- true } ()
	}
	// fmt.Println("commit-index", rf.me, "->", rf.commitIndex)

}

func (rf *Raft) InstallSnapshot(args *SnapshotArgs, reply *SnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.becameFollower(args.Term)
	}
	reply.NextIndex = args.LastIncludedIndex

	rf.heartbeat = time.Now()
	rf.commitIndex, rf.lastApplied = rf.snapshotLog(args.LastIncludedIndex, args.LastIncludedTerm)
	rf.persist()
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), args.Data)
	go rf.applySnapshot(args.Data)
	// fmt.Println("snapshot installed")
}

func (rf *Raft) SaveSnapshot(kvsnapshot []byte, maxraftstate int, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if maxraftstate > rf.persister.RaftStateSize() {
		return
	}

	if rf.log[0].Index <= index && index <= rf.log[len(rf.log)-1].Index {
		// fmt.Println("before", len(rf.log), rf.persister.RaftStateSize())
		rf.log = rf.log[(index-rf.GetLastIncludedIndex()):]
		// persist
		w1 := new(bytes.Buffer)
		e1 := labgob.NewEncoder(w1)
		e1.Encode(rf.currentTerm)
		e1.Encode(rf.votedFor)
		e1.Encode(rf.log)
		state := w1.Bytes()
		// snapshot
		w2 := new(bytes.Buffer)
		e2 := labgob.NewEncoder(w2)
		e2.Encode(rf.GetLastIncludedIndex())
		e2.Encode(rf.GetLastIncludedTerm())
		snapshot := append(w2.Bytes(), kvsnapshot...)  // swap places to keep ordering

		rf.persister.SaveStateAndSnapshot(state, snapshot)
		// fmt.Println("after", len(rf.log), rf.persister.RaftStateSize())
	} 
}

func (rf *Raft) becameFollower(Term int) {
	defer rf.persist()
	rf.currentTerm = Term
	rf.state = "follower"
	rf.votedFor = -1
	rf.votes = 0
	// fmt.Println("----------------- became follower", rf.me ,"in term", rf.currentTerm,"-----------------")
}

func (rf *Raft) becameCandidate() {
	defer rf.persist()
	rf.currentTerm += 1
	rf.state = "candidate"
	rf.votedFor = rf.me
	rf.votes = 1
	// fmt.Println("----------------- became candidate", rf.me ,"in term", rf.currentTerm,"-----------------")
}

func (rf *Raft) becameLeader() {
	defer rf.persist()
	rf.state = "leader"
	rf.votedFor = -1
	rf.votes = 0
	// fmt.Println("----------------- became Leader", rf.me ,"in term", rf.currentTerm,"-----------------")
}

// --- Leader Helper function
func (rf* Raft) leaderSendAppendEntries(server int) {
	args, reply := AppendEntryArgs{}, AppendEntryReply{}
	// init args
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.PrevLogIndex = rf.nextIndex[server] - 1
	args.LeaderCommit = rf.commitIndex
	
	// configure index if snapshot
	LastIncludedIndex := rf.GetLastIncludedIndex()
	args.PrevLogIndex -= LastIncludedIndex
	rf.nextIndex[server] -= LastIncludedIndex

	if args.PrevLogIndex >= 0 && args.PrevLogIndex < len(rf.log) {
		args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
	}
	if rf.nextIndex[server] >= 0 && rf.nextIndex[server] < len(rf.log) {
		args.Entries = append(args.Entries, rf.log[rf.nextIndex[server]:]...)				
	}

	args.PrevLogIndex += LastIncludedIndex
	rf.nextIndex[server] += LastIncludedIndex

	go func(server int, args *AppendEntryArgs, reply *AppendEntryReply) {
		rf.sendAppendEntries(server, args, reply)
	} (server, &args, &reply)
}

// --- Leader Helper function
func (rf *Raft) leaderSendInstallSnapshot(server int) {
	args, reply := SnapshotArgs{}, SnapshotReply{}
	// init args
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LastIncludedIndex = rf.GetLastIncludedIndex()
	args.LastIncludedTerm = rf.GetLastIncludedTerm()
	args.Data = rf.persister.ReadSnapshot()

	go func(server int, args *SnapshotArgs, reply *SnapshotReply) {
		rf.sendInstallSnapshot(server, args, reply)
	} (server, &args, &reply)
}

func (rf *Raft) leaderTodo() {
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
		rf.notify <- true 
	}()
	if rf.state != "leader" {return}
	// fmt.Println("----------------- sending heartbeats", rf.me , rf.currentTerm,"-----------------")

	// send heartbeats to all
	for peer, _ := range rf.peers {
		if peer != rf.me {
			/* Raft leader send an InstallSnapshot RPC to a follower,
				when the leader has discarded log entries that the follower needs. 
			*/
			if rf.nextIndex[peer] >= rf.GetLastIncludedIndex() {
				rf.leaderSendAppendEntries(peer)
			} else {
				rf.leaderSendInstallSnapshot(peer)
			}
		}
	}

	/* If there exists an N such that N > commitIndex, 
		a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
		set commitIndex = N 
	*/
	LastIncludedIndex := rf.GetLastIncludedIndex()
	for N := rf.commitIndex + 1; N <= rf.log[len(rf.log)-1].Index; N++ {
		majority := 1
		for peer := range rf.peers {
			if peer != rf.me {
				if rf.matchIndex[peer] >= N && rf.log[N-LastIncludedIndex].Term == rf.currentTerm {
					majority++
				}
			}
		}
		
		if majority > len(rf.peers)/2 {
			rf.commitIndex = N
		}
	}
	
}

func (rf *Raft) followerTodo() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != "follower" {return false}

	timeout := time.Duration(300+rand.Intn(300))*time.Millisecond
	// heartbeat recived
	if time.Since(rf.heartbeat) < timeout {
		// fmt.Println("----------------- follower got heartbeat", rf.me , "in term", rf.currentTerm,"-----------------")
		return true
	}

	// became candidate
	rf.becameCandidate()
	rf.heartbeat = time.Now()

	// start election
	for peer := range rf.peers {
		if peer != rf.me {
			args, reply := RequestVoteArgs{}, RequestVoteReply{}
			// init args
			args.Term = rf.currentTerm
			args.CandidateID = rf.me
			args.LastLogIndex = rf.log[len(rf.log)-1].Index
			args.LastLogTerm = rf.log[len(rf.log)-1].Term

			go func(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
				rf.sendRequestVote(server, args, reply)
			} (peer, &args, &reply)
		}
	}
	return false
}

func (rf *Raft) candidateTodo() {	
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != "candidate" {return}

	if time.Since(rf.heartbeat) < 300*time.Millisecond {
		// fmt.Println("----------------- candidate got heartbeat", rf.me , "in term", rf.currentTerm,"-----------------")
		rf.becameFollower(rf.currentTerm)
		return
	}
	
	// fmt.Println("----------------- total votes: ", rf.votes, "in term", rf.currentTerm,"-----------------")
	
	if rf.votes > len(rf.peers)/2 {
		rf.becameLeader()
		rf.reinit_after_election()
	} else {
		rf.becameFollower(rf.currentTerm)
	}
}

func (rf *Raft) Routine() {
	for {
		if rf.state == "follower" {
			if rf.followerTodo() {
				time.Sleep(time.Duration(300+rand.Intn(300))*time.Millisecond)
			}
		}

		if rf.state == "candidate" {
			time.Sleep(300*time.Millisecond )
			rf.candidateTodo()
		}
		
		if rf.state == "leader" {
			rf.leaderTodo()
			time.Sleep(100*time.Millisecond)
		}

	}
}

func (rf *Raft) commit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	LastIncludedIndex := rf.GetLastIncludedIndex()
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.apply <- ApplyMsg{true, rf.log[i-LastIncludedIndex].Command, rf.log[i-LastIncludedIndex].Index, nil}
		// fmt.Println(rf.me, "commited", i)
	}
	rf.lastApplied = rf.commitIndex
}

func (rf *Raft) Apply() {
	for {
		<-rf.notify
		rf.commit()
	}
}


func (rf *Raft) reinit_after_election() {
	if rf.state != "leader" {
		return
	}

	for peer := range rf.peers {
		rf.matchIndex[peer] = 0
		rf.nextIndex[peer] = rf.log[len(rf.log)-1].Index + 1
	}
}


//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {return ok}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.becameFollower(reply.Term)
	}

	if rf.state == "candidate" && reply.VoteGranted {
		rf.votes += 1
	}

	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {return ok}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.becameFollower(reply.Term)
	}
	
	if rf.state == "leader" && reply.Success {
		if len(args.Entries) ==  0 { return ok }
		rf.matchIndex[server] = args.Entries[len(args.Entries)-1].Index
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	} else if rf.state == "leader" {
		rf.nextIndex[server] = reply.NextIndex
	}

	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *SnapshotArgs, reply *SnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if !ok {return ok}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// fmt.Println("RPC", args.LeaderId, "sendInstallSnapshot", server, "Index:", args.LastIncludedIndex, args.LastIncludedTerm)
	if reply.Term > rf.currentTerm {
		rf.becameFollower(reply.Term)
	} else if rf.state == "leader" {
		rf.matchIndex[server] = reply.NextIndex
		rf.nextIndex[server] = reply.NextIndex + 1
	}

	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer func() {
		if rf.state == "leader" { rf.persist() }
		rf.mu.Unlock()
	}()
	// Your code here (2B).
	if rf.state == "leader" {
		// fmt.Println(rf.me, "added in log", len(rf.log), "in term", rf.currentTerm)
		rf.log = append(rf.log, LogEntry{rf.currentTerm, rf.log[len(rf.log)-1].Index + 1, command})
		return rf.log[len(rf.log)-1].Index, rf.currentTerm, true
	}

	return -1, rf.currentTerm, false
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.bstep = 1
	rf.votedFor = -1
	rf.state = "follower"

	rf.log = append(rf.log, LogEntry{})
	rf.nextIndex = make([]int, len(rf.peers))	
	rf.matchIndex = make([]int, len(rf.peers))
	
	rf.notify = make(chan bool, len(rf.peers))
	rf.apply = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())
	
	go rf.Routine()
	go rf.Apply()

	return rf
}
