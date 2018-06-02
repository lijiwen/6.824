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

import "sync"
import "labrpc"
import "time"
import "math/rand"

// import "bytes"
// import "encoding/gob"

const (
	flower    = 1
	candidate = 2
	leader    = 3
)

//ApplyMsg as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//LogEntry define log entry
type LogEntry struct {
	TermIndex int         //任期
	Command   interface{} //命令
}

//Raft A Go object implementing a single Raft peer.
type Raft struct {
	mu               sync.Mutex          // Lock to protect shared access to this peer's state
	peers            []*labrpc.ClientEnd // RPC end points of all peers
	persister        *Persister          // Object to hold this peer's persisted state
	me               int                 // this peer's index into peers[]
	currentTerm      int                 //当前任期
	votedFor         int                 //当前任期内收到选票的候选人ID或者-1
	role             int                 //角色：1，flower 2.candidate 3.leader
	commitIndex      int                 //提交序列号
	lastApplied      int                 //最新的被状态执行机执行的序列号
	nextIndex        []int               //leader持有的每个节点需要接收的下一个log序列号
	matchIndex       []int               //leader持有的已经复制到该节点的log的最高索引值
	lastAcceptedTime time.Time           //最后接收心跳的时间
	log              []LogEntry          //log
	leaderID         int                 //当前leader
	stateCh          chan int            //状态chan
	heartBeatTimer   *time.Timer         //心跳定时器
	applyCh          chan ApplyMsg
	closed           bool

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

//AppendEntries represent for hearbeat & logEntry
type AppendEntries struct {
	Term         int        //代表leader任期
	LeaderID     int        //代表leader id
	PreLogIndex  int        //最新日志之前的日志索引值
	PreLogTerm   int        //最新日志之前的日志任期
	Entries      []LogEntry //log
	LeaderCommit int        //领导人提交的日志条目索引值
}

//AppendEntriesReply is reply
//这个地方可能需要优化，返回对应的index
type AppendEntriesReply struct {
	Term    int  //任期
	Success bool //是否匹配preLogIndex & preLogTerm
}

func min(x, y int) int {
	if x > y {
		return y
	}
	return x
}
func (rf *Raft) sendHeartBeat(server int, request *AppendEntries, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntries", request, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if reply.Success {
			rf.nextIndex[server] = rf.nextIndex[server] + len(request.Entries) + 1
			rf.matchIndex[server] = rf.matchIndex[server] + len(request.Entries)
			return
		}
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.stateCh <- flower
			rf.role = flower
			return
		}
		if rf.nextIndex[server] > 1 {
			rf.nextIndex[server] = rf.nextIndex[server] - 1
		}
	}
}

/*
GetState return currentTerm and whether this server
* believes it is the leader.
 **/
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	term = rf.currentTerm
	isleader = (rf.role == 3)
	return term, isleader
}

func (rf *Raft) getCurrentState() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.role
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a  description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

// RequestVoteArgs RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
	// Your data here (2A, 2B).
}

//
// RequestVoteReply RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	index       int
	// Your data here (2A).
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role == flower {
		rf.resetHeartBeatTimer()
	}
	voteTerm := args.Term
	if voteTerm < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	if voteTerm > rf.currentTerm {
		rf.currentTerm = voteTerm
		rf.votedFor = -1
		rf.stateCh <- flower
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}

	// Your code here (2A, 2B).
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
	return ok
}

// Start the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := -1
	isLeader := true
	log := LogEntry{rf.currentTerm, command}
	if leader == rf.getCurrentState() {
		rf.log = append(rf.log, log)
		index = len(rf.log) - 1
		term = rf.currentTerm
	} else {
		isLeader = false
	}
	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	rf.closed = true
	// Your code here, if desired.
}

//Make
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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.role = flower
	rf.stateCh = make(chan int)
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)
	rf.log[0].Command = nil
	rf.log[0].TermIndex = 0
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyCh = applyCh
	// Your initialization code here (2A, 2B, 2C).
	go rf.stateMachine()
	go rf.createHeartBeatTimer()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
func (rf *Raft) createCommitTimer() {
	commitTimer := time.NewTicker(time.Duration(200))
	for {
		<-commitTimer.C
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			rf.applyCh <- ApplyMsg{rf.lastApplied, rf.log[rf.lastApplied].Command, false, nil}
		}
		rf.mu.Unlock()
	}
}
func (rf *Raft) getTimeOut() time.Duration {
	n := rand.Intn(100) + 200
	return time.Duration(n)
}

func (rf *Raft) stateMachine() {
	for {
		rf.role = <-rf.stateCh
		switch rf.role {
		case flower:
			if rf.heartBeatTimer == nil {
				rf.createHeartBeatTimer()
			} else {
				rf.resetHeartBeatTimer()
			}
			break
		case candidate:
			rf.stopHeartBeatTimer()
			rf.mu.Lock()
			rf.currentTerm = rf.currentTerm + 1
			rf.votedFor = -1
			rf.mu.Unlock()
			go time.AfterFunc(rf.getTimeOut()*time.Millisecond, func() {
				if rf.role == candidate {
					rf.stateCh <- candidate
					rf.mu.Lock()
					rf.role = candidate
					rf.mu.Unlock()
				}
			})
			go rf.operateByCandidate()
			break
		case leader:
			rf.mu.Lock()
			rf.leaderID = rf.me
			rf.role = leader
			rf.votedFor = rf.me
			rf.mu.Unlock()
			go rf.operateByLeader()
			break
		default:
			return
		}

	}
}

func (rf *Raft) createHeartBeatTimer() {
	rf.heartBeatTimer = time.NewTimer(rf.getTimeOut() * time.Millisecond)
	go func() {
		for {
			<-rf.heartBeatTimer.C
			if rf.stateCh == nil {
				break
			}
			println(rf.me, "become candidate at", time.Now().Nanosecond())
			rf.stateCh <- candidate
		}
	}()
}

func (rf *Raft) resetHeartBeatTimer() {
	rf.heartBeatTimer.Reset(rf.getTimeOut() * time.Millisecond)
}

func (rf *Raft) stopHeartBeatTimer() {
	rf.heartBeatTimer.Stop()
}

func (rf *Raft) operateByCandidate() {
	voteReply := make([]RequestVoteReply, len(rf.peers))
	result := make(chan RequestVoteReply)
	defer close(result)
	for i := range rf.peers {
		voteArgs := new(RequestVoteArgs)
		voteArgs.Term = rf.currentTerm
		voteArgs.CandidateID = rf.me
		voteArgs.LastLogIndex = len(rf.log) - 1
		voteArgs.LastLogTerm = rf.log[len(rf.log)-1].TermIndex
		if i == rf.me {
			rf.votedFor = rf.me
			continue
		}
		go func(k int) {
			ok := rf.sendRequestVote(k, voteArgs, &voteReply[k])
			if ok {
				voteReply[k].index = k
				result <- voteReply[k]
			}
		}(i)
	}

	var voteSuccess = 1
	for reply := range result {
		if reply.VoteGranted && reply.Term == rf.currentTerm {
			voteSuccess++
		}
		if reply.Term > rf.currentTerm {
			rf.mu.Lock()
			rf.currentTerm = reply.Term
			rf.votedFor = reply.index
			rf.mu.Unlock()
			rf.stateCh <- flower
		}
		if voteSuccess > len(rf.peers)/2 {
			println("i am leader", rf.me)
			rf.stateCh <- leader
		}
	}
}

func (rf *Raft) operateByLeader() {
	for rf.getCurrentState() == leader {
		if rf.role != leader {
			continue
		}
		for i := range rf.peers {
			if i == rf.me {
				rf.nextIndex[i] = len(rf.log)
				rf.matchIndex[i] = len(rf.log) - 1
				continue
			}
			request := new(AppendEntries)
			reply := new(AppendEntriesReply)
			request.LeaderID = rf.me
			request.LeaderCommit = rf.commitIndex
			request.Term = rf.currentTerm
			request.PreLogIndex = rf.commitIndex - 1
			if request.PreLogIndex != -1 {
				request.PreLogTerm = rf.log[request.PreLogIndex].TermIndex
			} else {
				request.PreLogTerm = -1
			}
			logs := make([]LogEntry, 0)
			for i := rf.nextIndex[i]; i < len(rf.log); i++ {
				var entry LogEntry
				entry.Command = rf.log[i].Command
				entry.TermIndex = rf.log[i].TermIndex
				logs = append(logs, entry)
			}
			request.Entries = logs
			go rf.sendHeartBeat(i, request, reply)
		}
		time.Sleep((rf.getTimeOut() * time.Millisecond) / 2)
	}
}

//AppendEntries 用于发送心跳
func (rf *Raft) AppendEntries(request *AppendEntries, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role == flower {
		rf.resetHeartBeatTimer()
	}
	if rf.currentTerm > request.Term {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	//这个地方需要设置状态转变
	if rf.currentTerm < request.Term {
		rf.currentTerm = request.Term
		rf.votedFor = request.LeaderID
		rf.leaderID = request.LeaderID
		rf.stateCh <- flower
		rf.role = flower
	}
	if rf.votedFor != request.LeaderID {
		reply.Success = false
		reply.Term = request.Term
		if rf.votedFor == -1 {
			rf.votedFor = request.LeaderID
		}
		rf.leaderID = request.LeaderID
		rf.stateCh <- flower
		rf.role = flower
	}
	if request.PreLogIndex > len(request.Entries)-1 {
		reply.Success = false
		reply.Term = rf.currentTerm
	}
	if request.PreLogIndex != -1 && rf.log[request.PreLogIndex].TermIndex != request.PreLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
	}
	if rf.role == candidate {
		rf.stateCh <- flower
		rf.role = flower
	}
	reply.Success = true
	reply.Term = rf.currentTerm
	rf.leaderID = request.LeaderID
	//下面检查是否有entry冲突
	baseIndex := request.PreLogIndex + 1
	for index := range request.Entries {
		currentIndex := index + baseIndex
		if rf.log[currentIndex].TermIndex != request.Entries[index].TermIndex {
			rf.log[currentIndex] = request.Entries[index]
			rf.log = rf.log[:currentIndex+1 : currentIndex+1]
			continue
		}
		rf.log = append(rf.log, request.Entries[index])
	}
}
