package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isLeader)
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
//import "fmt"
import "math/rand"
import "time"
import "encoding/gob"
import "bytes"

const BASE = 350
const HEARTBEAT = 100

// import "bytes"
// import "encoding/gob"

type Entry struct {
    Term int
    Command interface{}
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
    timer *time.Timer 
    voteCounter int
    heartbeatCounter int
    //heartbeatQuorum int
    timeChan chan bool
    applyCh chan ApplyMsg

    isLeader bool
    currentTerm int
    votedFor int
    log[] Entry

    commitIndex int
    lastApplied int

    nextIndex []int
    matchIndex []int


	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	// Your code here (2A).
    term = rf.currentTerm;
    //fmt.Println("ID:",rf.me, " XXXXXXXXXXXX=>", term, rf.isLeader)

	return term, rf.isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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

    //DPrintf("========>  Node:%d perisit log:%v\n", rf.me, rf.log)
    w := new(bytes.Buffer)
    e := gob.NewEncoder(w)
    e.Encode(rf.currentTerm)
    e.Encode(rf.votedFor)
    e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)

    //fmt.Println("node:%d Recover:#################====>", rf.me)
    //fmt.Println("currentTerm:#################====>", rf.currentTerm)
    //fmt.Println("voteFor:#################====>", rf.votedFor)
    //fmt.Println("logs:#################====>", rf.log)
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
    Term int
    CandidateId int
    LastLogIndex int
    LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
    Term int
    VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
    DPrintf("follower:%d len:%v log:%v ===>RequestVote ,currentTerm:%d ,commitIndex:%v, appliedIndex:%v, CandidateId:%d CandidateTerm:%d, CandidateLastLogIndex:%v, LastLogTerm:%v\n", rf.me, len(rf.log), rf.log, rf.currentTerm, rf.commitIndex, rf.lastApplied, args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm)
    //DPrintf("follower:%d ===>RequestVote ,currentTerm:%d CandidateId:%d CandidateTerm:%d\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)
    rf.mu.Lock()
    defer rf.mu.Unlock()
    rf.persist()
    voteValid := false

    reply.Term = rf.currentTerm
    if (args.Term < rf.currentTerm){
        reply.VoteGranted = false
        return 
    } else if(args.Term > rf.currentTerm){
        voteValid = true
        rf.currentTerm = args.Term
        if (rf.isLeader == true){
            rf.isLeader = false
        }
    }

    if (len(rf.log) > 0 ){
        //fmt.Println("Follower:", rf.log)
        //fmt.Println("Args:", args)
        if(rf.log[len(rf.log) - 1].Term > args.LastLogTerm){
            DPrintf("AAAAAAAAAA")
            reply.VoteGranted = false
            return 
        }

        if(rf.log[len(rf.log) - 1].Term == args.LastLogTerm && args.LastLogIndex < len(rf.log)){
            DPrintf("BBBBBBBBB")
            reply.VoteGranted = false
            return 
        }
    }

    if (voteValid == true){
        rf.votedFor = args.CandidateId
        //fmt.Println("#####Follower:", rf.me, " vote for:", args.CandidateId)
        reply.VoteGranted = true
        if (rf.isLeader == true){
            rf.isLeader = false
        }
        rf.ResetRandTimer()
    } else {
        reply.VoteGranted = false
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
    //ok := false
    //for i := 0; i < 3; i++ {
    //    DPrintf("######sendRequestVote:leader:%v currentTerm:%v follower:%v, retry:%v\n", rf.me, rf.currentTerm, server, i)
    //    if (args.Term == rf.currentTerm){
    //        ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
    //        if (ok == true){
    //            break
    //        }
    //    }
    //}
    ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	return ok
}

type AppendEntriesArgs struct {
    Term int
    LeaderId int
    PrevLogIndex int
    PrevLogTerm int
    Entries []Entry
    LeaderCommit int
}

type AppendEntriesReply struct {
    //Index int
    Term int
    Success bool
    Reback int
    Reterm int
    //Retry bool
}

func Min(a int, b int) int{
    if (a > b){
        return b
    } else {
        return a
    }
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
    rf.persist()
    //fmt.Println("AppendEntries follower:", rf.me, "===> commitIndex:", rf.commitIndex, " log:", rf.log)
    reply.Term = rf.currentTerm
    if ((args.Term < rf.currentTerm)){
        reply.Success = false
        return
    } else if(args.Term > rf.currentTerm){
        rf.currentTerm = args.Term
        if (rf.isLeader == true){
            rf.mu.Lock()
            rf.isLeader = false
            rf.mu.Unlock()
        }
    }

    reply.Success = rf.ResetRandTimer()
    //fmt.Println("AppendEntries===>Follower:",rf.me, " FollowerTerm:",rf.currentTerm," Entries:",args.Entries, " leaderId:", args.LeaderId, " leaderTerm:", args.Term, " leaderPreLogIndex:", args.PrevLogIndex, " fowllower len:",len(rf.log)," Log:",rf.log)
    DPrintf("Heartbeat===>Follower:%v commitIndex:%v lastApplied:%v, len:%v, Leader:%v===>commitIndex:%v\n", rf.me, rf.commitIndex, rf.lastApplied, len(rf.log), args.LeaderId, args.LeaderCommit)

    if (args.PrevLogIndex == 0){
        rf.log = rf.log[0:0]
        for _, entry := range args.Entries{
            rf.log = append(rf.log, entry)
        }

        rf.commitIndex = Min(len(rf.log), args.LeaderCommit)
        rf.ApplyMsg()

        //reply.Index = len(rf.log)
        //DPrintf("Follower:%v AppendEntries:%v Init Success:%v\n", rf.me, args.Entries, rf.log)
        reply.Success = true
        return

    }

    if (args.PrevLogIndex > len(rf.log)){
        reply.Success = false
        DPrintf("follower:%v ===> 1111111111111111", rf.me)
        reply.Reback = args.PrevLogIndex - len(rf.log)
        //reply.Reback = 1
        //reply.Index = len(rf.log)
        return
    }

    if (args.PrevLogIndex != 0 && rf.log[args.PrevLogIndex -1].Term != args.PrevLogTerm){
        reply.Success = false
        DPrintf("follower:%v ===> 2222222222222222", rf.me)
        reply.Reback = 1
        for i := args.PrevLogIndex - 2; i >= 0; i--{
            reply.Reback++
            if (rf.log[i].Term != rf.log[args.PrevLogIndex-1].Term){
                reply.Reterm = rf.log[args.PrevLogIndex -1].Term
                break
            }
        }
        //reply.Index = args.PrevLogIndex - 1
        return
    }

    //append entries

    rf.log = rf.log[0:args.PrevLogIndex]
    for _, entry := range args.Entries{
        rf.log = append(rf.log, entry)
    }

    //reply.Index = len(rf.log)
    //DPrintf("follower:%v AppendEntries Success===>len:%v, log:%v\n", rf.me, len(rf.log), rf.log)

    if (args.LeaderCommit > rf.commitIndex){
        rf.commitIndex = Min(len(rf.log), args.LeaderCommit)
    }

    rf.ApplyMsg()
    reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
    //ok := false
    //for i := 0; i < 3; i++ {
    //    DPrintf("######sendAppendEntries:leader:%v currentTerm:%v, follower:%v, retry:%v\n", rf.me, rf.currentTerm, server, i)
    //    if (args.Term == rf.currentTerm){
    //        ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
    //        if (ok == true){
    //            break
    //        }
    //    }
    //}
    ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
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

	// Your code here (2B).
    //DPrintf("me:%d isLeader:%v Start ==> Command:%v\n", rf.me, rf.isLeader, command)
    log_len := 0
    if (rf.isLeader == false){
        return len(rf.log), rf.currentTerm, rf.isLeader
    }

    if (rf.isLeader == true){
        entry := Entry{rf.currentTerm, command}
        rf.mu.Lock()
        rf.log =  append(rf.log, entry)
        log_len = len(rf.log)
        rf.mu.Unlock()
        //fmt.Println("\n leader:", rf.me, " Start:", command,"===>index:",log_len," YYYYY:",rf.log)
        rf.persist()
    }


	return log_len, rf.currentTerm, rf.isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) CheckCommitIndex(){
    //DPrintf("ZZZZZZZZZZZZZZ:leader:%v commitIndex:%v log:%v\n", rf.me, rf.commitIndex, rf.log)
    //for i := rf.commitIndex + 1; i <= len(rf.log); i++{
    for i := len(rf.log); i > rf.commitIndex; i--{
        quorum := 1
        for j := 0; j < len(rf.peers); j++{
            if (j == rf.me){
                continue
            }

            if (rf.matchIndex[j] >= i){
                quorum++
            }
        }


        if (quorum >= (len(rf.peers)/2 + 1) && rf.log[i - 1].Term == rf.currentTerm){
            rf.commitIndex = i
            break;
        }
    }

    DPrintf("CheckCommitIndex Follower:%v ApplyMsg:commitIndex:%v lastApplied:%v\n", rf.me, rf.commitIndex, rf.lastApplied)
    rf.ApplyMsg()
}
func (rf *Raft) ApplyMsg(){
    if (rf.commitIndex > rf.lastApplied){
        for i:= rf.lastApplied + 1; i <= rf.commitIndex; i++{
            msg := ApplyMsg{}
            msg.Index = i
            msg.Command = rf.log[i-1].Command
            //fmt.Println("Follower:",rf.me, "SendMsg:", msg.Command, "===> Term:",rf.log[i-1].Term, " apply index:", i)
            rf.applyCh<-msg
        }

        rf.lastApplied = rf.commitIndex
    }
}

func (rf *Raft) Heartbeat() {
    //heartbeat
    rf.mu.Lock()
    DPrintf("\n*************success elect leader:%d, term:%d***************\n", rf.me, rf.currentTerm)
    rf.isLeader = true
    rf.voteCounter = 0
    rf.heartbeatCounter = 0
    rf.timer.Stop()
    rf.mu.Unlock()

    //reinitialized
    for i := 0; i < len(rf.peers); i++{
        if (i == rf.me){
            continue
        }
        rf.nextIndex[i] = len(rf.log) + 1
        rf.matchIndex[i] = 0
    }


    go func(){
        for {
            //no quorum no leader
            //DPrintf("Leader:%d ===>heartbeatQuorum:%d heartbeatCounter:%d term:%d, isLeader:%v\n", rf.me, rf.heartbeatQuorum, rf.heartbeatCounter, rf.currentTerm, rf.isLeader)
            if (rf.isLeader == false){
                DPrintf("Leader->%d: is not a leader!!!\n", rf.me)
                go rf.TimeoutWatcher()
                return
            }
            //if ((rf.heartbeatQuorum < (len(rf.peers)/2 + 1)) && rf.heartbeatCounter > 0){
            //    DPrintf("Leader->%d, Term:%v:No Quorum:%d No Leader!!!\n", rf.me, rf.currentTerm, rf.heartbeatQuorum)
            //    rf.mu.Lock()
            //    rf.isLeader = false
            //    rf.mu.Unlock()
            //    go rf.TimeoutWatcher()
            //    return
            //} 

            //rf.me count 1
            //rf.heartbeatQuorum = 1

            for i := 0; i < len(rf.peers); i++{
                //DPrintf("Total:%d, ###Leader:%d ===> Heartbeat:%d\n",len(rf.peers), rf.me, i)
                if (i == rf.me){
                    continue
                }
                //DPrintf("####leader:%v===>SendEntrie:%v\n", rf.me, i)
                go rf.AppendEntrieToFollower(i)
            }

            rf.heartbeatCounter++
            time.Sleep(HEARTBEAT * time.Millisecond)
            rf.CheckCommitIndex()
        }
    }()
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
    rf.isLeader = false
    rf.heartbeatCounter = 0
    //rf.heartbeatQuorum = 0
    rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
    rf.timeChan = make(chan bool, 1)
    DPrintf("==========================================>Make:%v, Raft:%v\n", rf.me, &rf)
    rf.currentTerm = 0
    rf.commitIndex = 0
    rf.lastApplied = 0
    rf.nextIndex = make([]int, len(peers))
    rf.matchIndex = make([]int, len(peers))
    rf.timer = NewRandTimer()


    //election timeout
    go rf.TimeoutWatcher()

    //response
    go rf.StartNewElection()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}

func NewRandTimer() *time.Timer{
    rand := (time.Duration)(rand.Intn(150) + BASE)
    timer := time.NewTimer(rand * time.Millisecond)

    return timer
}

func (rf *Raft) ResetRandTimer() bool{
    rand := (time.Duration)(rand.Intn(150) + BASE)
    DPrintf("PPPPPPP: follower:%d term:%v ===> timeout:%dms now:%v\n", rf.me, rf.currentTerm, rand, time.Now().UnixNano()/1000000)
    if !rf.timer.Stop(){
        select {
            case <-rf.timer.C:
            default:
        }
    }
    return rf.timer.Reset(rand * time.Millisecond)
}

func (rf *Raft) StartNewElection(){
    for {
        select {
            case <- rf.timeChan:
                go rf.TimeoutWatcher()
                rf.votedFor = rf.me
                rf.currentTerm++
                DPrintf("%d--->start election a new leader, currentTerm:%d\n", rf.me, rf.currentTerm)
                rf.voteCounter = 1
                for i := 0; i < len(rf.peers); i++{
                    if (i == rf.me){
                        continue
                    }

                    //DPrintf("####leader:%v===>SendRequestVote:%v\n", rf.me, i)
                    go rf.RequestVoteServer(i)
                }
        }
    }

}

func (rf *Raft)TimeoutWatcher(){
    rf.ResetRandTimer()
    <-rf.timer.C
        //timeout channel
    if (rf.isLeader == false){
        DPrintf("\n\n%d--->election timeout isLeader:%v, now:%v\n", rf.me, rf.isLeader, time.Now().UnixNano()/1000000)
        rf.timeChan <- true
    }
}

func (rf *Raft)AppendEntrieToFollower(follower int){
    arg := AppendEntriesArgs{}
    arg.Term = rf.currentTerm
    arg.LeaderId = rf.me
    arg.PrevLogIndex = rf.nextIndex[follower] - 1
    DPrintf("TTTTT:leader:%v append follower:%v, PrevLogIndex:%v len:%v matchIndex:%v nextIndex:%v\n", rf.me, follower, arg.PrevLogIndex, len(rf.log), rf.matchIndex, rf.nextIndex)
    if (arg.PrevLogIndex > len(rf.log)){
        return
    }
    if (arg.PrevLogIndex != 0){
        DPrintf("KKKKKKKKKK:%v\n", arg.PrevLogIndex)
        arg.PrevLogTerm = rf.log[arg.PrevLogIndex - 1].Term
    }
    arg.LeaderCommit = rf.commitIndex
    arg.Entries = rf.log[arg.PrevLogIndex:]

    //fmt.Println("TTTTTTTTTTTTTT leader:",rf.me, " entries:", arg.Entries," nextIndex:",rf.nextIndex, " matchIndex:",rf.matchIndex, "===> follower:",follower, " preLogIndex:", arg.PrevLogIndex, " len:",len(rf.log)," log:", rf.log)

    reply := AppendEntriesReply{}
    ok := rf.sendAppendEntries(follower, &arg, &reply)
    if (ok == false){
        DPrintf("!!!!!!leader:%d append follower:%d failed, leader currentTerm:%d, PrevLogIndex:%v\n", rf.me, follower, rf.currentTerm, arg.PrevLogIndex)
        return;
    }
    DPrintf("Leader:%v EntryAppend===>Follower:%d Reply:%v, PrevLogIndex:%v, currentTerm:%v, argTerm:%v\n", rf.me, follower, reply, arg.PrevLogIndex, rf.currentTerm, arg.Term)
    if (arg.Term == rf.currentTerm){
        if (reply.Success == false){
            DPrintf("HEARTBEAT ERROR: Leader:%d Term:%d ===> Follower:%d Term:%d\n", rf.me, rf.currentTerm, follower, reply.Term)
            if (rf.currentTerm < reply.Term){
                rf.mu.Lock()
                rf.currentTerm = reply.Term
                rf.isLeader = false
                rf.mu.Unlock()
                return
            } else {
                //rf.mu.Lock()
                //rf.heartbeatQuorum++;
                //rf.mu.Unlock()

                if ((rf.nextIndex[follower] - reply.Reback) > 0){
                    rf.nextIndex[follower] = rf.nextIndex[follower] - reply.Reback;
                    if (reply.Reterm != 0){
                        if (rf.log[rf.nextIndex[follower]].Term != reply.Reterm ){
                            if (rf.nextIndex[follower] > 1){
                                rf.nextIndex[follower]--
                            }
                        } 
                    }
                }
            }
        } else {
                //rf.mu.Lock()
                //rf.heartbeatQuorum++;
                //rf.mu.Unlock()

                //DPrintf("###Reply:leader:%d len:%v follower:%d ==> ReplyIndex:%v \n", rf.me, len(rf.log), follower, reply.Index)
                if (len(arg.Entries) != 0){
                    rf.matchIndex[follower] =  arg.PrevLogIndex + len(arg.Entries)
                    rf.nextIndex[follower] = rf.matchIndex[follower] + 1
                }
        }
        rf.CheckCommitIndex()
    }
}

func (rf *Raft)RequestVoteServer(i int){
    arg := RequestVoteArgs{}
    arg.Term = rf.currentTerm
    arg.CandidateId = rf.me
    arg.LastLogIndex = len(rf.log)
    if (arg.LastLogIndex > 0){
        arg.LastLogTerm = rf.log[len(rf.log) - 1].Term
    }

    reply := RequestVoteReply {}

    DPrintf("%d send request vote:%d, currentTerm:%d\n", rf.me, i, rf.currentTerm)
    ok := rf.sendRequestVote(i, &arg, &reply)
    if (ok == false){
        DPrintf("!!!CandidateID:%d ask for vote from follower:%d connect failed, term:%d\n", rf.me, i, rf.currentTerm)
        return;
    }
    DPrintf("CandidateID:%v Follower:%v ===> Vote Response:currentTerm:%v, argTerm:%v\n", rf.me, i, rf.currentTerm, arg.Term)
    if (arg.Term == rf.currentTerm){
        DPrintf("me:%v vote:%v ==>reply:%v\n", rf.me, i, reply)
        if (reply.VoteGranted == false){
            //DPrintf("reply.Term:%d currentTerm:%d\n", reply.Term, arg.Term)
            rf.mu.Lock()
            if (reply.Term > rf.currentTerm){
                rf.currentTerm = reply.Term
            }
            rf.mu.Unlock()
            return
        }
        rf.mu.Lock()
        rf.voteCounter++
        rf.mu.Unlock()

        //election success
        DPrintf("####CandidateId:%d Vote_count:%d===>currentTerm:%d\n", rf.me, rf.voteCounter, rf.currentTerm)
        if ((rf.voteCounter >= (len(rf.peers)/2 + 1)) && (rf.isLeader == false)){
            rf.Heartbeat()
        }
    }
}


