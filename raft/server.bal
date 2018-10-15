import ballerina/grpc;
import ballerina/io;
import ballerina/config;
import ballerina/log;

endpoint grpc:Listener listener {
    host: "localhost",
    port: config:getAsInt("port", default = 7000)
};

map<boolean> voteLog;
boolean initVoteLog = voteLogInit();

public type VoteRequest record{
    int term;
    string candidateID;
    int lastLogIndex;
    int lastLogTerm;
};

public type VoteResponse record{
    boolean granted;
    int term;
};

public type AppendEntries record {
    int term;
    string leaderID;
    int prevLogIndex;
    int prevLogTerm;
    LogEntry[] entries;
    int leaderCommit;
};

public type AppendEntriesResponse record {
    int term;
    boolean sucess;
    int followerMatchIndex;

};
//
//AppendEntries(term, leaderID, prevLogIndex, prevLogTerm, entries[], leaderCommit)
//-> (term, conflictIndex, conflictTerm, success)
service raft bind listener {

    voteResponse(endpoint caller, VoteRequest voteReq, grpc:Headers headers) {
        boolean granted = voteResponseHandle(voteReq);
        VoteResponse res = { granted: granted, term: currentTerm };
        error? err = caller->send(res);
        log:printInfo(err.message but { () => "Server send response : " +
                res.term + " " + res.granted });

        _ = caller->complete();
    }



    appendEntries(endpoint caller, AppendEntries appendEntry, grpc:Headers headers) {
        AppendEntriesResponse res = heartbeatHandle(appendEntry);
        error? err = caller->send(res);
        log:printInfo(err.message but { () => "Server send response : " +
                res.term + " " + res.sucess });

        _ = caller->complete();
    }
}

public function heartbeatHandle(AppendEntries appendEntry) returns AppendEntriesResponse {
    //initLog();
    AppendEntriesResponse res;
    if (currentTerm < appendEntry.term) {
        //stepdown
    }
    if (currentTerm > appendEntry.term) {
        res = { term: currentTerm, sucess: false };
    } else {
        leader = untaint appendEntry.leaderID;
        state = "Follower";
        boolean sucess = appendEntry.prevLogTerm == 0 || (appendEntry.prevLogIndex < lengthof log && log[appendEntry.
                    prevLogIndex].term == appendEntry.prevLogTerm);
        //can parse entries
        int index = 0;
        if (sucess) {
            index = appendEntry.prevLogIndex;
            foreach i in appendEntry.entries{
                index++;
                if (getTerm(index) != i.term) {
                    log[index] = i;//not sure
                }
            }
            commitIndex = untaint min(appendEntry.leaderCommit, index);
        } else {
            index = 0;
        }
        res = { term: currentTerm, sucess: sucess, followerMatchIndex: index };

    }
    io:println(res);
    io:println(log);
    return res;
}



function voteLogInit() returns boolean {
    foreach node in nodeList {
        voteLog[node] = false;
    }
    return true;
}

function voteResponseHandle(VoteRequest voteReq) returns boolean {
    boolean granted;
    int term = voteReq.term;
    if (term > currentTerm) {
        currentTerm = untaint term;
        state = "Follower";
        votedFor = "None";
        //Leader variable init
    }
    if (term < currentTerm) {
        return (false);
    }
    if votedFor != "None" && votedFor != voteReq.candidateID {
        return (false);
    }
    int ourLastLogIndex = (lengthof log) - 1;
    int ourLastLogTerm = -1;
    if (lengthof log != 0) {
        ourLastLogTerm = log[ourLastLogIndex].term;
    }

    if (voteReq.lastLogTerm < ourLastLogTerm) {
        return (false);
    }

    if (voteReq.lastLogTerm == ourLastLogTerm && voteReq.lastLogIndex < ourLastLogIndex) {
        return (false);
    }

    votedFor = untaint voteReq.candidateID;

    return true;

    //
    //VoteRequest m = voteReq;
    //if (currentTerm == m.term && votedFor in [None, peer] &&(m.lastLogTerm > logTerm(len(log)) ||(m.lastLogTerm == logTerm(len(log)) &&m.lastLogIndex >= len(log)))):
}

function getTerm(int index) returns int {
    if (index < 1 || index >= lengthof log) {
        return 0;
    }
    else {
        return log[index].term;
    }
}
