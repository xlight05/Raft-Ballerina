import ballerina/io;
import ballerina/runtime;
import ballerina/task;
import ballerina/math;
import ballerina/grpc;
import ballerina/log;
import ballerina/config;

//TODO make proper election timeout with MIN MAX timeout
//TODO make proper hearbeat timeout
//TODO Test dynamic node changes

endpoint raftBlockingClient blockingEp {
    url: "http://localhost:3000"
};

map<raftBlockingClient> clientMap;

string leader;
string state = "Follower";
int currentTerm;
LogEntry[] log=[{}];
string votedFor = "None";
string currentNode = config:getAsString("ip")+":"+config:getAsString("port");
int commitIndex = 0;
int lastApplied =0;
task:Timer? timer;
task:Timer? heartbeatTimer;
map<int> candVoteLog;

map<int> nextIndex;
map<int> matchIndex;

public function startRaft() {
    //cheat for first node lol
    log[lengthof log] = { term: 1, command: "NA "+currentNode };
    apply("NA "+currentNode);
    commitIndex++;
    lastApplied++;
    //raftBlockingClient client;
    //grpc:ClientEndpointConfig cc = { url: currentNode };
    //client.init(cc);
    //clientMap[currentNode] = client;
    nextIndex[currentNode] = 1;
    matchIndex[currentNode] = 0;


    int interval = math:randomInRange(150, 300);

    (function () returns error?) onTriggerFunction = electLeader;

    function (error) onErrorFunction = timerError;

    timer = new task:Timer(onTriggerFunction, onErrorFunction,
        interval);
    timer.start();
}

public function joinRaft() {
    nextIndex[currentNode] = 1;
    matchIndex[currentNode] = 0;


    int interval = math:randomInRange(150, 300);

    (function () returns error?) onTriggerFunction = electLeader;

    function (error) onErrorFunction = timerError;

    timer = new task:Timer(onTriggerFunction, onErrorFunction,
        interval,delay=interval);
    timer.start();
}

function electLeader() returns error? {
    log:printInfo("Starting Leader Election by "+currentNode);
    //addNodes();//temp
    if (state == "Leader") {
        return;
    }
    currentTerm++;
    int electionTerm = currentTerm;
    votedFor = currentNode;
    state = "Candidate";
    VoteRequest req = { term: currentTerm, candidateID: currentNode, lastLogIndex: (lengthof log) - 1, lastLogTerm: log[(
        lengthof log) - 1].term };
    var voteResp = sendVoteRequests(req);
    match voteResp {
        int voteCount => {
            //check if another appendEntry came
            if (currentTerm != electionTerm) {
                return;
            }
            int quoram = <int>math:floor(lengthof clientMap / 2.0); //0 for first node
            if (voteCount < quoram) {
                state = "Follower";
                votedFor = "None";
                heartbeatTimer.stop();//not sure if started
                resetElectionTimer();
                //stepdown
            }else {
                state = "Leader";
                timer.stop();
                foreach i in clientMap {
                    nextIndex[i.cfg.url] = lengthof log;
                }
                startHeartbeatTimer();
            }
        }
        error err => {
            return err;
        }
    }
    log:printInfo (currentNode +" is a "+state);
    return ();
}

function sendVoteRequests(VoteRequest req) returns int|error {
    //votes for itself
    future[] futureVotes;
    foreach node in clientMap {
        if (node.cfg.url==currentNode){
            continue;
        }
        candVoteLog[node.cfg.url] = -1;
        future asyncRes = start seperate(node, req);
        futureVotes[lengthof futureVotes] = asyncRes;
        //ignore current Node
    }
    foreach i in futureVotes { //change this in to quoram
        _ = await i;
    }
    int count = 1;
    foreach item in candVoteLog {
        if (item == 1) {
            count++;
        }
        if (item==-2){
            candVoteLog.clear();
            return 0;
        }
    }
    candVoteLog.clear();
    //int count;
    ////busy while :/ fix using future map
    //while (true) {
    //    runtime:
    //    sleep(50);
    //    count = 0;
    //    foreach item in candVoteLog {
    //        if (item == 1) {
    //            count++;
    //        }
    //    }
    //    if (count > math:floor(<int>lengthof clientMap / 2.0)) {
    //        break;
    //    } else {
    //        //wait a bit
    //        runtime:sleep(100);
    //        //change // 150/2
    //        //check again
    //        break;
    //    }
    //}

    return count;
}

function seperate(raftBlockingClient node, VoteRequest req) {
    blockingEp = node;

    var unionResp = blockingEp->voteResponseRPC(req);

    match unionResp {
        (VoteResponse, grpc:Headers) payload => {
            VoteResponse result;
            grpc:Headers resHeaders;
            (result, resHeaders) = payload;
            if (result.term > currentTerm) {
                candVoteLog[node.cfg.url] = -2;
                return;
                //stepdown
            }
            if (result.granted) {
                candVoteLog[node.cfg.url] = 1;
            }
            else {
                candVoteLog[node.cfg.url] = 0;
            }

        }
        error err => {
            candVoteLog[node.cfg.url] = 0;
        }
    }
}

function sendHeartbeats() {
    if (state != "Leader") {
        return;
    }
    log:printInfo("Sending heartbeats");
    future[] heartbeatAsync;
    foreach node in clientMap {
        if (node.cfg.url==currentNode){
            continue;
        }
        future asy = start heartbeatChannel(node);
        heartbeatAsync[lengthof heartbeatAsync] = asy;
    }
    foreach item in heartbeatAsync {
        var x = await item;
    }
    commitEntry();
}

function heartbeatChannel(raftBlockingClient node) {
    if (state != "Leader") {
        return;
    }
    string peer = node.cfg.url;
    int nextIndexOfPeer = nextIndex[peer] ?: 0;
    int prevLogIndex = nextIndexOfPeer - 1;
    int prevLogTerm = 0;
    if (prevLogIndex > 0) {
        prevLogTerm = log[prevLogIndex].term;
    }
    //        int lastEntry = min(lengthof log,nextIndexOfPeer);
    LogEntry[] entryList;
    foreach i in prevLogIndex...lengthof log - 1 {
        entryList[lengthof entryList] = log[i];
    }
    AppendEntries appendEntry = {
        term: currentTerm,
        leaderID: currentNode,
        prevLogIndex: prevLogIndex,
        prevLogTerm: log[prevLogIndex].term,
        entries: entryList,
        leaderCommit: commitIndex
    };

    blockingEp = node;
    var heartbeatResp = blockingEp->appendEntriesRPC(appendEntry);
    match heartbeatResp {
        (AppendEntriesResponse, grpc:Headers) payload => {
            AppendEntriesResponse result;
            grpc:Headers resHeaders;
            (result, resHeaders) = payload;
            if (result.sucess) {
                matchIndex[peer] = result.followerMatchIndex;
                nextIndex[peer] = result.followerMatchIndex + 1; //atomicc
            } else {
                nextIndex[peer] = max(1, nextIndexOfPeer - 1);
                heartbeatChannel(node);
            }
        }
        error => {
            //?? please
        }
    }
    commitEntry();
}

function commitEntry() {
    if (state != "Leader") {
        return;
    }
    int item = lengthof log - 1;
    while (item > commitIndex) {
        int replicatedCount = 0;
        foreach server in clientMap {
            if (matchIndex[server.cfg.url] == item) {
                replicatedCount++;
            }
        }
        if (replicatedCount >= math:floor(lengthof clientMap / 2.0)) {
            commitIndex = item;
            apply(log[item].command);
            break;
        }
        item = item - 1;
    }
}


function min(int x, int y) returns int {
    if (x < y) {
        return x;
    } else {
        return y;
    }
}

function max(int x, int y) returns int {
    if (x > y) {
        return x;
    }
    else {
        return y;
    }
}
function timerError(error e) {
    io:println(e);
}


function stepDown() {
    
}

function resetElectionTimer() {
    int interval = math:randomInRange(150, 300);
    timer.stop();
    (function () returns error?) onTriggerFunction = electLeader;

    function (error) onErrorFunction = timerError;
    timer = new task:Timer(onTriggerFunction, onErrorFunction,
        interval);
    timer.start();
}

function startHeartbeatTimer() {
    int interval = 100; //change
    (function () returns error?) onTriggerFunction = sendHeartbeats;

    function (error) onErrorFunction = timerError;
    heartbeatTimer = new task:Timer(onTriggerFunction, onErrorFunction,
        interval);
    heartbeatTimer.start();
}

function startElectionTimer() {
    int interval = math:randomInRange(150, 300); //change
    (function () returns error?) onTriggerFunction = electLeader;

    function (error) onErrorFunction = timerError;
    timer = new task:Timer(onTriggerFunction, onErrorFunction,
        interval,delay=interval);
    timer.start();
}

function clientRequest(string command) returns boolean {
    if (state == "Leader") {
        log[lengthof log] = { term: currentTerm, command: command };
        future ee = start sendHeartbeats();
        _ = await ee;
        //check if commited moree
        return true;
    } else {
        return false;
    }
}

function addNode(string ip) returns ConfigChangeResponse {
    if (state != "Leader") {
        return { sucess: false, leaderHint: leader };
    } else {
        string command = "NA "+ip;
        //or commit
        boolean sucess = clientRequest(command);
        return { sucess: sucess, leaderHint: leader };
    }
}

function apply(string command)  {
    if (command.substring(0,2)=="NA"){
        string ip = command.split(" ")[1];
        raftBlockingClient client;
        grpc:ClientEndpointConfig cc = { url: ip };
        client.init(cc);
        clientMap[ip] = client;
        nextIndex[ip]=1;
        matchIndex[ip]=0;

    }
    log:printInfo (command+" Applied!!");
}

// function isQuoram(int count) returns boolean {
//     if (count==0){
//         return true;
//     }
// }