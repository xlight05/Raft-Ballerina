import ballerina/io;
import ballerina/runtime;
import ballerina/task;
import ballerina/math;
import ballerina/grpc;
import ballerina/log;
import ballerina/config;

endpoint raftBlockingClient blockingEp {
    url: "http://localhost:9090"
};

boolean init = addNodes();
map<raftBlockingClient> clientMap;

string[] nodeList = ["http://localhost:3000", "http://localhost:4000"];
string leader;
string state = "Follower";
int currentTerm;
LogEntry[] log;
string votedFor = "None";
string currentNode = config:getAsString("ip" + config:getAsString("port"));
int commitIndex = -1;
task:Timer? timer;


map<int> nextIndex;
map<int> matchIndex;

function startRaft() {
    int interval = math:randomInRange(150, 300);

    (function () returns error?) onTriggerFunction = electLeader;

    function (error) onErrorFunction = timerError;

    timer = new task:Timer(onTriggerFunction, onErrorFunction,
        interval);
    timer.start();
}

public type LogEntry record {
    int term;
    string command;
};

public function electLeader() returns error? {
    log:printInfo("Running Leader Election");
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
            if (currentTerm != electionTerm) {
                return;
            }

            int quoram = <int>math:floor(lengthof nodeList / 2.0);
            if (voteCount < quoram) {
                state = "Follower";
            }
            state = "Leader";
            startHeartbeatTimer();
        }
        error err => {
            return err;
        }
    }


    //Reset Timer
    //Send Heartbeats
    log:printInfo(config:getAsInt("port", default = 7000) + " is a " + state);
    return ();
}

function sendVoteRequests(VoteRequest req) returns int|error {
    int voteCount = 0;
    //votes for itself
    foreach node in clientMap {

        //ignore current Node

        blockingEp = node;

        var unionResp = blockingEp->voteResponse(req);

        match unionResp {
            (VoteResponse, grpc:Headers) payload => {
                VoteResponse result;
                grpc:Headers resHeaders;
                (result, resHeaders) = payload;
                if (result.term > currentTerm) {
                    //stepdown
                }
                if (result.granted) {
                    voteCount++;
                }

            }
            error err => {
                return err;
                //io:println("Error from Connector: " + err.message);
            }
        }
    }
    return voteCount;
}

function sendHeartbeats() {
    if (state != "Leader") {
        return;
    }

    foreach node in clientMap {
        string peer = node.cfg.url;
        int nextIndexOfPeer = nextIndex[peer] ?: 0;
        //if (nextIndexOfPeer > lengthof log){
        //
        //}
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
            term:currentTerm,
            leaderID: currentNode,
            prevLogIndex: prevLogIndex,
            prevLogTerm: log[prevLogIndex].term,
            entries: entryList,
            leaderCommit: commitIndex
        };

        blockingEp = node;
        var heartbeatResp = blockingEp->appendEntries(appendEntry);
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
                }
            }
            error => {
                //?? please
            }
        }
    }

}

function addNodes() returns boolean {
    foreach node in nodeList {
        raftBlockingClient client;
        grpc:ClientEndpointConfig cc = { url: node };
        client.init(cc);
        clientMap[node] = client;
        nextIndex[node] = 1;
        matchIndex[node] = 0;
    }
    return true;
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


function startElectionTimer() {
    int interval = math:randomInRange(150, 300);
    timer.stop();
    (function () returns error?) onTriggerFunction = electLeader;

    function (error) onErrorFunction = timerError;
    timer = new task:Timer(onTriggerFunction, onErrorFunction,
        interval);
    timer.start();
}
function startHeartbeatTimer() {
    int interval = math:randomInRange(150, 300);
    timer.stop();
    (function () returns error?) onTriggerFunction = sendHeartbeats;

    function (error) onErrorFunction = timerError;
    timer = new task:Timer(onTriggerFunction, onErrorFunction,
        interval);
    timer.start();
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
function resetHeartbeatTimer() {
    int interval = math:randomInRange(150, 300);
    timer.stop();
    (function () returns error?) onTriggerFunction = sendHeartbeats;

    function (error) onErrorFunction = timerError;
    timer = new task:Timer(onTriggerFunction, onErrorFunction,
        interval);
    timer.start();
}