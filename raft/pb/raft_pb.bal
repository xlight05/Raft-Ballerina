import ballerina/grpc;
import ballerina/io;

public type raftBlockingStub object {
    public grpc:Client clientEndpoint;
    public grpc:Stub stub;

    function initStub (grpc:Client ep) {
        grpc:Stub navStub = new;
        navStub.initStub(ep, "blocking", DESCRIPTOR_KEY, descriptorMap);
        self.stub = navStub;
    }
    
    function voteResponse (VoteRequest req, grpc:Headers? headers = ()) returns ((VoteResponse, grpc:Headers)|error) {
        
        var unionResp = self.stub.blockingExecute("raft.raft/voteResponse", req, headers = headers);
        match unionResp {
            error payloadError => {
                return payloadError;
            }
            (any, grpc:Headers) payload => {
                grpc:Headers resHeaders;
                any result;
                (result, resHeaders) = payload;
                return (check <VoteResponse>result, resHeaders);
            }
        }
    }
    
    function appendEntries (AppendEntries req, grpc:Headers? headers = ()) returns ((AppendEntriesResponse, grpc:Headers)|error) {
        
        var unionResp = self.stub.blockingExecute("raft.raft/appendEntries", req, headers = headers);
        match unionResp {
            error payloadError => {
                return payloadError;
            }
            (any, grpc:Headers) payload => {
                grpc:Headers resHeaders;
                any result;
                (result, resHeaders) = payload;
                return (check <AppendEntriesResponse>result, resHeaders);
            }
        }
    }
    
};

public type raftStub object {
    public grpc:Client clientEndpoint;
    public grpc:Stub stub;

    function initStub (grpc:Client ep) {
        grpc:Stub navStub = new;
        navStub.initStub(ep, "non-blocking", DESCRIPTOR_KEY, descriptorMap);
        self.stub = navStub;
    }
    
    function voteResponse (VoteRequest req, typedesc listener, grpc:Headers? headers = ()) returns (error?) {
        
        return self.stub.nonBlockingExecute("raft.raft/voteResponse", req, listener, headers = headers);
    }
    
    function appendEntries (AppendEntries req, typedesc listener, grpc:Headers? headers = ()) returns (error?) {
        
        return self.stub.nonBlockingExecute("raft.raft/appendEntries", req, listener, headers = headers);
    }
    
};


public type raftBlockingClient object {
    public grpc:Client client;
    public raftBlockingStub stub;
    public grpc:ClientEndpointConfig cfg;
    public function init (grpc:ClientEndpointConfig config) {
        // initialize client endpoint.
        grpc:Client c = new;
        c.init(config);
        self.client = c;
        // initialize service stub.
        raftBlockingStub s = new;
        s.initStub(c);
        self.stub = s;
        cfg = config;
    }

    public function getCallerActions () returns raftBlockingStub {
        return self.stub;
    }
};

public type raftClient object {
    public grpc:Client client;
    public raftStub stub;

    public function init (grpc:ClientEndpointConfig config) {
        // initialize client endpoint.
        grpc:Client c = new;
        c.init(config);
        self.client = c;
        // initialize service stub.
        raftStub s = new;
        s.initStub(c);
        self.stub = s;
    }

    public function getCallerActions () returns raftStub {
        return self.stub;
    }
};


//type VoteRequest record {
//    int term;
//    string candidateID;
//    int lastLogIndex;
//    int lastLogTerm;
//
//};
//
//type VoteResponse record {
//    boolean granted;
//    int term;
//
//};
//
//type AppendEntries record {
//    int term;
//    string leaderID;
//    int prevLogIndex;
//    int prevLogTerm;
//    LogEntry[] entries;
//    int leaderCommit;
//
//};
//
//type LogEntry record {
//    int term;
//    string command;
//
//};
//
//type AppendEntriesResponse record {
//    int term;
//    boolean sucess;
//
//};


@final string DESCRIPTOR_KEY = "raft.raft.proto";
map descriptorMap = {
"raft.raft.proto":"0A0A726166742E70726F746F1204726166742289010A0B566F74655265717565737412120A047465726D18012001280352047465726D12200A0B63616E6469646174654944180220012809520B63616E646964617465494412220A0C6C6173744C6F67496E646578180320012803520C6C6173744C6F67496E64657812200A0B6C6173744C6F675465726D180420012803520B6C6173744C6F675465726D223C0A0C566F7465526573706F6E736512180A076772616E74656418012001280852076772616E74656412120A047465726D18022001280352047465726D22D3010A0D417070656E64456E747269657312120A047465726D18012001280352047465726D121A0A086C6561646572494418022001280952086C6561646572494412220A0C707265764C6F67496E646578180320012803520C707265764C6F67496E64657812200A0B707265764C6F675465726D180420012803520B707265764C6F675465726D12280A07656E747269657318052003280B320E2E726166742E4C6F67456E7472795207656E747269657312220A0C6C6561646572436F6D6D6974180620012803520C6C6561646572436F6D6D697422380A084C6F67456E74727912120A047465726D18012001280352047465726D12180A07636F6D6D616E641802200128095207636F6D6D616E6422430A15417070656E64456E7472696573526573706F6E736512120A047465726D18012001280352047465726D12160A0673756365737318022001280852067375636573733280010A047261667412350A0C766F7465526573706F6E736512112E726166742E566F7465526571756573741A122E726166742E566F7465526573706F6E736512410A0D617070656E64456E747269657312132E726166742E417070656E64456E74726965731A1B2E726166742E417070656E64456E7472696573526573706F6E7365620670726F746F33"

};
