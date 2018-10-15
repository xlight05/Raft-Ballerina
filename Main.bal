import ballerina/io;
public function main() {
    foreach i in 100...103 {
        _=start test(i);
    }
}

function test(int x) {
    worker yay {
        int n = 10;
        foreach i in 1...n {
            io:println(x+" "+i);
        }
    }
}
















// import ballerina/io;
// import ballerina/math;
// import ballerina/runtime;
// import ballerina/task;

// int count;
// task:Timer? timer;
// int interval = 1000;
// public function main() {
//     io:println("Timer task demo");

//     (function() returns error?) onTriggerFunction = cleanup;

//     function(error) onErrorFunction = cleanupError;

//     timer = new task:Timer(onTriggerFunction, onErrorFunction,
//                            interval);
    
    
//     timer.start();
    
//     runtime:sleep(30000); // Temp. workaround to stop the process from exiting.
// }

// function cleanup() returns error? {
//     interval = interval + 1000;
//     (function() returns error?) onTriggerFunction = cleanup;

//     function(error) onErrorFunction = cleanupError;
//     timer.stop();
//      timer = new task:Timer(onTriggerFunction, onErrorFunction,
//                            interval);
//     timer.start();
//     count = count + 1;
    
//     io:println(count);
    
//     if (count >= 10) {
        

//         timer.stop();
//         io:println("Stopped timer");
//     }
//     return ();
// }

// function cleanupError(error e) {
//     io:print("[ERROR] cleanup failed");
//     io:println(e);
// }
