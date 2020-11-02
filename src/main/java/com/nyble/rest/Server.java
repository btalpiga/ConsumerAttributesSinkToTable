package com.nyble.rest;

import com.nyble.rest.resources.LogLevelRequest;
import com.nyble.rest.resources.Response;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class Server {

    @PutMapping("/logger")
    public Response setLogLevel(@RequestBody LogLevelRequest l){
        String logName = l.getLogName();
        String requestedLevel = l.getLogLevel();
        Logger logger = Logger.getLogger(logName);
        if(requestedLevel == null){
            requestedLevel = "WARN";
        }else{
            requestedLevel = requestedLevel.toUpperCase();
        }

        Level level;
        switch (requestedLevel){
            case ("ERROR"): {level = Level.ERROR; break;}
            case ("WARN"): {level = Level.WARN; break;}
            case ("INFO"): {level = Level.INFO; break;}
            case ("DEBUG"): {level = Level.DEBUG; break;}
            default: {
                level = Level.ALL;
            }
        }
        logger.setLevel(level);
//        logger.debug("Log level changed");
        return Response.success();
    }
}
