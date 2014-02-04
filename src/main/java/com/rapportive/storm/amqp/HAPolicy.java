package com.rapportive.storm.amqp;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class HAPolicy implements Serializable {
    private static final long serialVersionUID = -5276009714329009060L;
    
    private Map<String, Object> queueProperties;

    private HAPolicy(Map<String, Object> queueParams) {
        this.queueProperties = queueParams;
    }
    
    public static HAPolicy all() {
        HashMap<String, Object> args = new HashMap<String, Object>();
        args.put("x-ha-policy", "all");  
        return new HAPolicy(args);
    }
    
    public static HAPolicy nodes(String... nodeNames) {        
        if (nodeNames.length < 1) 
            throw new IllegalArgumentException("List of nodenames should contain at least one name");
        
        HashMap<String, Object> args = new HashMap<String, Object>();
        args.put("x-ha-policy", "nodes");
        args.put("x-ha-x-ha-policy-params", Arrays.asList(nodeNames));
        
        return new HAPolicy(args);        
    }
    
    public Map<String, Object> asQueueProperies() {
        return queueProperties;
    }
}
