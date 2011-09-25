package uk.co.samstokes.storm.scheme;

import java.io.UnsupportedEncodingException;

import java.util.Collections;
import java.util.List;

import org.json.simple.JSONValue;

import backtype.storm.spout.Scheme;

import backtype.storm.tuple.Fields;


public class JSONScheme implements Scheme {
    private static final long serialVersionUID = -7734176307841199017L;

    private final String encoding;


    public JSONScheme(String encoding) {
        this.encoding = encoding;
    }
    public JSONScheme() {
        this("UTF-8");
    }


    @Override
    public List<Object> deserialize(byte[] bytes) {
        final String chars;
        try {
            chars = new String(bytes, encoding);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        final Object json = JSONValue.parse(chars);
        return Collections.singletonList(json);
    }


    @Override
    public Fields getOutputFields() {
        return new Fields("object");
    }
}
