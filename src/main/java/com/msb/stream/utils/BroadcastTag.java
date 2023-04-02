package com.msb.stream.utils;

import scala.reflect.ClassTag;

public class BroadcastTag {
    public static <T> ClassTag<T> classTag(Class<T> clazz) {
        return scala.reflect.ClassManifestFactory.fromClass(clazz);
    }
}
