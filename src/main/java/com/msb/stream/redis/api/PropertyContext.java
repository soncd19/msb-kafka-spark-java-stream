package com.msb.stream.redis.api;


import com.msb.stream.redis.util.PropertyDescriptor;

import java.io.Serializable;
import java.util.Map;

public interface PropertyContext extends Serializable {

    PropertyValue getProperty(PropertyDescriptor descriptor);

    Map<PropertyDescriptor, String> getProperties();

    Map<String, String> getAllProperties();
}
