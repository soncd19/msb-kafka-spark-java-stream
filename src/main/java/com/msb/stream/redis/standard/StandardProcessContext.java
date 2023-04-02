package com.msb.stream.redis.standard;


import com.msb.stream.redis.api.PropertyContext;
import com.msb.stream.redis.api.PropertyValue;
import com.msb.stream.redis.util.PropertyDescriptor;
import org.w3c.dom.Document;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class StandardProcessContext implements PropertyContext {

    private final Map<PropertyDescriptor, String> properties;

    public StandardProcessContext(Document document) throws Exception {
        properties = StandardParserConfig.parser(document);
    }

    @Override
    public PropertyValue getProperty(PropertyDescriptor descriptor) {
        final String setPropertyValue = properties.get(descriptor);
        if (setPropertyValue != null) {
            return new StandardPropertyValue(setPropertyValue);
        }
        final String defaultValue = descriptor.getDefaultValue();
        return new StandardPropertyValue(defaultValue);
    }

    @Override
    public Map<PropertyDescriptor, String> getProperties() {
        return properties;
    }

    @Override
    public Map<String, String> getAllProperties() {
        final Map<String, String> propValueMap = new LinkedHashMap<>();
        for (final Map.Entry<PropertyDescriptor, String> entry : getProperties().entrySet()) {
            propValueMap.put(entry.getKey().getName(), entry.getValue());
        }
        return propValueMap;
    }
}
