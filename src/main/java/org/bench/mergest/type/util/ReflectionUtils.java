/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.bench.mergest.type.util;


import org.bench.mergest.type.hadoop.conf.Configurable;
import org.bench.mergest.type.hadoop.conf.Configuration;
import org.bench.mergest.type.hadoop.serializer.SerializationFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class ReflectionUtils {

    private static final Class<?>[] EMPTY_ARRAY = new Class[]{};
    volatile private static SerializationFactory serialFactory = null;

    /**
     * Cache of constructors for each class. Pins the classes so they
     * can't be garbage collected until ReflectionUtils can be collected.
     */
    private static final Map<Class<?>, Constructor<?>> CONSTRUCTOR_CACHE =
            new ConcurrentHashMap<Class<?>, Constructor<?>>();

    public static <T> T newInstance(Class<T> clazz) {
        try {
            return clazz.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Check and set 'configuration' if necessary.
     *
     * @param theObject object for which to set configuration
     * @param conf      Configuration
     */
    public static void setConf(Object theObject, Configuration conf) {
        if (conf != null) {
            if (theObject instanceof Configurable) {
                ((Configurable) theObject).setConf(conf);
            }
            setJobConf(theObject, conf);
        }
    }

    /**
     * This code is to support backward compatibility and break the compile
     * time dependency of core on mapred.
     * This should be made deprecated along with the mapred package HADOOP-1230.
     * Should be removed when mapred package is removed.
     */
    private static void setJobConf(Object theObject, Configuration conf) {
        //If JobConf and JobConfigurable are in classpath, AND
        //theObject is of type JobConfigurable AND
        //conf is of type JobConf then
        //invoke configure on theObject
        try {
            Class<?> jobConfClass =
                    conf.getClassByNameOrNull("org.apache.hadoop.mapred.JobConf");
            if (jobConfClass == null) {
                return;
            }

            Class<?> jobConfigurableClass =
                    conf.getClassByNameOrNull("org.apache.hadoop.mapred.JobConfigurable");
            if (jobConfigurableClass == null) {
                return;
            }
            if (jobConfClass.isAssignableFrom(conf.getClass()) &&
                    jobConfigurableClass.isAssignableFrom(theObject.getClass())) {
                Method configureMethod =
                        jobConfigurableClass.getMethod("configure", jobConfClass);
                configureMethod.invoke(theObject, conf);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error in configuring object", e);
        }
    }

    /**
     * Create an object for the given class and initialize it from conf
     *
     * @param theClass class of which an object is created
     * @param conf     Configuration
     * @return a new object
     */
    @SuppressWarnings("unchecked")
    public static <T> T newInstance(Class<T> theClass, Configuration conf) {
        T result;
        try {
            Constructor<T> meth = (Constructor<T>) CONSTRUCTOR_CACHE.get(theClass);
            if (meth == null) {
                meth = theClass.getDeclaredConstructor(EMPTY_ARRAY);
                meth.setAccessible(true);
                CONSTRUCTOR_CACHE.put(theClass, meth);
            }
            result = meth.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        setConf(result, conf);
        return result;
    }

    @SuppressWarnings("unchecked")
    public static <T> Class<T> getTemplateType(Class<?> clazz, int num) {
        return (Class<T>) getSuperTemplateTypes(clazz)[num];
    }

    @SuppressWarnings("unchecked")
    public static <T> Class<T> getTemplateType(Class<?> clazz, Class<?> classWithParameter, int num) {
        return (Class<T>) getSuperTemplateTypes(clazz)[num];
    }

    public static <T> Class<T> getTemplateType1(Class<?> clazz) {
        return getTemplateType(clazz, 0);
    }

    public static <T> Class<T> getTemplateType2(Class<?> clazz) {
        return getTemplateType(clazz, 1);
    }

    public static <T> Class<T> getTemplateType3(Class<?> clazz) {
        return getTemplateType(clazz, 2);
    }

    public static <T> Class<T> getTemplateType4(Class<?> clazz) {
        return getTemplateType(clazz, 3);
    }

    public static <T> Class<T> getTemplateType5(Class<?> clazz) {
        return getTemplateType(clazz, 4);
    }

    public static <T> Class<T> getTemplateType6(Class<?> clazz) {
        return getTemplateType(clazz, 5);
    }

    public static <T> Class<T> getTemplateType7(Class<?> clazz) {
        return getTemplateType(clazz, 6);
    }

    public static <T> Class<T> getTemplateType8(Class<?> clazz) {
        return getTemplateType(clazz, 7);
    }

    public static Class<?>[] getSuperTemplateTypes(Class<?> clazz) {
        Type type = clazz.getGenericSuperclass();
        while (true) {
            if (type instanceof ParameterizedType) {
                return getTemplateTypes((ParameterizedType) type);
            }

            if (clazz.getGenericSuperclass() == null) {
                throw new IllegalArgumentException();
            }

            type = clazz.getGenericSuperclass();
            clazz = clazz.getSuperclass();
        }
    }

    public static Class<?>[] getSuperTemplateTypes(Class<?> clazz, Class<?> searchedSuperClass) {
        if (clazz == null || searchedSuperClass == null) {
            throw new NullPointerException();
        }

        Class<?> superClass = null;
        do {
            superClass = clazz.getSuperclass();
            if (superClass == searchedSuperClass) {
                break;
            }
        }
        while ((clazz = superClass) != null);

        if (clazz == null) {
            throw new IllegalArgumentException("The searched for superclass is not a superclass of the given class.");
        }

        final Type type = clazz.getGenericSuperclass();
        if (type instanceof ParameterizedType) {
            return getTemplateTypes((ParameterizedType) type);
        } else {
            throw new IllegalArgumentException("The searched for superclass is not a generic class.");
        }
    }

    public static Class<?>[] getTemplateTypes(ParameterizedType paramterizedType) {
        Class<?>[] types = new Class<?>[paramterizedType.getActualTypeArguments().length];
        int i = 0;
        for (Type templateArgument : paramterizedType.getActualTypeArguments()) {
            assert (templateArgument instanceof Class<?>);
            types[i++] = (Class<?>) templateArgument;
        }
        return types;
    }

    public static Class<?>[] getTemplateTypes(Class<?> clazz) {
        Type type = clazz.getGenericSuperclass();
        assert (type instanceof ParameterizedType);
        ParameterizedType paramterizedType = (ParameterizedType) type;
        Class<?>[] types = new Class<?>[paramterizedType.getActualTypeArguments().length];
        int i = 0;
        for (Type templateArgument : paramterizedType.getActualTypeArguments()) {
            assert (templateArgument instanceof Class<?>);
            types[i++] = (Class<?>) templateArgument;
        }
        return types;
    }

    /**
     * Private constructor to prevent instantiation.
     */
    private ReflectionUtils() {
        throw new RuntimeException();
    }
}
