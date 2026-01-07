/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package com.huawei.boostkit.spark.udf;

import static java.lang.String.format;

import com.google.common.annotations.VisibleForTesting;

import nova.hetu.omniruntime.type.DataType.DataTypeId;
import nova.hetu.omniruntime.utils.OmniErrorType;
import nova.hetu.omniruntime.utils.OmniRuntimeException;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * hive udf executor for call evaluate interface to handle data.
 * if any exception is thrown, the C++ side will handle the exception.
 *
 * @since 2022-07-25
 */
public class HiveUdfExecutor {
    private static final String OMNI_CONF_FILE_PATH = format("conf%somni.conf", File.separatorChar);
    private static final Pattern PROPERTY_PATTERN = Pattern.compile("(.*)=+(.*)");
    private static String loadExceptionMsg;
    private static URLClassLoader classLoader;

    static {
        try {
            String hiveUdfDir = getHiveUdfDir();
            classLoader = createClassLoader(hiveUdfDir);
        } catch (IOException | OmniRuntimeException e) {
            loadExceptionMsg = e.getMessage();
        }
    }

    private static URLClassLoader createClassLoader(String udfPath) {
        File udfDir = new File(udfPath);
        if (!udfDir.exists()) {
            loadExceptionMsg = udfPath + " does not exist.";
            return null;
        }
        if (!udfDir.isDirectory()) {
            loadExceptionMsg = udfPath + " is not a directory.";
            return null;
        }
        File[] jarFiles = udfDir.listFiles();
        if (jarFiles == null || jarFiles.length == 0) {
            loadExceptionMsg = udfPath + " is invalid or has no files.";
            return null;
        }

        List<URL> urls = new ArrayList<>();
        for (File file : jarFiles) {
            try {
                urls.add(file.toURI().toURL());
            } catch (MalformedURLException e) {
                loadExceptionMsg = file + " to url failed.";
                return null;
            }
        }
        return URLClassLoader.newInstance(urls.toArray(new URL[urls.size()]), HiveUdfExecutor.class.getClassLoader());
    }

    private static String getHiveUdfDir() throws IOException {
        String omniHomeDir = System.getenv("OMNI_HOME");
        if (omniHomeDir == null || omniHomeDir.equals("")) {
            omniHomeDir = File.separator + "opt";
        }
        omniHomeDir = Paths.get("/", omniHomeDir).normalize().toString();

        String omniConfPath = omniHomeDir + File.separatorChar + OMNI_CONF_FILE_PATH;
        BufferedReader bufferedReader = new BufferedReader(
                new InputStreamReader(new FileInputStream(omniConfPath), StandardCharsets.UTF_8));
        String property;
        while ((property = bufferedReader.readLine()) != null) {
            Matcher matcher = PROPERTY_PATTERN.matcher(property.trim());
            if (!matcher.matches()) {
                continue;
            }
            if (matcher.group(1).trim().equals("hiveUdfDir")) {
                String hiveUdfDir = matcher.group(2).trim();
                return (hiveUdfDir.charAt(0) == '.')
                        ? Paths.get(omniHomeDir, hiveUdfDir).normalize().toString()
                        : Paths.get("/", hiveUdfDir).normalize().toString();
            }
        }
        String msg = "Do not configure the hiveUdfDir in " + omniConfPath + ".";
        throw new OmniRuntimeException(OmniErrorType.OMNI_JAVA_UDF_ERROR, msg);
    }

    /**
     * execute the hive udf for given udf path for test, the number of input and
     * output data is single one.
     *
     * @param udfPath the hive udf path
     * @param udfClassName the hive udf class name
     * @param paramTypes the hive udf param types
     * @param retType the hive udf return type
     * @param inputValueAddr the addresses of input values
     * @param inputNullAddr the addresses of input nulls
     * @param inputLengthAddr the addresses of input lengths
     * @param outputValueAddr the address of output value
     * @param outputNullAddr the address of output null
     * @param outputLengthAddr the address of output length
     */
    @VisibleForTesting
    public static void executeSingle(String udfPath, String udfClassName, DataTypeId[] paramTypes, DataTypeId retType,
                                     long inputValueAddr, long inputNullAddr, long inputLengthAddr, long outputValueAddr, long outputNullAddr,
                                     long outputLengthAddr) {
        URLClassLoader loader = createClassLoader(udfPath);
        executeSingle(loader, udfClassName, paramTypes, retType, inputValueAddr, inputNullAddr, inputLengthAddr,
                outputValueAddr, outputNullAddr, outputLengthAddr);
        try {
            loader.close();
        } catch (IOException e) {
            throw new OmniRuntimeException(OmniErrorType.OMNI_JAVA_UDF_ERROR,
                    "Close class loader failed since " + e + ".");
        }
    }

    /**
     * execute the hive udf, the number of input and output data is single one.
     *
     * @param udfClassName the hive udf class name
     * @param paramTypes the hive udf param types
     * @param retType the hive udf return type
     * @param inputValueAddr the addresses of input values
     * @param inputNullAddr the addresses of input nulls
     * @param inputLengthAddr the addresses of input lengths
     * @param outputValueAddr the address of output value
     * @param outputNullAddr the address of output null
     * @param outputLengthAddr the address of output length
     */
    public static void executeSingle(String udfClassName, DataTypeId[] paramTypes, DataTypeId retType,
                                     long inputValueAddr, long inputNullAddr, long inputLengthAddr, long outputValueAddr, long outputNullAddr,
                                     long outputLengthAddr) {
        if (classLoader == null) {
            throw new OmniRuntimeException(OmniErrorType.OMNI_JAVA_UDF_ERROR, loadExceptionMsg);
        }
        executeSingle(classLoader, udfClassName, paramTypes, retType, inputValueAddr, inputNullAddr, inputLengthAddr,
                outputValueAddr, outputNullAddr, outputLengthAddr);
    }

    private static void executeSingle(URLClassLoader loader, String udfClassName, DataTypeId[] paramTypes,
                                      DataTypeId retType, long inputValueAddr, long inputNullAddr, long inputLengthAddr, long outputValueAddr,
                                      long outputNullAddr, long outputLengthAddr) {
        Method method;
        Object udfObj;
        try {
            Class<?> udfClass = Class.forName(udfClassName, true, loader);
            if (UDF.class.isAssignableFrom(udfClass)) {
                // find the valid method
                method = findValidMethod(udfClass, paramTypes, retType);

                // create udf object
                udfObj = udfClass.getConstructor().newInstance();
            } else {
                String msg = "Not support " + udfClassName + " now.";
                throw new OmniRuntimeException(OmniErrorType.OMNI_JAVA_UDF_ERROR, msg);
            }
        } catch (ClassNotFoundException | NoSuchMethodException | SecurityException | InstantiationException
                 | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            String msg = udfClassName + " load failed since " + e + ".";
            throw new OmniRuntimeException(OmniErrorType.OMNI_JAVA_UDF_ERROR, msg);
        }

        try {
            Object[] inputArgObjects = getInputObjects(paramTypes, inputValueAddr, inputNullAddr, inputLengthAddr);
            Object result = method.invoke(udfObj, inputArgObjects);
            setResult(retType, outputValueAddr, outputNullAddr, outputLengthAddr, result);
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            String msg = udfClassName + " invoke failed since " + e.getCause() + ".";
            throw new OmniRuntimeException(OmniErrorType.OMNI_JAVA_UDF_ERROR, msg);
        }
    }

    private static Object[] getInputObjects(DataTypeId[] paramTypes, long inputValueAddr, long inputNullAddr,
                                            long inputLengthAddr) {
        int valueOffset = 0;
        Object[] inputArgObjects = new Object[paramTypes.length];
        for (int i = 0; i < paramTypes.length; i++) {
            switch (paramTypes[i]) {
                case OMNI_INT:
                case OMNI_DATE32: {
                    inputArgObjects[i] = (UdfUtil.UNSAFE.getByte(inputNullAddr + i) == 1)
                            ? null
                            : UdfUtil.UNSAFE.getInt(inputValueAddr + valueOffset);
                    valueOffset += Integer.BYTES;
                    break;
                }
                case OMNI_LONG: {
                    inputArgObjects[i] = (UdfUtil.UNSAFE.getByte(inputNullAddr + i) == 1)
                            ? null
                            : UdfUtil.UNSAFE.getLong(inputValueAddr + valueOffset);
                    valueOffset += Long.BYTES;
                    break;
                }
                case OMNI_DOUBLE: {
                    inputArgObjects[i] = (UdfUtil.UNSAFE.getByte(inputNullAddr + i) == 1)
                            ? null
                            : UdfUtil.UNSAFE.getDouble(inputValueAddr + valueOffset);
                    valueOffset += Double.BYTES;
                    break;
                }
                case OMNI_BOOLEAN: {
                    inputArgObjects[i] = (UdfUtil.UNSAFE.getByte(inputNullAddr + i) == 1)
                            ? null
                            : UdfUtil.UNSAFE.getByte(inputValueAddr + valueOffset) == 1;
                    valueOffset += Byte.BYTES;
                    break;
                }
                case OMNI_SHORT: {
                    inputArgObjects[i] = (UdfUtil.UNSAFE.getByte(inputNullAddr + i) == 1)
                            ? null
                            : UdfUtil.UNSAFE.getShort(inputValueAddr + valueOffset);
                    valueOffset += Short.BYTES;
                    break;
                }
                case OMNI_CHAR:
                case OMNI_VARCHAR: {
                    inputArgObjects[i] = getStringInputObject(inputValueAddr, inputNullAddr, inputLengthAddr,
                            valueOffset, i);
                    valueOffset += Long.BYTES; // for char or varchar, we store the pointer in input value
                    break;
                }
                default: {
                    String msg = paramTypes[i] + " not supported now.";
                    throw new OmniRuntimeException(OmniErrorType.OMNI_JAVA_UDF_ERROR, msg);
                }
            }
        }
        return inputArgObjects;
    }

    private static Object getStringInputObject(long inputValueAddr, long inputNullAddr, long inputLengthAddr,
                                               int valueOffset, int paramIdx) {
        if (UdfUtil.UNSAFE.getByte(inputNullAddr + paramIdx) == 1) {
            return null;
        } else {
            int length = UdfUtil.UNSAFE.getInt(inputLengthAddr + (long) paramIdx * Integer.BYTES);
            long valueAddr = UdfUtil.UNSAFE.getLong(inputValueAddr + valueOffset);
            byte[] chars = UdfUtil.getBytes(valueAddr, 0, length);
            return new String(chars, StandardCharsets.UTF_8);
        }
    }

    private static void setResult(DataTypeId retType, long outputValueAddr, long outputNullAddr, long outputLengthAddr,
                                  Object result) {
        if (result == null) {
            UdfUtil.UNSAFE.putByte(outputNullAddr, (byte) 1);
            if (retType == DataTypeId.OMNI_CHAR || retType == DataTypeId.OMNI_VARCHAR) {
                UdfUtil.UNSAFE.putInt(outputLengthAddr, 0);
            }
            return;
        }
        UdfUtil.UNSAFE.putByte(outputNullAddr, (byte) 0);
        switch (retType) {
            case OMNI_INT:
                UdfUtil.UNSAFE.putInt(outputValueAddr, (int) result);
                break;
            case OMNI_LONG:
                UdfUtil.UNSAFE.putLong(outputValueAddr, (long) result);
                break;
            case OMNI_DOUBLE:
                UdfUtil.UNSAFE.putDouble(outputValueAddr, (double) result);
                break;
            case OMNI_BOOLEAN:
                UdfUtil.UNSAFE.putByte(outputValueAddr, (byte) ((boolean) result ? 1 : 0));
                break;
            case OMNI_SHORT:
                UdfUtil.UNSAFE.putShort(outputValueAddr, (short) result);
                break;
            case OMNI_CHAR:
            case OMNI_VARCHAR: {
                byte[] chars = ((String) result).getBytes(StandardCharsets.UTF_8);
                UdfUtil.putBytes(outputValueAddr, 0, chars);
                UdfUtil.UNSAFE.putInt(outputLengthAddr, chars.length);
                break;
            }
            default: {
                String msg = retType + " not supported now.";
                throw new OmniRuntimeException(OmniErrorType.OMNI_JAVA_UDF_ERROR, msg);
            }
        }
    }

    /**
     * execute the hive udf for gaven udf path for test, the number of input and
     * output is multiple.
     *
     * @param udfPath the hive udf path
     * @param udfClassName the hive udf class name
     * @param paramTypes the hive udf param types
     * @param retType the hive udf return type
     * @param inputValueAddr the addresses of input values
     * @param inputNullAddr the addresses of input nulls
     * @param inputLengthAddr the addresses of input lengths
     * @param rowCount the row count of one batch
     * @param outputValueAddr the address of output value
     * @param outputNullAddr the address of output null
     * @param outputLengthAddr the address of output length
     * @param outputStateAddr the address of output state
     */
    @VisibleForTesting
    public static void executeBatch(String udfPath, String udfClassName, DataTypeId[] paramTypes, DataTypeId retType,
                                    long inputValueAddr, long inputNullAddr, long inputLengthAddr, int rowCount, long outputValueAddr,
                                    long outputNullAddr, long outputLengthAddr, long outputStateAddr) {
        URLClassLoader loader = createClassLoader(udfPath);
        executeBatch(loader, udfClassName, paramTypes, retType, inputValueAddr, inputNullAddr, inputLengthAddr,
                rowCount, outputValueAddr, outputNullAddr, outputLengthAddr, outputStateAddr);
        try {
            loader.close();
        } catch (IOException e) {
            throw new OmniRuntimeException(OmniErrorType.OMNI_JAVA_UDF_ERROR,
                    "Close class loader failed since " + e + ".");
        }
    }

    /**
     * execute the hive udf, the number of input and output is multiple.
     *
     * @param udfClassName the hive udf class name
     * @param paramTypes the hive udf param types
     * @param retType the hive udf return type
     * @param inputValueAddr the addresses of input values
     * @param inputNullAddr the addresses of input nulls
     * @param inputLengthAddr the addresses of input lengths
     * @param rowCount the row count
     * @param outputValueAddr the address of output value
     * @param outputNullAddr the address of output null
     * @param outputLengthAddr the address of output length
     * @param outputStateAddr the address of output state
     */
    public static void executeBatch(String udfClassName, DataTypeId[] paramTypes, DataTypeId retType,
                                    long inputValueAddr, long inputNullAddr, long inputLengthAddr, int rowCount, long outputValueAddr,
                                    long outputNullAddr, long outputLengthAddr, long outputStateAddr) {
        if (classLoader == null) {
            throw new OmniRuntimeException(OmniErrorType.OMNI_JAVA_UDF_ERROR, loadExceptionMsg);
        }
        executeBatch(classLoader, udfClassName, paramTypes, retType, inputValueAddr, inputNullAddr, inputLengthAddr,
                rowCount, outputValueAddr, outputNullAddr, outputLengthAddr, outputStateAddr);
    }

    private static void executeBatch(URLClassLoader loader, String udfClassName, DataTypeId[] paramTypes,
                                     DataTypeId retType, long inputValueAddr, long inputNullAddr, long inputLengthAddr, int rowCount,
                                     long outputValueAddr, long outputNullAddr, long outputLengthAddr, long outputStateAddr) {
        // load udf class
        Method method;
        Object udfObj;
        try {
            Class<?> udfClass = Class.forName(udfClassName, true, loader);
            if (UDF.class.isAssignableFrom(udfClass)) {
                // find the valid method
                method = findValidMethod(udfClass, paramTypes, retType);

                // create udf object
                udfObj = udfClass.getConstructor().newInstance();
            } else {
                String msg = "Does not support " + udfClassName + " now.";
                throw new OmniRuntimeException(OmniErrorType.OMNI_JAVA_UDF_ERROR, msg);
            }
        } catch (ClassNotFoundException | NoSuchMethodException | SecurityException | InstantiationException
                 | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            String msg = udfClassName + " load failed since " + e + ".";
            throw new OmniRuntimeException(OmniErrorType.OMNI_JAVA_UDF_ERROR, msg);
        }

        if (retType == DataTypeId.OMNI_VARCHAR || retType == DataTypeId.OMNI_CHAR) {
            // execute hive udf which outputs string
            executeBatchForString(udfClassName, paramTypes, inputValueAddr, inputNullAddr, inputLengthAddr, rowCount,
                    outputValueAddr, outputNullAddr, outputLengthAddr, outputStateAddr, method, udfObj);
        } else {
            // execute hive udf which outputs other
            executeBatchForNonString(udfClassName, paramTypes, retType, inputValueAddr, inputNullAddr, inputLengthAddr,
                    rowCount, outputValueAddr, outputNullAddr, method, udfObj);
        }
    }

    private static void executeBatchForString(String udfClassName, DataTypeId[] paramTypes, long inputValueAddr,
                                              long inputNullAddr, long inputLengthAddr, int rowCount, long outputValueAddr, long outputNullAddr,
                                              long outputLengthAddr, long outputStateAddr, Method method, Object udfObj) {
        int addrArraySize = paramTypes.length;
        long[] inputValueAddrs = UdfUtil.getLongs(inputValueAddr, 0, addrArraySize);
        long[] inputNullAddrs = UdfUtil.getLongs(inputNullAddr, 0, addrArraySize);
        long[] inputLengthAddrs = UdfUtil.getLongs(inputLengthAddr, 0, addrArraySize);

        int outputOffset = 0;
        int outputValueCapacity = UdfUtil.UNSAFE.getInt(outputStateAddr);
        int rowIdx = UdfUtil.UNSAFE.getInt(outputStateAddr + Integer.BYTES);
        for (; rowIdx < rowCount; rowIdx++) {
            try {
                Object[] inputArgs = getInputObjects(paramTypes, inputValueAddrs, inputNullAddrs, inputLengthAddrs,
                        rowIdx);
                Object result = method.invoke(udfObj, inputArgs);
                if (result != null) {
                    // set value
                    byte[] chars = ((String) result).getBytes(StandardCharsets.UTF_8);
                    int length = chars.length;
                    if (outputOffset + length > outputValueCapacity) {
                        UdfUtil.UNSAFE.putInt(outputStateAddr + Integer.BYTES, rowIdx);
                        return;
                    }

                    UdfUtil.putBytes(outputValueAddr, outputOffset, chars);
                    UdfUtil.UNSAFE.putByte(outputNullAddr + rowIdx, (byte) 0);
                    UdfUtil.UNSAFE.putInt(outputLengthAddr + (long) rowIdx * Integer.BYTES, length);
                    outputOffset += length;
                } else {
                    UdfUtil.UNSAFE.putByte(outputNullAddr + rowIdx, (byte) 1);
                    UdfUtil.UNSAFE.putInt(outputLengthAddr + (long) rowIdx * Integer.BYTES, 0);
                }
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                String msg = udfClassName + " invoke failed since " + e.getCause() + ".";
                throw new OmniRuntimeException(OmniErrorType.OMNI_JAVA_UDF_ERROR, msg);
            }
        }
        UdfUtil.UNSAFE.putInt(outputStateAddr + Integer.BYTES, rowCount);
    }

    private static void executeBatchForNonString(String udfClassName, DataTypeId[] paramTypes, DataTypeId retType,
                                                 long inputValueAddr, long inputNullAddr, long inputLengthAddr, int rowCount, long outputValueAddr,
                                                 long outputNullAddr, Method method, Object udfObj) {
        int addrArraySize = paramTypes.length;
        long[] inputValueAddrs = UdfUtil.getLongs(inputValueAddr, 0, addrArraySize);
        long[] inputNullAddrs = UdfUtil.getLongs(inputNullAddr, 0, addrArraySize);
        long[] inputLengthAddrs = UdfUtil.getLongs(inputLengthAddr, 0, addrArraySize);

        for (int rowIdx = 0; rowIdx < rowCount; rowIdx++) {
            try {
                Object[] inputArgs = getInputObjects(paramTypes, inputValueAddrs, inputNullAddrs, inputLengthAddrs,
                        rowIdx);
                Object result = method.invoke(udfObj, inputArgs);
                if (result != null) {
                    // set value
                    setResult(result, retType, outputValueAddr, rowIdx);
                    UdfUtil.UNSAFE.putByte(outputNullAddr + rowIdx, (byte) 0);
                } else {
                    UdfUtil.UNSAFE.putByte(outputNullAddr + rowIdx, (byte) 1);
                }
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                String msg = udfClassName + " invoke failed since " + e.getCause() + ".";
                throw new OmniRuntimeException(OmniErrorType.OMNI_JAVA_UDF_ERROR, msg);
            }
        }
    }

    private static Object[] getInputObjects(DataTypeId[] paramTypes, long[] inputValueAddrs, long[] inputNullAddrs,
                                            long[] inputLengthAddrs, int rowIdx) {
        Object[] inputObjects = new Object[paramTypes.length];
        for (int i = 0; i < paramTypes.length; i++) {
            if (UdfUtil.UNSAFE.getByte(inputNullAddrs[i] + rowIdx) == 1) {
                inputObjects[i] = null;
                continue;
            }

            switch (paramTypes[i]) {
                case OMNI_INT:
                    inputObjects[i] = UdfUtil.UNSAFE.getInt(inputValueAddrs[i] + (long) rowIdx * Integer.BYTES);
                    break;
                case OMNI_LONG:
                    inputObjects[i] = UdfUtil.UNSAFE.getLong(inputValueAddrs[i] + (long) rowIdx * Long.BYTES);
                    break;
                case OMNI_DOUBLE:
                    inputObjects[i] = UdfUtil.UNSAFE.getDouble(inputValueAddrs[i] + (long) rowIdx * Double.BYTES);
                    break;
                case OMNI_BOOLEAN:
                    inputObjects[i] = (UdfUtil.UNSAFE.getByte(inputValueAddrs[i] + rowIdx) == 1);
                    break;
                case OMNI_SHORT:
                    inputObjects[i] = UdfUtil.UNSAFE.getShort(inputValueAddrs[i] + (long) rowIdx * Short.BYTES);
                    break;
                case OMNI_CHAR:
                case OMNI_VARCHAR: {
                    long valueAddr = UdfUtil.UNSAFE.getLong(inputValueAddrs[i] + (long) rowIdx * Long.BYTES);
                    int length = UdfUtil.UNSAFE.getInt(inputLengthAddrs[i] + (long) rowIdx * Integer.BYTES);
                    byte[] chars = UdfUtil.getBytes(valueAddr, 0, length);
                    inputObjects[i] = new String(chars, StandardCharsets.UTF_8);
                    break;
                }
                default: {
                    String msg = paramTypes[i] + " not supported now.";
                    throw new OmniRuntimeException(OmniErrorType.OMNI_JAVA_UDF_ERROR, msg);
                }
            }
        }
        return inputObjects;
    }

    private static void setResult(Object result, DataTypeId retType, long outputValueAddr, int rowIdx) {
        switch (retType) {
            case OMNI_INT: {
                UdfUtil.UNSAFE.putInt(outputValueAddr + (long) rowIdx * Integer.BYTES, (int) result);
                break;
            }
            case OMNI_LONG: {
                UdfUtil.UNSAFE.putLong(outputValueAddr + (long) rowIdx * Long.BYTES, (long) result);
                break;
            }
            case OMNI_DOUBLE: {
                UdfUtil.UNSAFE.putDouble(outputValueAddr + (long) rowIdx * Double.BYTES, (double) result);
                break;
            }
            case OMNI_BOOLEAN: {
                UdfUtil.UNSAFE.putByte(outputValueAddr + rowIdx, (byte) ((boolean) result ? 1 : 0));
                break;
            }
            case OMNI_SHORT: {
                UdfUtil.UNSAFE.putShort(outputValueAddr + (long) rowIdx * Short.BYTES, (Short) result);
                break;
            }
            default: {
                String msg = retType + " not supported now.";
                throw new OmniRuntimeException(OmniErrorType.OMNI_JAVA_UDF_ERROR, msg);
            }
        }
    }

    private static Method findValidMethod(Class<?> udfClass, DataTypeId[] argumentTypes, DataTypeId retType) {
        Method[] methods = udfClass.getMethods();
        for (Method method : methods) {
            if (!method.getName().equals("evaluate")) {
                continue;
            }

            // check return type
            Class<?> returnType = method.getReturnType();
            DataTypeId retDataType = getDataTypeId(returnType);
            if (retDataType != retType) {
                continue;
            }

            // check argument types
            Class<?>[] parameterTypes = method.getParameterTypes();
            if (parameterTypes.length != argumentTypes.length) {
                continue;
            }
            boolean isValid = true;
            for (int i = 0; i < parameterTypes.length; i++) {
                DataTypeId paramDataType = getDataTypeId(parameterTypes[i]);
                if (paramDataType != argumentTypes[i]) {
                    isValid = false;
                    break;
                }
            }
            if (!isValid) {
                continue;
            }

            return method;
        }

        String msg = udfClass.getName() + " does not have valid method.";
        throw new OmniRuntimeException(OmniErrorType.OMNI_JAVA_UDF_ERROR, msg);
    }

    private static DataTypeId getDataTypeId(Class<?> cls) {
        if (cls == int.class || cls == Integer.class) {
            return DataTypeId.OMNI_INT;
        } else if (cls == long.class || cls == Long.class) {
            return DataTypeId.OMNI_LONG;
        } else if (cls == double.class || cls == Double.class) {
            return DataTypeId.OMNI_DOUBLE;
        } else if (cls == boolean.class || cls == Boolean.class) {
            return DataTypeId.OMNI_BOOLEAN;
        } else if (cls == short.class || cls == Short.class) {
            return DataTypeId.OMNI_SHORT;
        } else if (cls == char.class || cls == Character.class) {
            return DataTypeId.OMNI_CHAR;
        } else if (cls == String.class) {
            return DataTypeId.OMNI_VARCHAR;
        } else {
            String msg = "Does not support " + cls.getName() + " now.";
            throw new OmniRuntimeException(OmniErrorType.OMNI_JAVA_UDF_ERROR, msg);
        }
    }
}
