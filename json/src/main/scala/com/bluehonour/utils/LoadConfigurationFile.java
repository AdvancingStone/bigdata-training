package com.bluehonour.utils;

import com.bluehonour.constant.JsonConstant;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class LoadConfigurationFile {

    private static final HashMap<String, Object> map;

    static {
        Yaml yaml = new Yaml();
        InputStream stream = LoadConfigurationFile.class.getClassLoader().getResourceAsStream(JsonConstant.JSON_YML_PATH());
        map = yaml.load(stream);
    }

    public static Map<String, Object> loadJsonYaml() {
        return map;
    }
}
