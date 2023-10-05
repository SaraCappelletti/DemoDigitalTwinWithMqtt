package demo;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import it.wldt.adapter.mqtt.digital.MqttDigitalAdapter;
import it.wldt.adapter.mqtt.digital.MqttDigitalAdapterConfiguration;
import it.wldt.adapter.mqtt.digital.exception.MqttDigitalAdapterConfigurationException;
import it.wldt.adapter.mqtt.digital.topic.MqttQosLevel;
import it.wldt.adapter.mqtt.physical.MqttPhysicalAdapter;
import it.wldt.adapter.mqtt.physical.MqttPhysicalAdapterConfiguration;
import it.wldt.adapter.mqtt.physical.exception.MqttPhysicalAdapterConfigurationException;
import it.wldt.core.engine.WldtEngine;
import it.wldt.exception.EventBusException;
import it.wldt.exception.ModelException;
import it.wldt.exception.WldtConfigurationException;
import it.wldt.exception.WldtRuntimeException;
import org.eclipse.paho.client.mqttv3.MqttException;

import java.util.function.Function;

public class Main {
    public static void main(String[] args) throws WldtConfigurationException, ModelException, WldtRuntimeException, EventBusException, MqttPhysicalAdapterConfigurationException, MqttException, MqttDigitalAdapterConfigurationException  {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "error");

        WldtEngine dtEngine = new WldtEngine(new DefaultShadowingFunction(), "mqtt-device-digital-twin");
        dtEngine.addPhysicalAdapter(buildMqttPhysicalAdapter());
        dtEngine.addDigitalAdapter(buildMqttDigitalAdapter());

        dtEngine.startLifeCycle();
    }



    private static MqttPhysicalAdapter buildMqttPhysicalAdapter() throws MqttPhysicalAdapterConfigurationException, MqttException {
        MqttPhysicalAdapterConfiguration configuration = MqttPhysicalAdapterConfiguration
                .builder("localhost", 1883, "mqtt.physical.adapter")
                .addPhysicalAssetPropertyAndTopic("switch", "OFF", "device/switch", msg -> {
                    JsonObject obj = new Gson().fromJson(msg, JsonObject.class);
                    return obj.getAsJsonPrimitive("value").getAsString();
                }).addPhysicalAssetEventAndTopic("overheating", "device.event", "device/overheating", msg -> {
                    JsonObject obj = new Gson().fromJson(msg, JsonObject.class);
                    return obj.getAsJsonPrimitive("value").getAsString();
                }).addPhysicalAssetActionAndTopic("switch", "device.actuation", "application/json", "device/actions/switch", actionBody -> {
                    JsonObject obj = new JsonObject();
                    obj.addProperty("value", (String) actionBody);
                    return new Gson().toJson(obj);
                }).build();
        return new MqttPhysicalAdapter("mqtt-device-pa", configuration);
    }

    private static MqttDigitalAdapter buildMqttDigitalAdapter() throws MqttException, MqttDigitalAdapterConfigurationException {
        //NB: In MqttDigitalAdapter topics for Property and Event are OutgoingTopics,
        // so each function takes as input something that has the same type as the body of the Event or Property
        MqttDigitalAdapterConfiguration configuration = MqttDigitalAdapterConfiguration
                .builder("localhost", 1883, "mqtt.digital.adapter")
                .addEventNotificationTopic("overheating", "dt/overheating", MqttQosLevel.MQTT_QOS_0, Function.identity())
                .addPropertyTopic("switch", "dt/switch", MqttQosLevel.MQTT_QOS_0, Function.identity())
                .addActionTopic("switch", "dt/action/switch", Function.identity())
                .build();
        return new MqttDigitalAdapter("test-mqtt-da", configuration);
    }
}