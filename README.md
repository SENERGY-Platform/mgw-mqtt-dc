Maps MQTT-Messages to MGW-Messages according to Topic-Descriptions. Topic-Descriptions may be generated or user-defined.

## Config
Config values can be set in config.json or as Environment-Variable. (Example: config.json: "connector_id", Env: "CONNECTOR_ID")
#### connector_id
String. Identifies device connector in MGW.

#### mgw_mqtt_broker
String. Address of the MQTT-Broker of the MGW. Example: tcp://mgw-broker:1883

#### mgw_mqtt_user
String. Username used for MGW MQTT-Broker.

#### mgw_mqtt_pw
String. Password used for MGW MQTT-Broker.

#### mgw_mqtt_client_id
String. Client-Id used for MGW MQTT-Broker.

#### mqtt_broker
String. Address of the mapped MQTT-Broker. Example: tcp://broker:1883

#### mqtt_user
String. Username used for the mapped MQTT-Broker.

#### mqtt_pw
String. Password used for the mapped MQTT-Broker.

#### mqtt_event_client_id
String. Client-Id used for event subscriptions to the mapped MQTT-Broker.

#### mqtt_cmd_client_id
String. Client-Id used for response subscriptions and command publications to the mapped MQTT-Broker.

#### debug
Boolean.

#### update_period
String. Duration. Interval between updates of Device-Informations. 

#### device_descriptions_dir
String. Directory. Location of Topic-Descriptions.

#### delete_devices
Boolean. Decides if removed devices should be deleted or markt as offline.

#### max_correlation_id_age
String. Duration.

#### generator_use
Boolean. Decides if Topic-Descriptions should be generated.

#### generator_auth_username

#### generator_auth_password

#### generator_filter_devices_by_attribute
String. Used to filter platform devices which should be used to generate topic-descriptions. Must be used if user has more than one MGW or mgw-mqtt-dc.
If set, only devices with matching `senergy/local-mqtt` attribute are used.

#### generator_auth_endpoint
#### generator_auth_client_id
#### generator_auth_client_secret
#### generator_permission_search_url
#### generator_device_repository_url
#### generator_device_descriptions_dir
String. Directory. Location where Topic-Descriptions should be generated.

#### generator_truncate_device_prefix
String. The local id of a device must be globally unique but the user might want to keep the device-topic simple. To enable this the user may truncate a prefix when generating topics.

Example:
```
local device id: "foo:d1"
generator_truncate_device_prefix: "foo:"
topic-template: "{{.Device}}/bar"
generated topic: "d1/bar"
``` 

## Topic-Descriptions
Topic-Descriptions are used to describe how to map between the two mqtt brokers. They may be defined as json, yaml or csv. The user may define multiple files in multiple subdirectories. Examples can be found in `pkg/topicdescription/testdata/topicdesc`.

### Topic-Description Fields
- event_topic: may not be used in the same description as cmd_topic
- cmd_topic: may not be used in the same description as event_topic
- resp_topic: must be used in the same description as a cmd_topic
- device_type_id
- device_local_id
- service_local_id
- device_name

## Topic-Description Generator
Topic-Descriptions may be generated from devices on the Senergy-Platform.

### Device-Type
If a device type is to be used for generated topic-descriptions it has to use the attribute `senergy/local-mqtt` with any value. And its services define attributes used to generate topic-descriptions.

#### Service-Attributes
- `senergy/local-mqtt/event-topic-tmpl`: template used to generate a event topic description.
- `senergy/local-mqtt/cmd-topic-tmpl`: template used to generate a command topic description.
- `senergy/local-mqtt/resp-topic-tmpl`: template used to generate a response topic description for a matching command.

The attributes define template to generate topics. Placeholders for these templates are:
- `{{.Device}}` local device id (may be truncated by `generator_truncate_device_prefix`)
- `{{.LocalDeviceId}}` same as `{{.Device}}`
- `{{.Service}}`
- `{{.LocalServiceId}}` same as `{{.Service}}`

### Device

#### Device-Local-Id 
Please reference the documentation to the config value `generator_truncate_device_prefix`

#### Attributes
`senergy/local-mqtt`: optional, in combination with the config field `generator_filter_devices_by_attribute` 

### Warning
Removed platform devices may be recreated by the mgw if the mgw-mqtt-dc is unable to request updates from the platform.

## Docker-Compose Example

```yaml
version: "3"
services:
  test-mgw-broker:
    image: eclipse-mosquitto:1.6.12
  mgw-senergy-connector:
    container_name: mgw-senergy-connector
    image: ghcr.io/senergy-platform/mgw-senergy-connector:dev
    environment:
      CONF_MB_HOST: test-mgw-broker
      CONF_MB_PORT: 1883
      CONF_DM_URL: http://mgw-device-manager
      CONF_DM_API: devices
      CONF_MQTTCLIENT_QOS: 2
      CONF_LOGGER_LEVEL: debug
      CC_LIB_CONNECTOR_LOW_LEVEL_LOGGER: "True"
      CONF_DSROUTER_MAX_COMMAND_AGE: 60
      CONF_HUB_NAME: test-hub
      CC_LIB_CONNECTOR_HOST: fgseitsrancher.wifa.intern.uni-leipzig.de
      CC_LIB_CONNECTOR_PORT: 2883
      CC_LIB_CONNECTOR_TLS: "False"
      CC_LIB_CONNECTOR_QOS: 2
      CC_LIB_API_HUB_ENDPT: https://fgseitsrancher.wifa.intern.uni-leipzig.de:8000/device-manager/hubs
      CC_LIB_API_DEVICE_ENDPT: https://fgseitsrancher.wifa.intern.uni-leipzig.de:8000/device-manager/local-devices
      CC_LIB_API_AUTH_ENDPT: https://fgseitsrancher.wifa.intern.uni-leipzig.de:8087/auth/realms/master/protocol/openid-connect/token
      CC_LIB_CREDENTIALS_USER: "*****"
      CC_LIB_CREDENTIALS_PW: "*****"
      CC_LIB_CREDENTIALS_CLIENT_ID: client-connector-lib
      CC_LIB_CONNECTOR_CLEAN_SESSION: "True"
    depends_on:
      - test-mgw-broker
      - mgw-device-manager
    restart: unless-stopped
  mgw-device-manager:
    container_name: mgw-device-manager
    image: ghcr.io/senergy-platform/mgw-device-manager:dev
    ports:
      - "7002:80"
    environment:
      - CONF_MB_HOST=test-mgw-broker
      - CONF_MB_PORT=1883
    depends_on:
      - test-mgw-broker
    restart: unless-stopped
  mgw-mqtt-dc:
    container_name: mgw-mqtt-dc
    image: ghcr.io/senergy-platform/mgw-mqtt-dc:dev
    environment:
      - MGW_MQTT_BROKER=tcp://test-mgw-broker:1883
      - MGW_MQTT_CLIENT_ID=mgw-mqtt-dc
      - MQTT_BROKER=tcp://test-mgw-broker:1883
      - MQTT_CMD_CLIENT_ID=mgw-mqtt-dc-cmd
      - MQTT_EVENT_CLIENT_ID=mgw-mqtt-dc-event
      - GENERATOR_USE=true
      - GENERATOR_AUTH_USERNAME=*****
      - GENERATOR_AUTH_PASSWORD=*****
      - GENERATOR_AUTH_ENDPOINT=https://fgseitsrancher.wifa.intern.uni-leipzig.de:8087
      - GENERATOR_PERMISSION_SEARCH_URL=https://fgseitsrancher.wifa.intern.uni-leipzig.de:8000/permissions/query
      - GENERATOR_DEVICE_REPOSITORY_URL=https://fgseitsrancher.wifa.intern.uni-leipzig.de:8000/device-repository
      - GENERATOR_TRUNCATE_DEVICE_PREFIX=testmgwprefix_
    depends_on:
      - mgw-device-manager
    restart: unless-stopped
```

## DeviceType Example
```json
{
   "id":"urn:infai:ses:device-type:7c162b2c-56fa-4ca2-a9cd-936da7c8b1a9",
   "name":"mgw-mqtt-dc-test",
   "description":"test dc for mgw-mqtt-dc",
   "services":[
      {
         "id":"urn:infai:ses:service:ba1e8a32-61bf-4fe8-a129-4e8d4b220e00",
         "local_id":"brightness",
         "name":"brightness",
         "description":"",
         "interaction":"request",
         "aspect_ids":[
            "urn:infai:ses:aspect:a7470d73-dde3-41fc-92bd-f16bb28f2da6"
         ],
         "protocol_id":"urn:infai:ses:protocol:f3a63aeb-187e-4dd9-9ef5-d97a6eb6292b",
         "inputs":[
            {
               "id":"urn:infai:ses:content:f438a16d-cc94-472c-acd4-d1fb071c0314",
               "content_variable":{
                  "id":"urn:infai:ses:content-variable:ffa689f5-d15a-4d5e-9956-89d79ee0a39c",
                  "name":"brightness",
                  "type":"https://schema.org/Integer",
                  "sub_content_variables":null,
                  "characteristic_id":"urn:infai:ses:characteristic:72b624b5-6edc-4ec4-9ad9-fa00b39915c0",
                  "value":null,
                  "serialization_options":null
               },
               "serialization":"json",
               "protocol_segment_id":"urn:infai:ses:protocol-segment:0d211842-cef8-41ec-ab6b-9dbc31bc3a65"
            }
         ],
         "outputs":[
            
         ],
         "function_ids":[
            "urn:infai:ses:controlling-function:6ce74d6d-7acb-40b7-aac7-49daca214e85"
         ],
         "attributes":[
            {
               "key":"senergy/local-mqtt/cmd-topic-tmpl",
               "value":"{{.Device}}/{{.Service}}/set",
               "origin":"web-ui"
            }
         ],
         "rdf_type":""
      },
      {
         "id":"urn:infai:ses:service:be6e0d93-246b-4d96-adca-6f9f8617f723",
         "local_id":"battery",
         "name":"event",
         "description":"",
         "interaction":"event",
         "aspect_ids":[
            "urn:infai:ses:aspect:f0bd89b6-4f60-49a9-9489-ea215f2ec3d2"
         ],
         "protocol_id":"urn:infai:ses:protocol:f3a63aeb-187e-4dd9-9ef5-d97a6eb6292b",
         "inputs":[
            
         ],
         "outputs":[
            {
               "id":"urn:infai:ses:content:707ec1c9-02c5-4459-84b7-93aceff90505",
               "content_variable":{
                  "id":"urn:infai:ses:content-variable:39b22f49-4f5f-44b9-83ef-0a74871572bd",
                  "name":"battery",
                  "type":"https://schema.org/Float",
                  "sub_content_variables":null,
                  "characteristic_id":"urn:infai:ses:characteristic:5caa707d-dc08-4f3b-bd9f-f08935c8dd3c",
                  "value":null,
                  "serialization_options":null
               },
               "serialization":"json",
               "protocol_segment_id":"urn:infai:ses:protocol-segment:0d211842-cef8-41ec-ab6b-9dbc31bc3a65"
            }
         ],
         "function_ids":[
            "urn:infai:ses:measuring-function:00549f18-88b5-44c7-adb1-f558e8d53d1d"
         ],
         "attributes":[
            {
               "key":"senergy/local-mqtt/event-topic-tmpl",
               "value":"{{.Device}}/{{.Service}}",
               "origin":"web-ui"
            }
         ],
         "rdf_type":""
      }
   ],
   "device_class_id":"urn:infai:ses:device-class:14e56881-16f9-4120-bb41-270a43070c86",
   "attributes":[
      {
         "key":"senergy/local-mqtt",
         "value":"true",
         "origin":"web-ui"
      }
   ],
   "rdf_type":""
}
```

## Device Example
```json
{
   "id":"urn:infai:ses:device:51a2a119-bf99-4951-8fc3-c0c7d9b20629",
   "local_id":"testmgwprefix_d1",
   "name":"test-mqtt-dc-1",
   "attributes":null,
   "device_type_id":"urn:infai:ses:device-type:7c162b2c-56fa-4ca2-a9cd-936da7c8b1a9"
}
```