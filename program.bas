Rem --- eWON start section: Init Section
eWON_init_section:
Rem --- eWON user (start)
Onwan '@WANAction(GETSYS PRG, "EVTINFO")'
Function WANAction($WANStatus%)
  IF $WANStatus% = 1 Then
  PRINT "WAN up"
  @Start()
  ENDIF
ENDFN
Function Start()
  PRINT "starting MQTT"
  @MosquittoInit()
  PRINT "Initialized."
  ONMQTTStatus "@MosquittoMQTTStatusChange(mqtt('status'))"
ENDFN
Function MosquittoInit()
  MQTT "open", mqtt_client$, mqtt_host$
  MQTT "setparam", "CAFile", ca_file_path$
  MQTT "setparam", "ProtocolVersion", "3.1.1"
  MQTT "setparam", "keepalive", "60"
  MQTT "setparam", "port", mqtt_port$
  MQTT "setparam", "username", username$
  MQTT "setparam", "password", password$
  MQTT "setparam", "WillPayload", "OFFLINE"
  MQTT "setparam", "WillTopic", mqtt_state_topic$
  MQTT "setparam", "WillQoS", "0"
  MQTT "setparam", "WillRetain", "1"
  MQTT "connect"
  ONMQTT '@readMsg(mqtt("read"))'
ENDFN
Function readMsg($msgID%)
  PRINT "READING MESSAGE"
  IF $msgID% > 0 Then
    msgTopic$ = MQTT "msgtopic"
    msgData$ = MQTT "msgdata"
    IF msgTopic$ = mqtt_base_topic$ + "CMD" AND msgData$ = "REBIRTH" THEN
      PRINT "REBIRTH COMMAND"
      @PublishAllTags()
    ENDIF
  ENDIF
ENDFN
Function MosquittoMQTTStatusChange($status%)
  IF $status% = 5 Then
    PRINT "MQTT connected"
    MQTT "subscribe", mqtt_base_topic$ + "CMD", 0
    MQTT "publish", mqtt_state_topic$, "ONLINE", 0, 1
    //@PublishAllTags()
    TSET 1, poll_seconds%
    ONTIMER 1, "@RBE()"
  ELSE
    PRINT "MQTT disconnected"
    TSET 1, 0
  ENDIF
ENDFN
FUNCTION RBE()
  $payload$ = '{"t":'
  $read_timestamp% = GETSYS PRG, "TIMESEC"
  @ReadRoundTags()
  $timestamp_str$ = STR$($read_timestamp%)
  $payload$ = $payload$ + $timestamp_str$ + ', "m":'
  
  $metrics$ = "["
  
  FOR $n% = 1 To no_tags%
    IF NOT tags_values(1, $n%) = tags_values_prev(1, $n%) THEN
      IF $metrics$(LEN($metrics$) TO) = '}' THEN
        $metrics$ = $metrics$ + ','
      ENDIF
      $metrics$ = $metrics$ + '{"a":"' + RTRIM tags_ids$(1, $n%) + '","v":"' + STR$(tags_values(1, $n%)) + '"}'
    ENDIF
  NEXT $n%
  
  $metrics$ = $metrics$ + "]"
  
  IF NOT $metrics$ = "[]" THEN
    //PRINT $metrics$
    $payload$ = $payload$ + $metrics$ + '}'
    //PRINT $payload$
    MQTT "publish", mqtt_base_topic$ + 'DATA', $payload$, 0, 0
  ENDIF
ENDFN
FUNCTION PublishAllTags()
  $payload$ = '{"t":'
  $read_timestamp% = GETSYS PRG, "TIMESEC"
  @ReadTags()
  $timestamp_str$ = STR$($read_timestamp%)
  $payload$ = $payload$ + $timestamp_str$ + ', "m":'
  
  $metrics$ = "["
  FOR $n% = 1 To no_tags%
    $metrics$ = $metrics$ + '{"n":"' + RTRIM tags_names$(1, $n%) + '","a":"' + RTRIM tags_ids$(1, $n%) + '","t":"' + RTRIM tags_types$(1, $n%) + '","v":"' + STR$(tags_values(1, $n%)) + '"}'
    IF NOT $n% = no_tags% THEN
      $metrics$ = $metrics$ + ","
    ENDIF
  NEXT $n%
  $metrics$ = $metrics$ + "]"
  //PRINT $metrics$
  $payload$ = $payload$ + $metrics$ + '}'
  //PRINT $payload$
  MQTT "publish", mqtt_base_topic$ + 'BIRTH', $payload$, 0, 0
ENDFN

FUNCTION ReadTags()
  FOR $n% = 1 To no_tags%
    tags_values_prev(1, $n%) = tags_values(1, $n%)
    tags_values(1, $n%) = GETIO RTRIM tags_names$(1, $n%)
  NEXT $n%
ENDFN
FUNCTION ReadRoundTags()
  FOR $n% = 1 To no_tags%
    IF RTRIM tags_types$(1, $n%) = 'float' THEN
      $prev_val = tags_values(1, $n%)
      $new_val = GETIO RTRIM tags_names$(1, $n%)
      $value_truncated = INT($new_val * 100) / 100
      $min_change = 0.01
      IF $value_truncated > 1000.0 THEN
        $min_change = 5.0
      ELSE
        IF $value_truncated > 100.0 THEN
          $min_change = 0.5
        ENDIF
      ENDIF
      
      tags_values_prev(1, $n%) = $prev_val
      IF $prev_val - $min_change < $value_truncated AND $value_truncated < $prev_val + $min_change THEN
        // Value is within filter range, do not change value
        //PRINT "VALUE " + RTRIM tags_names$(1, $n%) + " UNCHANGED " + STR$($value_truncated - $prev_val)
        tags_values(1, $n%) = $prev_val
      ELSE
        //PRINT "VALUE " + RTRIM tags_names$(1, $n%) + " CHANGED " + STR$($value_truncated - $prev_val)
        tags_values(1, $n%) = $value_truncated
      ENDIF
      
      //PRINT RTRIM tags_names$(1, $n%)
      //PRINT STR$($value_truncated) + " | " + STR$($min_change)
    ELSE
      tags_values_prev(1, $n%) = tags_values(1, $n%)
      tags_values(1, $n%) = GETIO RTRIM tags_names$(1, $n%)
    ENDIF
  NEXT $n%
ENDFN
// Program defined Variables
no_tags% = GETSYS PRG, "NBTAGS"
flexy_serial$ = GETSYS PRG, "SERNUM"
no_tags% = GETSYS PRG, "NBTAGS"
DIM tags_values_prev(1, no_tags%)
DIM tags_values(1, no_tags%)
DIM tags_names$(1, no_tags%, 60)
DIM tags_types$(1, no_tags%, 10)
DIM tags_ids$(1, no_tags%, 5)
FOR x% = 0 To no_tags% -1
  i% = x% * -1
  SETSYS TAG, "load", i%
  tag_name$ = GETSYS TAG, "Name"
  tags_ids$(1, x%+1) = GETSYS TAG, "ID"
  tags_types$(1, x%+1) = TYPE$(GETIO RTRIM tag_name$)
  tags_names$(1,x%+1) = RTRIM tag_name$
NEXT x%
// User defined Variables
mqtt_namespace$ = "flexy_v1.0"
mqtt_host$ = "mqtt.iono2x.com"
mqtt_port$ = "8873"
username$ = "ewon_flexy_" + flexy_serial$
password$ = "1zVxXdyviFyjurUk4pGW"
ca_file_path$ = "/usr/certs/mqtt.iono2x.com.crt"
PRINT no_tags%
group_id$ = "kerry_foods"
node_id$ = "iono2x_2021-214"
device_id$ = username$

mqtt_base_topic$ = mqtt_namespace$ + "/" + group_id$ + "/" + node_id$ + "/" + device_id$ + "/"
mqtt_client$ = group_id$ + "_" + node_id$ + "_" + device_id$
mqtt_state_topic$ = mqtt_base_topic$ + "STATE"
poll_seconds% = 20

@Start()
Rem --- eWON user (end)
End
Rem --- eWON end section: Init Section