/*
 * Copyright 2013 Red Hat, Inc.
 *
 * Red Hat licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 *
 */
package com.mycompany.myproject;


import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.platform.Verticle;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;


public class PingVerticle extends Verticle {

    public static final String ADDRESS = "default-address";
    public static final String DEFAULT_TOPIC = "default-topic";
    public static final String DEFAULT_PARTITION = "default-partition";
    public static final String DEFAULT_BROKER_LIST = "localhost:9092";
    public static final int DEFAULT_REQUEST_ACKS = 1;
    public static final String DEFAULT_KEY_SERIALIZER_CLASS = "kafka.serializer.StringEncoder";

    public static final String PAYLOAD = "payload";
    public static final String MESSAGE = "Test message from KafkaModuleDeployWithCorrectConfigIT!";


    public void start() {
      final Logger logger = container.logger();

      logger.info("PingVerticle starting ... ");

      JsonObject config = new JsonObject();
      config.putString("address", ADDRESS);
      config.putString("metadata.broker.list", DEFAULT_BROKER_LIST);
      config.putString("kafka-topic", DEFAULT_TOPIC);
      config.putString("kafka-partition", DEFAULT_PARTITION);
      config.putNumber("request.required.acks", DEFAULT_REQUEST_ACKS);
      config.putString("serializer.class", DEFAULT_KEY_SERIALIZER_CLASS);

      container.deployModule("com.zanox.vertx~mod-kafka~1.1.2-SNAPSHOT", config);

      Handler<Message<JsonObject>> replyHandler = new Handler<Message<JsonObject>>() {
          public void handle(Message<JsonObject> message) {
              logger.info("error " + message.body().getString("status"));
              logger.info(message.body().getString("message"));
          }
      };

      JsonObject jsonObject = new JsonObject();
      jsonObject.putString("payload", "your message goes here");
      vertx.eventBus().send(ADDRESS, jsonObject, replyHandler);

      logger.info("PingVerticle started");
  }
}
