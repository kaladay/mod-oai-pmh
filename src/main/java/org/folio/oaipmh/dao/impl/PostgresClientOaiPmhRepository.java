package org.folio.oaipmh.dao.impl;

import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.folio.oaipmh.Request;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.SQLConnection;
import org.springframework.stereotype.Component;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.core.parsetools.JsonEvent;
import io.vertx.pgclient.PgConnection;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;

@Component("oaiPmhRepository")
public class PostgresClientOaiPmhRepository {

  private static final String INSTANCES_TABLE_NAME = "INSTANCES";
  private static final String INSTANCE_ID_FIELD_NAME = "instanceid";
  public static final String INSTANCE_ID_COLUMN_NAME = "INSTANCE_ID";
  public static final String REQUEST_ID_COLUMN_NAME = "REQUEST_ID";

  public Promise<Void> saveInstancesIds(List<JsonEvent> instances, Request request, String requestId, PostgresClient postgresClient) {
      Promise<Void> promise = Promise.promise();
      postgresClient.getConnection(e -> {
        List<Tuple> batch = new ArrayList<>();
        List<JsonObject> entities = instances.stream().map(JsonEvent::objectValue).collect(toList());

        for (JsonObject jsonObject : entities) {
          String id = jsonObject.getString(INSTANCE_ID_FIELD_NAME);
          batch.add(Tuple.of(UUID.fromString(id), requestId, jsonObject));
        }
        String tenantId = request.getTenant();
        String sql = "INSERT INTO " + PostgresClient.convertToPsqlStandard(tenantId) + "." + INSTANCES_TABLE_NAME + " (instance_id, request_id, json) VALUES ($1, $2, $3) RETURNING instance_id";

        PgConnection connection = e.result();
        connection.preparedQuery(sql).executeBatch(batch, queryRes -> {
          if (queryRes.failed()) {
            promise.fail(queryRes.cause());
          } else {
            promise.complete();
          }
        });
      });
      return promise;
    }

  public Promise<Void> deleteInstanceIds(List<String> instanceIds, String requestId, PostgresClient postgresClient, Handler<AsyncResult<Void>> failureHandler) {
    Promise<Void> promise = Promise.promise();
    String instanceIdsStr = instanceIds.stream().map(e -> "'" + e + "'").collect(Collectors.joining(", "));
    final String sql = String.format("DELETE FROM " + INSTANCES_TABLE_NAME + " WHERE " +
      REQUEST_ID_COLUMN_NAME + " = '%s' AND " + INSTANCE_ID_COLUMN_NAME + " IN (%s)", requestId, instanceIdsStr);
    postgresClient.startTx(conn -> {
      try {
        postgresClient.execute(conn, sql, reply -> {
          if (reply.succeeded()) {
            endTransaction(postgresClient, conn).future().onComplete(o -> promise.complete());
          } else {
            endTransaction(postgresClient, conn).future().onComplete(failureHandler);
          }
        });
      } catch (Exception e) {
        endTransaction(postgresClient, conn).future().onComplete(failureHandler);
      }
    });
    return promise;
  }

  public Promise<Void> endTransaction(PostgresClient postgresClient, AsyncResult<SQLConnection> conn) {
    Promise<Void> promise = Promise.promise();
    try {
      postgresClient.endTx(conn, promise);
    } catch (Exception e) {
      promise.fail(e);
    }
    return promise;
  }

  public Promise<List<JsonObject>> getNextInstances(Request request, int batchSize, PostgresClient postgresClient,
                                                   Handler<AsyncResult<Void>> failureHandler
  ) {
    Promise<List<JsonObject>> promise = Promise.promise();
    final String sql = String.format("SELECT json FROM " + INSTANCES_TABLE_NAME + " WHERE " +
      REQUEST_ID_COLUMN_NAME + " = '%s' ORDER BY " + INSTANCE_ID_COLUMN_NAME + " LIMIT %d", request.getRequestId(), batchSize + 1);
    postgresClient.startTx(conn -> {
      try {
        postgresClient.select(conn, sql, reply -> {
          if (reply.succeeded()) {
            List<JsonObject> list = StreamSupport
              .stream(reply.result().spliterator(), false)
              .map(this::createJsonFromRow).map(e -> e.getJsonObject("json")).collect(toList());
            promise.complete(list);
          } else {
            endTransaction(postgresClient, conn).future().onComplete(failureHandler);
          }
        });
      } catch (Exception e) {
        endTransaction(postgresClient, conn).future().onComplete(failureHandler);
      }
    });
    return promise;
  }

  private JsonObject createJsonFromRow(Row row) {
    JsonObject json = new JsonObject();
    if (row != null) {
      for (int i = 0; i < row.size(); i++) {
        json.put(row.getColumnName(i), convertRowValue(row.getValue(i)));
      }
    }
    return json;
  }

  private Object convertRowValue(Object value) {
    if (value == null) {
      return "";
    }
    return value instanceof JsonObject ? value : value.toString();
  }

}
