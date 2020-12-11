package org.folio.oaipmh.helpers;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;
import static javax.ws.rs.core.HttpHeaders.ACCEPT;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.folio.oaipmh.Constants.*;
import static org.folio.oaipmh.Constants.INVALID_IDENTIFIER_ERROR_MESSAGE;
import static org.folio.oaipmh.Constants.REPOSITORY_MAX_RECORDS_PER_RESPONSE;
import static org.folio.oaipmh.Constants.OKAPI_TENANT;
import static org.folio.oaipmh.Constants.OKAPI_TOKEN;
import static org.folio.oaipmh.Constants.RECORD_METADATA_PREFIX_PARAM_ERROR;
import static org.folio.oaipmh.Constants.RECORD_NOT_FOUND_ERROR;
import static org.folio.oaipmh.Constants.REPOSITORY_SUPPRESSED_RECORDS_PROCESSING;
import static org.folio.oaipmh.helpers.RepositoryConfigurationUtil.getBooleanProperty;
import static org.folio.oaipmh.helpers.RepositoryConfigurationUtil.isDeletedRecordsEnabled;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_ARGUMENT;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.CANNOT_DISSEMINATE_FORMAT;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.ID_DOES_NOT_EXIST;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.apache.commons.lang.StringUtils;
import org.folio.oaipmh.MetadataPrefix;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.dao.PostgresClientFactory;
import org.folio.oaipmh.helpers.records.RecordMetadataManager;
import org.folio.oaipmh.helpers.streaming.BatchStreamWrapper;
import org.folio.rest.client.SourceStorageSourceRecordsClient;
import org.folio.rest.tools.utils.TenantTool;
import org.openarchives.oai._2.GetRecordType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.RecordType;
import org.openarchives.oai._2.ResumptionTokenType;
import org.openarchives.oai._2.StatusType;
import org.springframework.util.ReflectionUtils;
import javax.ws.rs.core.Response;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.parsetools.JsonEvent;
import io.vertx.core.parsetools.JsonParser;
import io.vertx.core.parsetools.impl.JsonParserImpl;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.impl.Connection;

public class GetOaiRecordWithHoldingsHelper extends GetOaiRecordHelper {

  private static final int DATABASE_FETCHING_CHUNK_SIZE = 50;

  private static final String INVENTORY_INSTANCES_ENDPOINT = "/oai-pmh-view/enrichedInstances";

  private static final String ERROR_FROM_STORAGE = "Got error response from %s, uri: '%s' message: %s";

  private static final String SKIP_SUPPRESSED_FROM_DISCOVERY_RECORDS = "skipSuppressedFromDiscoveryRecords";

  private static final String INSTANCE_IDS_ENRICH_PARAM_NAME = "instanceIds";

  private static final String INSTANCE_ID_FIELD_NAME = "instanceid";

  public static final GetOaiRecordWithHoldingsHelper INSTANCE = new GetOaiRecordWithHoldingsHelper();

  public static GetOaiRecordWithHoldingsHelper getInstance() {
    return INSTANCE;
  }

  @Override
  public Future<Response> handle(Request request, Context ctx) {
    Promise<Response> promise = Promise.promise();

    try {
      List<OAIPMHerrorType> errors = validateRequest(request);
      if (!errors.isEmpty()) {
        return buildResponseWithErrors(request, promise, errors);
      }

      String instanceId = request.getIdentifier().replace(request.getIdentifierPrefix(), "");

      JsonObject pseudoInstance = new JsonObject();
      pseudoInstance.put(INSTANCE_ID_FIELD_NAME, instanceId);
      List<JsonObject> jsonInstances = new ArrayList<>();
      jsonInstances.add(pseudoInstance);

      enrichInstances(jsonInstances, request, ctx).onComplete(r -> {

        if (r.failed()) {
          super.handle(request, ctx).onComplete(r1 -> {
            promise.handle(r1);
          });
          return;
        }

        final JsonObject instanceWithHolding = r.result().get(0);

        System.out.print("\n\n\nDEBUG: instanceWithHoldings = " + instanceWithHolding.encodePrettily() + "\n\n\n");

        final SourceStorageSourceRecordsClient srsClient = new SourceStorageSourceRecordsClient(request.getOkapiUrl(),
            request.getTenant(), request.getOkapiToken());

        final boolean deletedRecordsSupport = RepositoryConfigurationUtil.isDeletedRecordsEnabled(request);
        final boolean suppressedRecordsSupport = getBooleanProperty(request.getOkapiHeaders(),
            REPOSITORY_SUPPRESSED_RECORDS_PROCESSING);

        final Date updatedAfter = request.getFrom() == null ? null
            : convertStringToDate(request.getFrom(), false, true);
        final Date updatedBefore = request.getUntil() == null ? null
            : convertStringToDate(request.getUntil(), true, true);

        int batchSize = Integer.parseInt(
            RepositoryConfigurationUtil.getProperty(request.getTenant(), REPOSITORY_MAX_RECORDS_PER_RESPONSE));

        try {
          srsClient.getSourceStorageSourceRecords(null, null,
              request.getIdentifier() != null ? request.getStorageIdentifier() : null, "MARC",
              // 1. NULL if we want suppressed and not suppressed, TRUE = ONLY SUPPRESSED FALSE = ONLY NOT SUPPRESSED
              // 2. use suppressed from discovery filtering only when deleted record support is enabled
              deletedRecordsSupport ? null : suppressedRecordsSupport, deletedRecordsSupport, null, updatedAfter,
              updatedBefore, null, request.getOffset(), batchSize + 1, response -> {
                try {
                  if (org.folio.rest.tools.client.Response.isSuccess(response.statusCode())) {
                    response.bodyHandler(bh -> {

                      RecordMetadataManager metadataManager = RecordMetadataManager.getInstance();

                      final JsonObject srsInstanceCollection = bh.toJsonObject();
                      final Integer totalRecords = srsInstanceCollection.getInteger(TOTAL_RECORDS_PARAM);

                      System.out.print("\n\n\nDEBUG: source record = " + srsInstanceCollection.encodePrettily() + "\n\n\n");

                      final JsonArray sourceRecords = srsInstanceCollection.getJsonArray(SOURCE_RECORDS);

                      final JsonObject srsInstance = sourceRecords.getJsonObject(0);

                      final JsonObject updatedSrsInstance = metadataManager
                        .populateMetadataWithItemsData(srsInstance, instanceWithHolding, suppressedRecordsSupport);

                      System.out.print("\n\n\nDEBUG: updated srs instance = " + updatedSrsInstance.encodePrettily() + "\n\n\n");

                      final JsonArray enhancedSourceRecords = new JsonArray();
                      enhancedSourceRecords.add(updatedSrsInstance);

                      final JsonObject srsEnhancedInstanceCollection = new JsonObject();
                      srsEnhancedInstanceCollection.put(SOURCE_RECORDS, enhancedSourceRecords);
                      srsEnhancedInstanceCollection.put(TOTAL_RECORDS_PARAM, totalRecords);

                      promise.complete(buildRecordsResponse(ctx, request, srsEnhancedInstanceCollection));
                    });
                  } else {
                    logger.error("ListRecords response from SRS status code: {}: {}", response.statusMessage(), response.statusCode());
                    throw new IllegalStateException(response.statusMessage());
                  }
                } catch (Exception e) {
                  logger.error("Exception getting ListRecords", e);
                  promise.fail(e);
                }
              });
        } catch (UnsupportedEncodingException e) {
          e.printStackTrace();
        }

      });

    } catch (Exception e) {
      handleException(promise, e);
    }
    return promise.future();
  }

  @Override
  protected List<OAIPMHerrorType> validateRequest(Request request) {
    List<OAIPMHerrorType> errors = new ArrayList<>();
    if (!validateIdentifier(request)) {
      errors.add(new OAIPMHerrorType().withCode(BAD_ARGUMENT).withValue
        (INVALID_IDENTIFIER_ERROR_MESSAGE));
    }
    if (request.getMetadataPrefix() != null) {
      if (!MetadataPrefix.getAllMetadataFormats().contains(request.getMetadataPrefix())) {
        errors.add(new OAIPMHerrorType().withCode(CANNOT_DISSEMINATE_FORMAT)
          .withValue(CANNOT_DISSEMINATE_FORMAT_ERROR));
      }
    } else {
      errors.add(new OAIPMHerrorType().withCode(BAD_ARGUMENT).withValue
        (RECORD_METADATA_PREFIX_PARAM_ERROR));
    }
    return errors;
  }

  @Override
  protected void addRecordsToOaiResponse(OAIPMH oaipmh, Collection<RecordType> records) {

    if (!records.isEmpty()) {
      oaipmh.withGetRecord(new GetRecordType().withRecord(records.iterator().next()));
    } else {
      oaipmh.withErrors(createNoRecordsFoundError());
    }
  }

  @Override
  protected void addResumptionTokenToOaiResponse(OAIPMH oaipmh, ResumptionTokenType resumptionToken) {
    if (resumptionToken != null) {
      throw new UnsupportedOperationException("Control flow is not applicable for GetRecord verb.");
    }
  }

  @Override
  protected OAIPMHerrorType createNoRecordsFoundError() {
    return new OAIPMHerrorType().withCode(ID_DOES_NOT_EXIST).withValue(RECORD_NOT_FOUND_ERROR);
  }

  private Future<List<JsonObject>> enrichInstances(List<JsonObject> result, Request request, Context context) {
    Map<String, JsonObject> instances = result.stream().collect(toMap(e -> e.getString(INSTANCE_ID_FIELD_NAME), Function.identity()));
    Promise<List<JsonObject>> completePromise = Promise.promise();
    HttpClient httpClient = context.owner().createHttpClient();

    HttpClientRequest enrichInventoryClientRequest = createEnrichInventoryClientRequest(httpClient, request);
    BatchStreamWrapper databaseWriteStream = getBatchHttpStream(httpClient, completePromise, enrichInventoryClientRequest, context);
    JsonObject entries = new JsonObject();
    entries.put(INSTANCE_IDS_ENRICH_PARAM_NAME, new JsonArray(new ArrayList<>(instances.keySet())));
    entries.put(SKIP_SUPPRESSED_FROM_DISCOVERY_RECORDS, isSkipSuppressed(request));
    enrichInventoryClientRequest.end(entries.encode());

    AtomicReference<ArrayDeque<Promise<Connection>>> queue = new AtomicReference<>();
    try {
      queue.set(getWaitersQueue(context.owner(), request));
    } catch (IllegalStateException ex) {
      logger.error(ex.getMessage());
      completePromise.fail(ex);
      return completePromise.future();
    }

    databaseWriteStream.setCapacityChecker(() -> queue.get().size() > 20);

    databaseWriteStream.handleBatch(batch -> {
      try {
        for (JsonEvent jsonEvent : batch) {
          JsonObject value = jsonEvent.objectValue();
          String instanceId = value.getString(INSTANCE_ID_FIELD_NAME);
          Object itemsandholdingsfields = value.getValue(RecordMetadataManager.ITEMS_AND_HOLDINGS_FIELDS);
          if (itemsandholdingsfields instanceof JsonObject) {
            JsonObject instance = instances.get(instanceId);
            if (instance != null) {
              enrichDiscoverySuppressed((JsonObject) itemsandholdingsfields, instance);
              instance.put(RecordMetadataManager.ITEMS_AND_HOLDINGS_FIELDS,
                itemsandholdingsfields);
            } else { // it can be the case only for testing
              logger.info(format("Instance with instanceId %s wasn't in the request", instanceId));
            }
          }
        }

        if (databaseWriteStream.isTheLastBatch() && !completePromise.future().isComplete()) {
          completePromise.complete(new ArrayList<>(instances.values()));
        }
      } catch (Exception e) {
        completePromise.fail(e);
      }
    });

    return completePromise.future();
  }

  private HttpClientRequest createEnrichInventoryClientRequest(HttpClient httpClient, Request request) {
    final HttpClientRequest httpClientRequest = httpClient
      .postAbs(format("%s%s", request.getOkapiUrl(), INVENTORY_INSTANCES_ENDPOINT));

    httpClientRequest.putHeader(OKAPI_TOKEN, request.getOkapiToken());
    httpClientRequest.putHeader(OKAPI_TENANT, TenantTool.tenantId(request.getOkapiHeaders()));
    httpClientRequest.putHeader(ACCEPT, APPLICATION_JSON);
    httpClientRequest.putHeader(CONTENT_TYPE, APPLICATION_JSON);

    return httpClientRequest;
  }

    /**
   * Here the reflection is used by the reason that we need to have an access to vert.x "waiters" objects
   * which are accumulated when batches are saved to database, the handling batches from inventory view
   * is performed match faster versus saving to database. By this reason in some time we got a lot of
   * waiters objects which holds many of others as well and this leads to OutOfMemory.
   * In solution we just don't allow to request new batches while we have 20 waiters objects
   * which perform saving instances to DB.
   * In future we can consider using static AtomicInteger to count the number of current db requests.
   * It will be more readable in code, but less reliable because wouldn't take into account other requests.
   */
  private Object getValueFrom(Object obj, String fieldName) {
    Field field = requireNonNull(ReflectionUtils.findField(requireNonNull(obj.getClass()), fieldName));
    ReflectionUtils.makeAccessible(field);
    return ReflectionUtils.getField(field, obj);
  }

  private ArrayDeque<Promise<Connection>> getWaitersQueue(Vertx vertx, Request request) {
    PgPool pgPool = PostgresClientFactory.getPool(vertx, request.getTenant());
    if (Objects.nonNull(pgPool)) {
      try {
        return (ArrayDeque<Promise<Connection>>) getValueFrom(getValueFrom(pgPool, "pool"), "waiters");
      } catch (NullPointerException ex) {
        throw new IllegalStateException("Cannot get the pool size. Object for retrieving field is null.");
      }
    } else {
      throw new IllegalStateException("Cannot obtain the pool. Pool is null.");
    }
  }

  private BatchStreamWrapper getBatchHttpStream(HttpClient inventoryHttpClient, Promise<?> promise, HttpClientRequest inventoryQuery, Context vertxContext) {
    final Vertx vertx = vertxContext.owner();

    BatchStreamWrapper databaseWriteStream = new BatchStreamWrapper(vertx, DATABASE_FETCHING_CHUNK_SIZE);

    inventoryQuery.handler(resp -> {
      if (resp.statusCode() != 200) {
        String errorMsg = getErrorFromStorageMessage("inventory-storage", inventoryQuery.absoluteURI(), resp.statusMessage());
        resp.bodyHandler(buffer -> logger.error(errorMsg + resp.statusCode() + "body: " + buffer.toString()));
        promise.fail(new IllegalStateException(errorMsg));
      } else {
        JsonParser jp = new JsonParserImpl(resp);
        jp.objectValueMode();
        jp.pipeTo(databaseWriteStream);
        jp.endHandler(e -> {
          databaseWriteStream.end();
          inventoryHttpClient.close();
        })
        .exceptionHandler(throwable -> {
          logger.error("Error has been occurred at JsonParser while reading data from response. Message:{0}", throwable.getMessage(), throwable);
          databaseWriteStream.end();
          inventoryHttpClient.close();
          promise.fail(throwable);
        });
      }
    });

    inventoryQuery.exceptionHandler(e -> {
      logger.error(e.getMessage(), e);
      handleException(promise, e);
    });

    databaseWriteStream.exceptionHandler(e -> {
      if (e != null) {
        handleException(promise, e);
      }
    });
    return databaseWriteStream;
  }

  private void enrichDiscoverySuppressed(JsonObject itemsandholdingsfields, JsonObject instance) {
    if (Boolean.parseBoolean(instance.getString("suppressFromDiscovery")))
      for (Object item : itemsandholdingsfields.getJsonArray("items")) {
        if (item instanceof JsonObject) {
          JsonObject itemJson = (JsonObject) item;
          itemJson.put(RecordMetadataManager.INVENTORY_SUPPRESS_DISCOVERY_FIELD, true);
        }
      }
  }

  private boolean isSkipSuppressed(Request request) {
    return !getBooleanProperty(request.getOkapiHeaders(), REPOSITORY_SUPPRESSED_RECORDS_PROCESSING);
  }

  private String getErrorFromStorageMessage(String errorSource, String uri, String responseMessage) {
    return format(ERROR_FROM_STORAGE, errorSource, uri, responseMessage);
  }

  private void handleException(Promise<?> promise, Throwable e) {
    logger.error(e.getMessage(), e);
    promise.fail(e);
  }

}
