package org.folio.oaipmh.helpers;

import static java.lang.String.format;
import static javax.ws.rs.core.HttpHeaders.ACCEPT;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.folio.oaipmh.Constants.CANNOT_DISSEMINATE_FORMAT_ERROR;
import static org.folio.oaipmh.Constants.INVALID_IDENTIFIER_ERROR_MESSAGE;
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.folio.oaipmh.MetadataPrefix;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.helpers.records.RecordMetadataManager;
import org.folio.oaipmh.helpers.streaming.BatchStreamWrapper;
import org.folio.rest.tools.utils.TenantTool;
import org.openarchives.oai._2.GetRecordType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.RecordType;
import org.openarchives.oai._2.ResumptionTokenType;
import org.openarchives.oai._2.StatusType;

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

public class GetOaiRecordWithHoldingsHelper extends GetOaiRecordHelper {

  private static final int DATABASE_FETCHING_CHUNK_SIZE = 50;

  private static final String INVENTORY_INSTANCES_ENDPOINT = "/oai-pmh-view/enrichedInstances";

  private static final String ERROR_FROM_STORAGE = "Got error response from %s, uri: '%s' message: %s";

  private static final String SKIP_SUPPRESSED_FROM_DISCOVERY_RECORDS = "skipSuppressedFromDiscoveryRecords";

  private static final String INSTANCE_ID_ENRICH_PARAM_NAME = "instanceId";

  public static final GetOaiRecordWithHoldingsHelper INSTANCE = new GetOaiRecordWithHoldingsHelper();

  public static GetOaiRecordWithHoldingsHelper getInstance() {
    return INSTANCE;
  }

  @Override
  protected Map<String, RecordType> buildRecords(Context context, Request request, JsonArray instances) {
    final boolean suppressedRecordsProcessingEnabled = getBooleanProperty(request.getOkapiHeaders(),
      REPOSITORY_SUPPRESSED_RECORDS_PROCESSING);

    if (instances == null || instances.isEmpty()) {
      return Collections.emptyMap();
    }

    // Using LinkedHashMap just to rely on order returned by storage service
    final Map<String, RecordType> records = new LinkedHashMap<>();

    List<Future> futures = new ArrayList<>();
  
    RecordMetadataManager metadataManager = RecordMetadataManager.getInstance();
    String identifierPrefix = request.getIdentifierPrefix();
    for (Object entity : instances) {
      JsonObject instance = (JsonObject) entity;
      String recordId = storageHelper.getRecordId(instance);
      String identifierId = storageHelper.getIdentifierId(instance);
      if (StringUtils.isNotEmpty(identifierId)) {
        RecordType record = new RecordType()
          .withHeader(createHeader(instance)
            .withIdentifier(getIdentifier(identifierPrefix, identifierId)));
        if (isDeletedRecordsEnabled(request) && storageHelper.isRecordMarkAsDeleted(instance)) {
          record.getHeader().setStatus(StatusType.DELETED);
        }

        System.out.print("\n\n\nDEBUG: (BEFORE) instance (" + recordId + ") = " + instance.encodePrettily() + "\n\n\n");
        futures.add(enrichInstance(recordId, instance, request, context).onComplete(r -> {
          final JsonObject instanceWithHolding = r.succeeded() ? r.result() : instance;

          System.out.print("\n\n\nDEBUG: (AFTER) instanceWithHoldings = " + instanceWithHolding.encodePrettily() + "\n\n\n");

          // Some repositories like SRS can return record source data along with other info
          final String source = storageHelper.getInstanceRecordSource(instanceWithHolding);
          if (source != null && record.getHeader().getStatus() == null) {
            if (suppressedRecordsProcessingEnabled) {
              final String altSource = metadataManager.updateMetadataSourceWithDiscoverySuppressedData(source, instanceWithHolding);
              record.withMetadata(buildOaiMetadata(request, altSource));
            }
            else {
              record.withMetadata(buildOaiMetadata(request, source));
            }
          } else {
            context.put(recordId, instanceWithHolding);
          }
          if (filterInstance(request, instanceWithHolding)) {
            records.put(recordId, record);
          }
        }));
      }
    }

    CompositeFuture.all(futures);
    return records;
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

  private Future<JsonObject> enrichInstance(String instanceId, JsonObject instance, Request request, Context context) {
    Promise<JsonObject> completePromise = Promise.promise();
    HttpClient httpClient = context.owner().createHttpClient();

    HttpClientRequest enrichInventoryClientRequest = createEnrichInventoryClientRequest(httpClient, request);
    BatchStreamWrapper databaseWriteStream = getBatchHttpStream(httpClient, completePromise, enrichInventoryClientRequest, context);

    JsonObject entries = new JsonObject();
    entries.put(INSTANCE_ID_ENRICH_PARAM_NAME, instanceId);
    entries.put(SKIP_SUPPRESSED_FROM_DISCOVERY_RECORDS, isSkipSuppressed(request));
    enrichInventoryClientRequest.end(entries.encode());

    databaseWriteStream.handleBatch(batch -> {
      try {
        for (JsonEvent jsonEvent : batch) {
          JsonObject value = jsonEvent.objectValue();
          Object itemsandholdingsfields = value.getValue(RecordMetadataManager.ITEMS_AND_HOLDINGS_FIELDS);

          if (itemsandholdingsfields instanceof JsonObject) {
            enrichDiscoverySuppressed((JsonObject) itemsandholdingsfields, instance);
            instance.put(RecordMetadataManager.ITEMS_AND_HOLDINGS_FIELDS,
              itemsandholdingsfields);
          }
        }

        if (databaseWriteStream.isTheLastBatch() && !completePromise.future().isComplete()) {
          completePromise.complete(instance);
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

  private BatchStreamWrapper getBatchHttpStream(HttpClient inventoryHttpClient, Promise<?> promise, HttpClientRequest inventoryQuery, Context vertxContext) {
    final Vertx vertx = vertxContext.owner();

    BatchStreamWrapper databaseWriteStream = new BatchStreamWrapper(vertx, DATABASE_FETCHING_CHUNK_SIZE);

    inventoryQuery.handler(resp -> {
      if(resp.statusCode() != 200) {
        String errorMsg = getErrorFromStorageMessage("inventory-storage", inventoryQuery.absoluteURI(), resp.statusMessage());
        logger.error(errorMsg);
        promise.fail(new IllegalStateException(errorMsg));
      } else {
        JsonParser jp = new JsonParserImpl(resp);
        jp.objectValueMode();
        jp.pipeTo(databaseWriteStream);
        jp.endHandler(e -> {
          databaseWriteStream.end();
          inventoryHttpClient.close();
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
