package org.folio.oaipmh.helpers.streaming;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static javax.ws.rs.core.HttpHeaders.ACCEPT;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.folio.oaipmh.Constants.NEXT_RECORD_ID_PARAM;
import static org.folio.oaipmh.Constants.OFFSET_PARAM;
import static org.folio.oaipmh.Constants.OKAPI_TENANT;
import static org.folio.oaipmh.Constants.OKAPI_TOKEN;
import static org.folio.oaipmh.Constants.REPOSITORY_MAX_RECORDS_PER_RESPONSE;
import static org.folio.oaipmh.Constants.REPOSITORY_SUPPRESSED_RECORDS_PROCESSING;
import static org.folio.oaipmh.Constants.REQUEST_ID_PARAM;
import static org.folio.oaipmh.Constants.UNTIL_PARAM;
import static org.folio.oaipmh.helpers.RepositoryConfigurationUtil.getBooleanProperty;
import static org.folio.oaipmh.service.Utils.shouldSkipSuppressedRecords;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import javax.ws.rs.core.Response;

import org.apache.commons.collections4.CollectionUtils;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.dao.impl.PostgresClientOaiPmhRepository;
import org.folio.oaipmh.helpers.AbstractHelper;
import org.folio.oaipmh.helpers.RepositoryConfigurationUtil;
import org.folio.oaipmh.helpers.records.RecordMetadataManager;
import org.folio.oaipmh.helpers.response.ResponseHelper;
import org.folio.oaipmh.service.FolioIntegrationService;
import org.folio.rest.client.SourceStorageSourceRecordsClient;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.spring.SpringContextUtil;
import org.openarchives.oai._2.ListRecordsType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.RecordType;
import org.openarchives.oai._2.ResumptionTokenType;
import org.openarchives.oai._2.StatusType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.collect.Maps;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.parsetools.JsonEvent;
import io.vertx.core.parsetools.JsonParser;
import io.vertx.core.parsetools.impl.JsonParserImpl;

@Component
public abstract class StreamingHelper extends AbstractHelper {

  private final Logger logger = LoggerFactory.getLogger(getClass());
  public static final int DATABASE_FETCHING_CHUNK_SIZE = 100;

  private static final String INSTANCE_ID_FIELD_NAME = "instanceid";
  private static final String SKIP_SUPPRESSED_FROM_DISCOVERY_RECORDS = "skipSuppressedFromDiscoveryRecords";
  private static final String INSTANCE_IDS_ENRICH_PARAM_NAME = "instanceIds";

  private static final String INVENTORY_INSTANCES_ENDPOINT = "/oai-pmh-view/enrichedInstances";

  protected FolioIntegrationService folioIntegrationService;

  protected PostgresClientOaiPmhRepository oaiPmhRepository;

  protected void handleStreamingResponse(Request request, Context vertxContext, Promise<Response> promise, PostgresClient postgresClient, String resumptionToken, boolean deletedRecordSupport, String requestId) {
    Future<Void> fetchingIdsPromise;
    if (resumptionToken == null
      || request.getRequestId() == null) { // the first request from EDS
      fetchingIdsPromise = createBatchStream(request, promise, vertxContext, requestId, postgresClient);
      fetchingIdsPromise.onFailure(t-> handleException(promise, t));
      fetchingIdsPromise.onSuccess(e -> processBatch(request, vertxContext, postgresClient, promise, deletedRecordSupport, requestId, true));
    } else {
      processBatch(request, vertxContext, postgresClient, promise, deletedRecordSupport, requestId, false);
    }
  }

  public StreamingHelper() {
    try {
      SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void processBatch(Request request, Context context, PostgresClient postgresClient, Promise<Response> promise, boolean deletedRecordSupport, String requestId, boolean firstBatch) {
    try {
      int batchSize = Integer.parseInt(
        RepositoryConfigurationUtil.getProperty(request.getTenant(),
          REPOSITORY_MAX_RECORDS_PER_RESPONSE));

      final Future<List<JsonObject>> nextInstances = oaiPmhRepository.getNextInstances(request, batchSize, postgresClient,
        o -> handleException(promise, o.cause()));


      nextInstances.compose(ids -> enrichInstancesWIthItemAndHoldingFields(ids, request, context).future()
        .onFailure(t -> handleException(promise, t))
        .onSuccess(instances -> {

          if (CollectionUtils.isEmpty(instances) && !firstBatch) { // resumption token doesn't exist in context
            handleException(promise, new IllegalArgumentException(
              "Specified resumption token doesn't exists"));
            return;
          }

          if (!instances.isEmpty() && instances.get(0).getString(INSTANCE_ID_FIELD_NAME).equals(request.getNextRecordId())) {
            handleException(promise, new IllegalArgumentException(
              "Stale resumption token"));
            return;
          }

          if (CollectionUtils.isEmpty(instances)) {
            buildRecordsResponse(request, requestId, instances, new HashMap<>(),
              firstBatch, null, deletedRecordSupport)
              .onSuccess(e -> postgresClient.closeClient(o -> promise.complete(e)))
              .onFailure(e -> handleException(promise, e));
            return;
          }

          String nextInstanceId = instances.size() < batchSize ? null : instances.get(batchSize).getString(INSTANCE_ID_FIELD_NAME);
          List<JsonObject> instancesWithoutLast = nextInstanceId != null ? instances.subList(0, batchSize) : instances;
          final SourceStorageSourceRecordsClient srsClient = new SourceStorageSourceRecordsClient(request.getOkapiUrl(),
            request.getTenant(), request.getOkapiToken());


          Future<Map<String, JsonObject>> srsResponse = Future.future();
          if (CollectionUtils.isNotEmpty(instances)) {
            srsResponse = requestSRSByIdentifiers(srsClient, instancesWithoutLast, deletedRecordSupport);
          } else {
            srsResponse.complete();
          }
          srsResponse.onSuccess(res -> buildRecordsResponse(request, requestId, instancesWithoutLast, res,
            firstBatch, nextInstanceId, deletedRecordSupport).onSuccess(result ->
            oaiPmhRepository.deleteInstanceIds(instancesWithoutLast.stream()
              .map(e -> e.getString(INSTANCE_ID_FIELD_NAME))
              .collect(toList()), requestId, postgresClient, o -> handleException(promise, o.cause()))
              .future().onComplete(e -> postgresClient.closeClient(o -> promise.complete(result)))).onFailure(e -> handleException(promise, e)));
          srsResponse.onFailure(t -> handleException(promise, t));

        }));
      } catch (Exception e) {
        handleException(promise, e);
      }


}
    //todo
//  protected abstract void processNextBatchOfInstanceIds(List<JsonObject> result, Request request, Context context);

  private Promise<List<JsonObject>> enrichInstancesWIthItemAndHoldingFields(List<JsonObject> result, Request request, Context context) {
    Map<String, JsonObject> instances = result.stream().collect(toMap(e -> e.getString(INSTANCE_ID_FIELD_NAME), Function.identity()));
    Promise<List<JsonObject>> completePromise = Promise.promise();
    HttpClient httpClient = context.owner().createHttpClient();

    HttpClientRequest enrichInventoryClientRequest = createEnrichInventoryClientRequest(httpClient, request);
    BatchStreamWrapper databaseWriteStream = getBatchHttpStream(httpClient, completePromise, enrichInventoryClientRequest, context);
    JsonObject entries = new JsonObject();
    entries.put(INSTANCE_IDS_ENRICH_PARAM_NAME, new JsonArray(new ArrayList<>(instances.keySet())));
    entries.put(SKIP_SUPPRESSED_FROM_DISCOVERY_RECORDS, shouldSkipSuppressedRecords(request));
    enrichInventoryClientRequest.end(entries.encode());

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
              logger.info(String.format("Instance with instanceId %s wasn't in the request", instanceId));
            }
          }
        }

        if (isTheLastBatch(databaseWriteStream, batch)) {
          completePromise.complete(new ArrayList<>(instances.values()));
        }
      } catch (Exception e) {
        completePromise.fail(e);
      }
    });

    return completePromise;
  }

  private void enrichDiscoverySuppressed(JsonObject itemsandholdingsfields, JsonObject instance) {
    if (Boolean.parseBoolean(instance.getString("suppressfromdiscovery")))
      for (Object item : itemsandholdingsfields.getJsonArray("items")) {
        if (item instanceof JsonObject) {
          JsonObject itemJson = (JsonObject) item;
          itemJson.put(RecordMetadataManager.INVENTORY_SUPPRESS_DISCOVERY_FIELD, true);
        }
      }
  }

  private ResumptionTokenType buildResumptionTokenFromRequest(Request request, String id, long returnedCount, String nextInstanceId) {
    long offset = returnedCount + request.getOffset();
    if (nextInstanceId == null) {
      return new ResumptionTokenType()
        .withValue("")
        .withCursor(
          BigInteger.valueOf(offset));
    }
    Map<String, String> extraParams = new HashMap<>();
    extraParams.put(OFFSET_PARAM, String.valueOf(returnedCount));
    extraParams.put(REQUEST_ID_PARAM, id);
    extraParams.put(NEXT_RECORD_ID_PARAM, nextInstanceId);
    if (request.getUntil() == null) {
      extraParams.put(UNTIL_PARAM, getUntilDate(request, request.getFrom()));
    }

    String resumptionToken = request.toResumptionToken(extraParams);

    return new ResumptionTokenType()
      .withValue(resumptionToken)
      .withCursor(
        request.getOffset() == 0 ? BigInteger.ZERO : BigInteger.valueOf(request.getOffset()));
  }

  private Future<Response> buildRecordsResponse(
    Request request, String requestId, List<JsonObject> batch,
    Map<String, JsonObject> srsResponse, boolean firstBatch,
    String nextInstanceId, boolean deletedRecordSupport) {

    Promise<Response> promise = Promise.promise();
    try {
      List<RecordType> records = buildRecordsList(request, batch, srsResponse, deletedRecordSupport);

      ResponseHelper responseHelper = getResponseHelper();
      OAIPMH oaipmh = responseHelper.buildBaseOaipmhResponse(request);
      if (records.isEmpty() && nextInstanceId == null && (firstBatch && batch.isEmpty())) {
        oaipmh.withErrors(createNoRecordsFoundError());
      } else {
        oaipmh.withListRecords(new ListRecordsType().withRecords(records));
      }
      Response response;
      if (oaipmh.getErrors().isEmpty()) {
        if (!firstBatch || nextInstanceId != null) {
          ResumptionTokenType resumptionToken = buildResumptionTokenFromRequest(request, requestId,
            records.size(), nextInstanceId);
          oaipmh.getListRecords().withResumptionToken(resumptionToken);
        }
        response = responseHelper.buildSuccessResponse(oaipmh);
      } else {
        response = responseHelper.buildFailureResponse(oaipmh, request);
      }

      promise.complete(response);
    } catch (Exception e) {
      handleException(promise, e);
    }
    return promise.future();
  }

  private List<RecordType> buildRecordsList(Request request, List<JsonObject> batch, Map<String, JsonObject> srsResponse,
                                            boolean deletedRecordSupport) {
    RecordMetadataManager metadataManager = RecordMetadataManager.getInstance();

    final boolean suppressedRecordsProcessing = getBooleanProperty(request.getOkapiHeaders(),
      REPOSITORY_SUPPRESSED_RECORDS_PROCESSING);

    List<RecordType> records = new ArrayList<>();

    for (JsonObject inventoryInstance : batch) {
      final String instanceId = inventoryInstance.getString(INSTANCE_ID_FIELD_NAME);
      final JsonObject srsInstance = srsResponse.get(instanceId);
      if (srsInstance == null) {
        continue;
      }
      JsonObject updatedSrsInstance = metadataManager
        .populateMetadataWithItemsData(srsInstance, inventoryInstance,
          suppressedRecordsProcessing);
      String identifierPrefix = request.getIdentifierPrefix();
      RecordType record = new RecordType()
        .withHeader(createHeader(inventoryInstance)
          .withIdentifier(getIdentifier(identifierPrefix, instanceId)));

      if (deletedRecordSupport && storageHelper.isRecordMarkAsDeleted(updatedSrsInstance)) {
        record.getHeader().setStatus(StatusType.DELETED);
      }
      String source = storageHelper.getInstanceRecordSource(updatedSrsInstance);
      if (source != null && record.getHeader().getStatus() == null) {
        if (suppressedRecordsProcessing) {
          source = metadataManager.updateMetadataSourceWithDiscoverySuppressedData(source, updatedSrsInstance);
          source = metadataManager.updateElectronicAccessFieldWithDiscoverySuppressedData(source, updatedSrsInstance);
        }
        record.withMetadata(buildOaiMetadata(request, source));
      }

      if (filterInstance(request, srsInstance)) {
        records.add(record);
      }
    }
    return records;
  }

  private Future<Void> createBatchStream(Request request,
                                          Promise<Response> oaiPmhResponsePromise,
                                          Context vertxContext, String requestId, PostgresClient postgresClient) {
    Promise<Void> completePromise = Promise.promise();
    HttpClient httpClient = vertxContext.owner().createHttpClient();
    HttpClientRequest httpClientRequest = folioIntegrationService.buildGetUpdatedInstanceIdsQuery(httpClient, request);
    BatchStreamWrapper databaseWriteStream = getBatchHttpStream(httpClient, oaiPmhResponsePromise, httpClientRequest, vertxContext);
    httpClientRequest.sendHead();
    databaseWriteStream.handleBatch(batch -> {
      Promise<Void> savePromise = oaiPmhRepository.saveInstancesIds(batch, request, requestId, postgresClient);
      savePromise.future().onFailure(completePromise::fail);
      if (isTheLastBatch(databaseWriteStream, batch)) {
        savePromise.future().onSuccess(e -> completePromise.complete());
      }

    });
    return completePromise.future();
  }

  private boolean isTheLastBatch(BatchStreamWrapper databaseWriteStream, List<JsonEvent> batch) {
    return batch.size() < DATABASE_FETCHING_CHUNK_SIZE ||
      (databaseWriteStream.isStreamEnded()
        && databaseWriteStream.getItemsInQueueCount() <= DATABASE_FETCHING_CHUNK_SIZE);
  }

  private BatchStreamWrapper getBatchHttpStream(HttpClient inventoryHttpClient, Promise<?> exceptionPromise, HttpClientRequest inventoryQuery, Context vertxContext) {
    final Vertx vertx = vertxContext.owner();

    BatchStreamWrapper databaseWriteStream = new BatchStreamWrapper(vertx);

    inventoryQuery.handler(resp -> {
      JsonParser jp = new JsonParserImpl(resp);
      jp.objectValueMode();
      jp.pipeTo(databaseWriteStream);
      jp.endHandler(e -> {
        databaseWriteStream.end();
        inventoryHttpClient.close();
      });
    });

    inventoryQuery.exceptionHandler(e -> {
      logger.error(e.getMessage(), e);
      handleException(exceptionPromise, e);
    });

    databaseWriteStream.exceptionHandler(e -> {
      if (e != null) {
        handleException(exceptionPromise, e);
      }
    });
    return databaseWriteStream;
  }

  //TODO
  private HttpClientRequest createEnrichInventoryClientRequest(HttpClient httpClient, Request request) {
    final HttpClientRequest httpClientRequest = httpClient
      .postAbs(String.format("%s%s", request.getOkapiUrl(), INVENTORY_INSTANCES_ENDPOINT));

    httpClientRequest.putHeader(OKAPI_TOKEN, request.getOkapiToken());
    httpClientRequest.putHeader(OKAPI_TENANT, TenantTool.tenantId(request.getOkapiHeaders()));
    httpClientRequest.putHeader(ACCEPT, APPLICATION_JSON);
    httpClientRequest.putHeader(CONTENT_TYPE, APPLICATION_JSON);

    return httpClientRequest;
  }
  //TODO
  private Future<Map<String, JsonObject>> requestSRSByIdentifiers(SourceStorageSourceRecordsClient srsClient,
                                                                  List<JsonObject> batch, boolean deletedRecordSupport) {
    final List<String> listOfIds = extractListOfIdsForSRSRequest(batch);
    logger.info("Request to SRS: {0}", listOfIds);
    Promise<Map<String, JsonObject>> promise = Promise.promise();
    try {
      final Map<String, JsonObject> result = Maps.newHashMap();
      srsClient.postSourceStorageSourceRecords("INSTANCE", deletedRecordSupport, listOfIds, rh -> rh.bodyHandler(bh -> {
          try {
            final Object o = bh.toJson();
            if (o instanceof JsonObject) {
              JsonObject entries = (JsonObject) o;
              final JsonArray records = entries.getJsonArray("sourceRecords");
              records.stream()
                .filter(Objects::nonNull)
                .map(r -> (JsonObject) r)
                .forEach(jo -> result.put(jo.getJsonObject("externalIdsHolder").getString("instanceId"), jo));
            } else {
              logger.debug("Can't process response from SRS: {0}", bh.toString());
            }
            promise.complete(result);
          } catch (Exception e) {
            handleException(promise, e);
          }
        }
      ));
    } catch (Exception e) {
      handleException(promise, e);
    }

    return promise.future();
  }

  //TODO
  private List<String> extractListOfIdsForSRSRequest(List<JsonObject> batch) {

    return batch.stream().
      filter(Objects::nonNull)
      .map(instance -> instance.getString(INSTANCE_ID_FIELD_NAME))
      .collect(toList());
  }

  @Autowired
  public void setFolioIntegrationService(FolioIntegrationService folioIntegrationService) {
    this.folioIntegrationService = folioIntegrationService;
  }
  @Autowired
  public void setOaiPmhRepository(PostgresClientOaiPmhRepository oaiPmhRepository) {
    this.oaiPmhRepository = oaiPmhRepository;
  }
}
