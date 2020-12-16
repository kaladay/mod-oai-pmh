package org.folio.oaipmh.helpers;

import static java.lang.String.format;
import static javax.ws.rs.core.HttpHeaders.ACCEPT;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.folio.oaipmh.Constants.OKAPI_TENANT;
import static org.folio.oaipmh.Constants.OKAPI_TOKEN;
import static org.folio.oaipmh.Constants.REPOSITORY_MAX_RECORDS_PER_RESPONSE;
import static org.folio.oaipmh.Constants.REPOSITORY_SUPPRESSED_RECORDS_PROCESSING;
import static org.folio.oaipmh.Constants.SOURCE_RECORDS;
import static org.folio.oaipmh.Constants.TOTAL_RECORDS_PARAM;
import static org.folio.oaipmh.helpers.RepositoryConfigurationUtil.getBooleanProperty;

import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.List;

import javax.ws.rs.core.Response;

import org.assertj.core.util.Arrays;
import org.folio.oaipmh.Constants;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.helpers.records.RecordMetadataManager;
import org.folio.rest.client.SourceStorageSourceRecordsClient;
import org.folio.rest.tools.utils.TenantTool;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.OAIPMHerrorcodeType;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.parsetools.impl.JsonParserImpl;

public class GetOaiRecordWithHoldingsHelper extends GetOaiRecordHelper {

  private static final String INVENTORY_INSTANCES_ENDPOINT = "/oai-pmh-view/enrichedInstances";

  private static final String ERROR_FROM_STORAGE = "Got error response from %s, uri: '%s' message: %s";

  private static final String SKIP_SUPPRESSED_FROM_DISCOVERY_RECORDS = "skipSuppressedFromDiscoveryRecords";

  private static final String INSTANCE_IDS_ENRICH_PARAM_NAME = "instanceIds";

  private static final String INSTANCE_ID_FIELD_NAME = "instanceid";

  public static final GetOaiRecordWithHoldingsHelper INSTANCE = new GetOaiRecordWithHoldingsHelper();

  public static GetOaiRecordWithHoldingsHelper getInstance() {
    return INSTANCE;
  }

  /**
   * Handle the request.
   *
   * 1) Attempt to load the Instance by the InstanceId.
   * 2) If InstanceId is not found, then call GetOaiRecordHelper.handle() for normal operation.
   * 3) If InstanceId is found, then load Source Record.
   * 4) If Source Record is not found then error.
   * 5) Return the merged the Source Record and the InstanceId.
   */
  @Override
  public Future<Response> handle(Request request, Context ctx) {
    Promise<Response> promise = Promise.promise();

    try {
      List<OAIPMHerrorType> errors = validateRequest(request);
      if (!errors.isEmpty()) {
        return buildResponseWithErrors(request, promise, errors);
      }

      String instanceId = request.getIdentifier().replace(request.getIdentifierPrefix(), "");

      JsonObject instance = new JsonObject();
      instance.put(INSTANCE_ID_FIELD_NAME, instanceId);

      enhanceInstance(instance, request, ctx).onComplete(r -> {

        if (r.failed()) {
          super.handle(request, ctx).onComplete(r1 -> {
            promise.handle(r1);
          });
          return;
        }

        final JsonObject instanceWithHolding = r.result();

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

                      final RecordMetadataManager metadataManager = RecordMetadataManager.getInstance();
                      final JsonObject srsInstanceCollection = bh.toJsonObject();
                      final Integer totalRecords = srsInstanceCollection.getInteger(TOTAL_RECORDS_PARAM);

                      if (totalRecords == 0) {
                        final OAIPMH oaipmh = getResponseHelper().buildOaipmhResponseWithErrors(request, OAIPMHerrorcodeType.ID_DOES_NOT_EXIST, Constants.RECORD_NOT_FOUND_ERROR);
                        final Response notFound = getResponseHelper().buildFailureResponse(oaipmh, request);
                        promise.complete(notFound);
                        return;
                      }

                      final JsonArray sourceRecords = srsInstanceCollection.getJsonArray(SOURCE_RECORDS);
                      final JsonObject srsInstance = sourceRecords.getJsonObject(0);
                      final JsonObject updatedSrsInstance = metadataManager
                        .populateMetadataWithItemsData(srsInstance, instanceWithHolding, suppressedRecordsSupport);

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

  private Future<JsonObject> enhanceInstance(JsonObject instance, Request request, Context context) {
    Promise<JsonObject> promise = Promise.promise();
    HttpClient httpClient = context.owner().createHttpClient();
    HttpClientRequest inventoryQuery = createEnrichInventoryClientRequest(httpClient, request);

    String instanceId = instance.getString(INSTANCE_ID_FIELD_NAME);

    inventoryQuery.handler(resp -> {
      if (resp.statusCode() != 200) {
        String errorMsg = getErrorFromStorageMessage("inventory-storage", inventoryQuery.absoluteURI(), resp.statusMessage());
        resp.bodyHandler(buffer -> logger.error(errorMsg + resp.statusCode() + "body: " + buffer.toString()));
        promise.fail(new IllegalStateException(errorMsg));
      } else {
        new JsonParserImpl(resp)
          .objectValueMode()
          .handler(jsonEvent -> {
            JsonObject value = jsonEvent.objectValue();
            Object itemsandholdingsfields = value.getValue(RecordMetadataManager.ITEMS_AND_HOLDINGS_FIELDS);

            if (itemsandholdingsfields instanceof JsonObject) {
              enrichDiscoverySuppressed((JsonObject) itemsandholdingsfields, instance);
              instance.put(RecordMetadataManager.ITEMS_AND_HOLDINGS_FIELDS, itemsandholdingsfields);
              promise.complete(instance);
            } else {
              promise.fail(format("Instance with instanceId %s wasn't in the request", instanceId));
            }
          })
          .endHandler(e -> {
            httpClient.close();
          })
          .exceptionHandler(throwable -> {
            logger.error("Error has been occurred at JsonParser while reading data from response. Message:{0}", throwable.getMessage(), throwable);
            httpClient.close();
            promise.fail(throwable);
          });
      }
    });

    inventoryQuery.exceptionHandler(e -> {
      logger.error(e.getMessage(), e);
      handleException(promise, e);
    });

    JsonObject entries = new JsonObject();
    entries.put(INSTANCE_IDS_ENRICH_PARAM_NAME, new JsonArray(Arrays.asList(new String[] { instanceId })));
    entries.put(SKIP_SUPPRESSED_FROM_DISCOVERY_RECORDS, isSkipSuppressed(request));

    inventoryQuery.end(entries.encode());

    return promise.future();
  }

  /**
   * Identical to MarcWithHoldingsRequestHelper.createEnrichInventoryClientRequest().
   */
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
   * Identical to MarcWithHoldingsRequestHelper.enrichDiscoverySuppressed().
   */
  private void enrichDiscoverySuppressed(JsonObject itemsandholdingsfields, JsonObject instance) {
    if (Boolean.parseBoolean(instance.getString("suppressFromDiscovery")))
      for (Object item : itemsandholdingsfields.getJsonArray("items")) {
        if (item instanceof JsonObject) {
          JsonObject itemJson = (JsonObject) item;
          itemJson.put(RecordMetadataManager.INVENTORY_SUPPRESS_DISCOVERY_FIELD, true);
        }
      }
  }

  /**
   * Identical to MarcWithHoldingsRequestHelper.isSkipSuppressed().
   */
  private boolean isSkipSuppressed(Request request) {
    return !getBooleanProperty(request.getOkapiHeaders(), REPOSITORY_SUPPRESSED_RECORDS_PROCESSING);
  }

  /**
   * Identical to MarcWithHoldingsRequestHelper.getErrorFromStorageMessage().
   */
  private String getErrorFromStorageMessage(String errorSource, String uri, String responseMessage) {
    return format(ERROR_FROM_STORAGE, errorSource, uri, responseMessage);
  }

  /**
   * Identical to MarcWithHoldingsRequestHelper.handleException().
   */
  private void handleException(Promise<?> promise, Throwable e) {
    logger.error(e.getMessage(), e);
    promise.fail(e);
  }

}
