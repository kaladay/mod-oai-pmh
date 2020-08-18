package org.folio.oaipmh.service;

import static org.folio.oaipmh.Constants.REPOSITORY_SUPPRESSED_RECORDS_PROCESSING;
import static org.folio.oaipmh.helpers.RepositoryConfigurationUtil.getBooleanProperty;

import java.text.ParseException;
import java.time.format.DateTimeParseException;
import java.util.Date;

import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.lang3.StringUtils;
import org.folio.oaipmh.Constants;
import org.folio.oaipmh.Request;
import org.springframework.stereotype.Component;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class Utils {

  private static final Logger logger = LoggerFactory.getLogger(Utils.class);

  /**
   * Parse a date from string and compensate one date or one second because in SRS the dates are non-inclusive.
   * @param dateTimeString - date/time supplied
   * @param shouldCompensateUntilDate = if the date is used as until parameter
   * @return date that will be used to query SRS
   */
  public static Date convertStringToDate(String dateTimeString, boolean shouldCompensateUntilDate) {
    try {
      if (StringUtils.isEmpty(dateTimeString)) {
        return null;
      }
      Date date = DateUtils.parseDate(dateTimeString, Constants.DATE_FORMATS);
      if (shouldCompensateUntilDate){
        if (dateTimeString.matches(Constants.DATE_ONLY_PATTERN)){
          date = DateUtils.addDays(date, 1);
        }else{
          date = DateUtils.addSeconds(date, 1);
        }
      }
      return date;
    } catch (DateTimeParseException | ParseException e) {
      logger.error(e);
      return null;

    }
  }

  /**
   * Returns true or false depending on the value set in UI
   * @param request - oai pmh request
   */
  public static boolean shouldSkipSuppressedRecords (Request request) {
    return !getBooleanProperty(request.getOkapiHeaders(), REPOSITORY_SUPPRESSED_RECORDS_PROCESSING);
  }
}
