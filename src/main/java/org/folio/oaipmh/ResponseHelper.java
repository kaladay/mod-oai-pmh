package org.folio.oaipmh;

import com.sun.xml.bind.marshaller.NamespacePrefixMapper;
import gov.loc.marc21.slim.RecordType;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2_0.oai_dc.Dc;
import org.purl.dc.elements._1.ObjectFactory;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import static javax.xml.XMLConstants.W3C_XML_SCHEMA_NS_URI;

public class ResponseHelper {
  private static final Logger logger = LoggerFactory.getLogger(ResponseHelper.class);
  private static final String SCHEMA_PATH = "ramls" + File.separator + "schemas" + File.separator;
  private static final String RESPONSE_SCHEMA = SCHEMA_PATH + "OAI-PMH.xsd";
  private static final String DC_SCHEMA = SCHEMA_PATH + "oai_dc.xsd";
  private static final String SIMPLE_DC_SCHEMA = SCHEMA_PATH + "simpledc20021212.xsd";
  private static final String MARC21_SCHEMA = SCHEMA_PATH + "MARC21slim.xsd";

  private static final Map<String, String> NAMESPACE_PREFIX_MAP = new HashMap<>();
  private final NamespacePrefixMapper namespacePrefixMapper;

  private static ResponseHelper ourInstance;

  static {
    NAMESPACE_PREFIX_MAP.put("http://www.loc.gov/MARC21/slim", "marc");
    NAMESPACE_PREFIX_MAP.put("http://purl.org/dc/elements/1.1/", "dc");
    NAMESPACE_PREFIX_MAP.put("http://www.openarchives.org/OAI/2.0/oai_dc/", "oai_dc");
    try {
      ourInstance = new ResponseHelper();
    } catch (JAXBException | SAXException e) {
      logger.error("The jaxb context could not be initialized");
      throw new IllegalStateException("Marshaller and unmarshaller are not available", e);
    }
  }
  private JAXBContext jaxbContext;
  private Schema oaipmhSchema;


  public static ResponseHelper getInstance() {
    return ourInstance;
  }

  /**
   * The main purpose is to initialize JAXB Marshaller and Unmarshaller to use the instances for business logic operations
   */
  private ResponseHelper() throws JAXBException, SAXException {
    jaxbContext = JAXBContext.newInstance(OAIPMH.class, RecordType.class, Dc.class,
      ObjectFactory.class);
    // Specifying OAI-PMH schema to validate response if the validation is enabled. Enabled by default if no config specified
    if (Boolean.parseBoolean(System.getProperty("jaxb.marshaller.enableValidation", Boolean.TRUE.toString()))) {
      ClassLoader classLoader = this.getClass().getClassLoader();
      StreamSource[] streamSources = {
        new StreamSource(classLoader.getResourceAsStream(RESPONSE_SCHEMA)),
        new StreamSource(classLoader.getResourceAsStream(MARC21_SCHEMA)),
        new StreamSource(classLoader.getResourceAsStream(SIMPLE_DC_SCHEMA)),
        new StreamSource(classLoader.getResourceAsStream(DC_SCHEMA))
      };
      oaipmhSchema = SchemaFactory.newInstance(W3C_XML_SCHEMA_NS_URI).newSchema(streamSources);
    }
    namespacePrefixMapper = new NamespacePrefixMapper() {
      @Override
      public String getPreferredPrefix(String namespaceUri, String suggestion, boolean requirePrefix) {
        return NAMESPACE_PREFIX_MAP.getOrDefault(namespaceUri, suggestion);
      }
    };
  }

  /**
   * Marshals {@link OAIPMH} object and returns string representation
   * @param response {@link OAIPMH} object to marshal
   * @return marshaled {@link OAIPMH} object as string representation
   */
  public String writeToString(OAIPMH response) {
    StringWriter writer = new StringWriter();
    try {
      // Marshaller is not thread-safe, so we should create every time a new one
      Marshaller jaxbMarshaller = jaxbContext.createMarshaller();
      if (oaipmhSchema != null) {
        jaxbMarshaller.setSchema(oaipmhSchema);
      }
      // Specifying xsi:schemaLocation (which will trigger xmlns:xsi being added to RS as well)
      jaxbMarshaller.setProperty(Marshaller.JAXB_SCHEMA_LOCATION,
        "http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd");
      // Specifying if output should be formatted
      jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.parseBoolean(System.getProperty("jaxb.marshaller.formattedOutput")));
      // needed to replace the namespace prefixes with a more readable format.
      jaxbMarshaller.setProperty("com.sun.xml.bind.namespacePrefixMapper", namespacePrefixMapper);
      jaxbMarshaller.marshal(response, writer);
    } catch (JAXBException e) {
      // In case there is an issue to marshal response, there is no way to handle it
      throw new IllegalStateException("The OAI-PMH response cannot be converted to string representation.", e);
    }

    return writer.toString();
  }

  /**
   * Unmarshals {@link OAIPMH} object based on passed string
   * @param oaipmhResponse the {@link OAIPMH} response in string representation
   * @return the {@link OAIPMH} object based on passed string
   */
  public OAIPMH stringToOaiPmh(String oaipmhResponse) {
    try {
      // Unmarshaller is not thread-safe, so we should create every time a new one
      Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
      if (oaipmhSchema != null) {
        jaxbUnmarshaller.setSchema(oaipmhSchema);
      }
      return (OAIPMH) jaxbUnmarshaller.unmarshal(new StringReader(oaipmhResponse));
    } catch (JAXBException e) {
      // In case there is an issue to unmarshal response, there is no way to handle it
      throw new IllegalStateException("The string cannot be converted to OAI-PMH response.", e);
    }
  }

  /**
   * Unmarshals {@link RecordType} or {@link Dc} objects based on passed byte array
   * @param byteSource the {@link RecordType} or {@link Dc} objects in byte[] representation
   * @return the object based on passed byte array
   */
  public Object bytesToObject(byte[] byteSource) {
    try {
      // Unmarshaller is not thread-safe, so we should create every time a new one
      Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
      if (oaipmhSchema != null) {
        jaxbUnmarshaller.setSchema(oaipmhSchema);
      }
      return jaxbUnmarshaller.unmarshal(new ByteArrayInputStream(byteSource));
    } catch (JAXBException e) {
      // In case there is an issue to unmarshal byteSource, there is no way to handle it
      throw new IllegalStateException("The byte array cannot be converted to JAXB object response.", e);
    }
  }

  /**
   * @return Checks if the Jaxb context initialized successfully
   */
  public boolean isJaxbInitialized() {
    return jaxbContext != null;
  }
}
