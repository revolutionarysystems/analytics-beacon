package uk.co.revsys.analytics.beacon;

import com.amazonaws.services.kinesis.model.PutRecordResult;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.io.IOUtils;
import org.json.JSONObject;
import uk.co.revsys.ripe.client.RipeClient;
import uk.co.revsys.ripe.client.RipeSearchResult;

public class AnalyticsBeaconServlet extends HttpServlet {

    private static final Logger log = Logger.getLogger(AnalyticsBeaconServlet.class.getName());

    private KinesisClient kinesisClient;
    private RipeClient ripeClient;
    private String css;
    private CharsetEncoder encoder;
    private String region;
    private String stream;

    @Override
    public void init() throws ServletException {
        super.init();
        try {
            Properties properties = new Properties();
            InputStream propertiesStream = getClass().getResourceAsStream("/aws.properties");
            properties.load(propertiesStream);
            region = properties.getProperty("region");
            stream = properties.getProperty("stream");
            kinesisClient = new KinesisClient(properties.getProperty("accessKey"), properties.getProperty("secretKey"));
            ripeClient = new RipeClient();
            InputStream cssStream = getClass().getResourceAsStream("/font.css");
            css = IOUtils.toString(cssStream);
            cssStream.close();
        } catch (IOException ex) {
            log.log(Level.SEVERE, "Unable to initialise analytics beacon", ex);
        } catch (NoSuchAlgorithmException ex) {
            log.log(Level.SEVERE, "Unable to initialise analytics beacon", ex);
        } catch (RuntimeException ex) {
            log.log(Level.SEVERE, "Unable to initialise analytics beacon", ex);
        }
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try {
            String accountId = req.getParameter("charset");
            String ipAddress = req.getRemoteAddr();
            JSONObject data = new JSONObject();
            data.put("accountId", accountId);
            data.put("ipAddress", ipAddress);
            data.put("timestamp", new Date().getTime());
            Enumeration<String> headerNames = req.getHeaderNames();
            JSONObject headers = new JSONObject();
            while (headerNames.hasMoreElements()) {
                String headerName = headerNames.nextElement();
                String headerValue = req.getHeader(headerName);
                headers.put(headerName, headerValue);
            }
            data.put("headers", headers);
            try {
                RipeSearchResult searchResult = ripeClient.search(ipAddress);
                HashMap network = new HashMap();
                network.put("name", searchResult.getNetworkName());
                network.put("description", searchResult.getNetworkDescription());
                data.put("network", network);
            } catch (IOException ex) {
                log.log(Level.SEVERE, "Unable to get network information", ex);
            } catch (RuntimeException ex) {
                log.log(Level.SEVERE, "Unable to get network information", ex);
            }
            System.out.println(data.toString());
            PutRecordResult result = kinesisClient.putRecord(region, stream, Math.round(Math.random() * 1000000) + "::beacon", Charset.forName("UTF-8").encode(data.toString()));
            System.out.println("result = " + result);
        } catch (InvalidKeyException ex) {
            log.log(Level.SEVERE, "Unable to send analytics beacon", ex);
        } catch (NoSuchAlgorithmException ex) {
            log.log(Level.SEVERE, "Unable to send analytics beacon", ex);
        } catch (IOException ex) {
            log.log(Level.SEVERE, "Unable to send analytics beacon", ex);
        } catch (RuntimeException ex) {
            log.log(Level.SEVERE, "Unable to send analytics beacon", ex);
        }
        resp.setContentType("text/css");
        resp.getOutputStream().print(css);
        resp.getOutputStream().flush();
    }

}
