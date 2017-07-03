package etl.cloud.twitter.ads;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.sun.deploy.util.StringUtils;
import twitter4j.*;
import twitter4j.api.TwitterAdsCampaignApi;
import twitter4j.api.TwitterAdsStatApi;
import twitter4j.auth.AuthorizationFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.internal.http.HttpParameter;
import twitter4j.internal.models4j.*;
import twitter4j.models.ads.*;
import twitter4j.util.TwitterAdUtil;

import java.io.*;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class ReportDownloader {

    public ReportDownloader(String accountId,
                            String oAuthConsumerKey, String oAuthConsumerSecret,
                            String oAuthAccessToken, String oAuthAccessTokenSecret,
                            int asyncJobExecutionTimeout) {
        this.accountId = accountId;

        Configuration configuration = new ConfigurationBuilder()
                .setOAuthConsumerKey(oAuthConsumerKey)
                .setOAuthConsumerSecret(oAuthConsumerSecret)
                .setOAuthAccessToken(oAuthAccessToken)
                .setOAuthAccessTokenSecret(oAuthAccessTokenSecret)
                .build();
        this.adsClient = new TwitterAdsClient(configuration, AuthorizationFactory.getInstance(configuration));
        this.asyncJobExecutionTimeout = asyncJobExecutionTimeout;

        TwitterAds twitterAdsInstance = new TwitterAdsFactory(configuration).getAdsInstance();
        this.statApi = twitterAdsInstance.getStatApi();
        this.campaignApi = twitterAdsInstance.getCampaignApi();
    }

    public void download(String entity, String segmentationType, String metricGroups, String startTime, String endTime,
                         String granularity, boolean withDeleted, String placement,
                         String outputFile)
            throws TwitterException, IOException, IllegalArgumentException, ParseException {

        // Fail early if outputFile can't be written
        try (FileWriter fw = new FileWriter(outputFile)) {
            fw.write("");
        }

        // Validate parameters
        TwitterAdUtil.ensureNotNull(entity, "entity");
        TwitterAdUtil.ensureNotNull(metricGroups, "metricGroups");
        TwitterAdUtil.ensureNotNull(startTime, "startTime");
        TwitterAdUtil.ensureNotNull(endTime, "endTime");
        TwitterAdUtil.ensureNotNull(granularity, "granularity");
        TwitterAdUtil.ensureNotNull(placement, "placement");

        Map<String, String> entitiesInfo;
        switch (entity) {
            case "CAMPAIGN":
                entitiesInfo = getAccountCampaigns();
                break;
            //TODO: reporting for other entity types support
            default:
                throw new IllegalArgumentException("entity is not recognized or unsupported.");
        }

        metricGroups = metricGroups.replaceAll("\\s+", "");

        // Get the data
        List<String> reportGeneratorIds = createAsyncJobs(
                entity, Lists.newArrayList(entitiesInfo.keySet()), startTime, endTime, withDeleted,
                granularity, metricGroups, placement, segmentationType);
        List<String> reportURLs = fetchDataURLs(reportGeneratorIds);
        List<String> rawJSONs = fetchAllData(reportURLs);
        String finalReportData = groomRawData(rawJSONs, entitiesInfo);

        // Save to file
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile))) {
            bw.write(finalReportData);
        }
    }

    private static final int MAX_ENTITY_IDS_PER_QUERY = 20;

    private String accountId;
    private int asyncJobExecutionTimeout;
    private Map<String, String> accountCampaigns;  // Id:Name
    private TwitterAdsClient adsClient;
    private TwitterAdsStatApi statApi;
    private TwitterAdsCampaignApi campaignApi;

    private Map<String, String> getAccountCampaigns() throws TwitterException {
        if (accountCampaigns == null) {
            accountCampaigns = fetchAccountCampaigns();
        }
        return accountCampaigns;
    }

    private Map<String, String> fetchAccountCampaigns() throws TwitterException {
        Map<String, String> campaigns = new HashMap<>();
        BaseAdsListResponseIterable<Campaign> allCampaigns = campaignApi.getAllCampaigns(this.accountId, null, null, false, null, null, null);
        for (BaseAdsListResponse<Campaign> allCampaign : allCampaigns) {
            for (Campaign cam : allCampaign.getData()) {
                campaigns.put(cam.getId(), cam.getName());
            }
        }
        return campaigns;
    }

    // Throw away excessive info, concatenate json arrays with data from multiple jobs and add entity names.
    private String groomRawData(List<String> rawJSONs, Map<String, String> entitiesInfo) throws ParseException {
        JSONArray allStatsData = new JSONArray();
        JSONParser parser = new JSONParser();

        for (String raw : rawJSONs) {
            JSONObject fullResponse = (JSONObject) parser.parse(raw);
            JSONArray statsData = (JSONArray) fullResponse.get("data");
            for (Object obj : statsData) {
                JSONObject singleEntityInfo = (JSONObject) obj;

                String entityId = (String) singleEntityInfo.get("id");
                singleEntityInfo.put("name", entitiesInfo.get(entityId));
                allStatsData.add(singleEntityInfo);
            }
        }

        return allStatsData.toJSONString();
    }

    private List<String> fetchAllData(List<String> dataURLs) throws IOException {
        List<String> data = new ArrayList<>();
        for (String url : dataURLs) {
            data.add(fetchDataGZip(url));
        }
        return data;
    }

    private String fetchDataGZip(String url) throws IOException {
        URL urlObj = new URL(url);
        HttpURLConnection con = (HttpURLConnection) urlObj.openConnection();
        con.setRequestProperty("Accept-Encoding", "gzip");
        BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(con.getInputStream())));

        StringBuilder builder = new StringBuilder();
        String line = reader.readLine();
        while (line != null) {
            builder.append(line);
            line = reader.readLine();
        }
        return builder.toString();
    }

    private List<String> fetchDataURLs(List<String> jobIds) throws TwitterException {
        List<String> dataURLs = new ArrayList<>();
        long timeoutMillis = TimeUnit.SECONDS.toMillis(asyncJobExecutionTimeout);

        boolean jobFinished;
        BaseAdsListResponseIterable<JobDetails> jobExecutionDetails;
        for (String jobId : jobIds) {
            jobFinished = false;
            long deadline = System.currentTimeMillis() + timeoutMillis;
            do {
                jobExecutionDetails = statApi.getJobExecutionDetails(accountId, Lists.newArrayList(jobId));
                for (BaseAdsListResponse<JobDetails> base : jobExecutionDetails) {
                    for (JobDetails jd : base.getData()) {
                        jobFinished = (jd != null) && (jd.getStatus() == TwitterAsyncQueryStatus.SUCCESS);
                    }
                }
                TwitterAdUtil.reallySleep(timeoutMillis / 10);
            } while (!jobFinished && System.currentTimeMillis() <= deadline);

            if (!jobFinished) {
                throw new TwitterException("report generation timed out.");
            }

            for (BaseAdsListResponse<JobDetails> base : jobExecutionDetails) {
                for (JobDetails jd : base.getData()) {
                    dataURLs.add(jd.getUrl());
                }
            }
        }

        return dataURLs;
    }

    private List<String> createAsyncJobs(String entity, List<String> entityIds, String startTime,
                                         String endTime, boolean withDeleted, String granularity,
                                         String metricGroups, String placement, String segmentationType) throws TwitterException {
        List<String> jobsDetails = new ArrayList<>();

        int totalEntityIds = entityIds.size();
        List<String> chunkOfIds = new ArrayList<>();
        for (int i = 0; i < totalEntityIds; i++) {
            chunkOfIds.add(entityIds.get(i));
            if (chunkOfIds.size() == MAX_ENTITY_IDS_PER_QUERY || i == totalEntityIds - 1) {
                BaseAdsResponse<JobDetails> campaignReportJob = createAsyncJob(
                        entity, chunkOfIds, startTime, endTime, withDeleted,
                        granularity, metricGroups, placement, segmentationType);
                jobsDetails.add(campaignReportJob.getData().getJobId());

                chunkOfIds.clear();
            }
        }

        return jobsDetails;
    }

    private BaseAdsResponse<JobDetails> createAsyncJob(String entity, Collection<String> entityIds, String startTime,
                                                       String endTime, boolean withDeleted, String granularity,
                                                       String metricGroups, String placement, String segmentationType) throws TwitterException {
        String baseUrl = "https://ads-api.twitter.com/1/stats/jobs/accounts/" + accountId;

        List<HttpParameter> params = new ArrayList<>();
        params.add(new HttpParameter("entity", entity));
        params.add(new HttpParameter("entity_ids", StringUtils.join(entityIds, ",")));
        params.add(new HttpParameter("start_time", startTime));
        params.add(new HttpParameter("end_time", endTime));
        params.add(new HttpParameter("with_deleted", withDeleted));
        params.add(new HttpParameter("granularity", granularity));
        params.add(new HttpParameter("metric_groups", metricGroups));
        params.add(new HttpParameter("placement", placement));

        if (segmentationType != null && TwitterAdUtil.isNotNullOrEmpty(segmentationType)) {
            params.add(new HttpParameter("segmentation_type", segmentationType));
        }

        Type type = new TypeToken<BaseAdsResponse<JobDetails>>() {}.getType();
        return adsClient.executeHttpRequest(baseUrl, params.toArray(new HttpParameter[params.size()]), type, HttpVerb.POST);
    }

}
