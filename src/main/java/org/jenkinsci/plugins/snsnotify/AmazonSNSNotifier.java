package org.jenkinsci.plugins.snsnotify;

import com.amazonaws.auth.*;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.MessageAttributeValue;
import hudson.EnvVars;
import hudson.Extension;
import hudson.Launcher;
import hudson.Util;
import hudson.model.*;
import hudson.model.BuildListener;
import hudson.model.Run.Artifact;
import hudson.tasks.BuildStepDescriptor;
import hudson.tasks.BuildStepMonitor;
import hudson.tasks.Notifier;
import hudson.tasks.Publisher;
import hudson.util.Secret;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.kohsuke.stapler.DataBoundConstructor;
import org.kohsuke.stapler.StaplerRequest;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.logging.Logger;

public class AmazonSNSNotifier extends Notifier {

    private final static Logger LOG = Logger.getLogger(AmazonSNSNotifier.class.getName());

    private final String projectTopicArn;
    private final String subjectTemplate;
    private final String messageTemplate;

    @DataBoundConstructor
    public AmazonSNSNotifier(String projectTopicArn, String subjectTemplate, String messageTemplate) {
        super();
        this.projectTopicArn = projectTopicArn;
        this.subjectTemplate = subjectTemplate;
        this.messageTemplate = messageTemplate;
    }

    public String getProjectTopicArn() {
        return projectTopicArn;
    }

    public String getSubjectTemplate() {
        return subjectTemplate;
    }

    public String getMessageTemplate() {
        return messageTemplate;
    }

    // ~~

    @Override
    public DescriptorImpl getDescriptor() {
        return (DescriptorImpl) super.getDescriptor();
    }

    // ~~

    @SuppressWarnings("unchecked")
    public static AmazonSNSNotifier getNotifier(AbstractProject project) {
        Map<Descriptor<Publisher>, Publisher> map = project.getPublishersList().toMap();
        for (Publisher publisher : map.values()) {
            if (publisher instanceof AmazonSNSNotifier) {
                return (AmazonSNSNotifier) publisher;
            }
        }
        return null;
    }

    public void onStarted(AbstractBuild build, TaskListener listener) {
        if (getDescriptor().isDefaultSendNotificationOnStart()) {
            LOG.info("Prepare SNS notification for build started...");
            send(build, listener, BuildPhase.STARTED);
        }
    }

    public void onCompleted(AbstractBuild build, TaskListener listener) {
        boolean notifyOnConsecutiveSuccesses = getDescriptor().isDefaultNotifyOnConsecutiveSuccesses();
        boolean previousBuildSuccessful = isPreviousBuildSuccess(build);
        if (notifyOnConsecutiveSuccesses || (!notifyOnConsecutiveSuccesses && !previousBuildSuccessful)) {
            LOG.info("Prepare SNS notification for build completed...");
            send(build, listener, BuildPhase.COMPLETED);
        }
    }

    public BuildStepMonitor getRequiredMonitorService() {
        return BuildStepMonitor.NONE;
    }

    @Override
    public boolean perform(AbstractBuild<?, ?> build, Launcher launcher, BuildListener listener) throws InterruptedException, IOException {
        //return super.perform(build, launcher, listener);
        return true;
    }

    // ~~ SNS specific implementation

    private void send(AbstractBuild build, TaskListener listener, BuildPhase phase) {
        String awsAccessKey = getDescriptor().getAwsAccessKey();
        Secret awsSecretKey = getDescriptor().getAwsSecretKey();
        String publishTopic = isEmpty(projectTopicArn) ?
                getDescriptor().getDefaultTopicArn() : projectTopicArn;
        boolean useLocalCredential = getDescriptor().isDefaultLocalCredential();
        Map<String,MessageAttributeValue> messageAttributes = new HashMap<>();

        if (isEmpty(publishTopic)) {
            listener.error("No global or project topic ARN sent; cannot send SNS notification");
            return;
        }

        if (!(useLocalCredential) && (isEmpty(awsAccessKey) || isEmpty(awsSecretKey.getPlainText()))) {
            listener.error("AWS credentials not configured; cannot send SNS notification");
            return;
        }

        AwsClientBuilder.EndpointConfiguration snsApiEndpoint = getSNSApiEndpoint(publishTopic);
        if (snsApiEndpoint == null) {
            listener.error("Could not determine SNS API Endpoint from topic ARN: " + publishTopic);
            return;
        }

        // ~~ prepare subject (incl. variable replacement)
        String subject;
        if (StringUtils.isEmpty(subjectTemplate)) {
            Result buildResult = build.getResult();
            subject = truncate(
                    String.format("Build %s: %s",
                            phase == BuildPhase.STARTED || buildResult == null? "STARTED" : buildResult.toString(),
                            build.getFullDisplayName()), 100);

            // ~~ prepare message result attribute
            messageAttributes.put("result", new MessageAttributeValue()
                                            .withDataType("String")
                                            .withStringValue(buildResult == null? "STARTED" : buildResult.toString()));
        } else {
            subject = replaceVariables(build, listener, phase, subjectTemplate);
        }

        // ~~ prepare message (incl. variable replacement)
        String message = replaceVariables(build, listener, phase, 
                isEmpty(messageTemplate) ? getDescriptor().getDefaultMessageTemplate() : messageTemplate);

        // ~~ prepare message phase attribute
        messageAttributes.put("phase", new MessageAttributeValue()
                                        .withDataType("String")
                                        .withStringValue(phase.toString()));

        LOG.info("Setup SNS client '" + snsApiEndpoint + "' ...");
        AWSCredentialsProvider awsCredentialsProvider;
        try {
            if (useLocalCredential) {
                awsCredentialsProvider = new DefaultAWSCredentialsProviderChain();
            }
            else {
                awsCredentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(awsAccessKey, awsSecretKey.getPlainText()));
            }
        }
        catch (Exception e) {
            listener.error("Failed to send SNS notification: " + e.getMessage());
            return;
        }

        AmazonSNS snsClient = AmazonSNSClient.builder()
                .withEndpointConfiguration(snsApiEndpoint)
                .withCredentials(awsCredentialsProvider)
                .build();

        try {
            String summary = String.format("subject=%s topic=%s", subject, publishTopic);
            LOG.info("Publish SNS notification: " + summary + " ...");
            PublishRequest pubReq = new PublishRequest(publishTopic, message, subject);
            pubReq.setMessageAttributes(messageAttributes);
            snsClient.publish(pubReq);
            listener.getLogger().println("Published SNS notification: " + summary);
        } catch (Exception e) {
            listener.error("Failed to send SNS notification: " + e.getMessage());
        } finally {
            snsClient.shutdown();
        }
    }

    /**
     * Checks to see if the current build result was SUCCESS and the previous build's result
     * was SUCCESS. If this is true then the build state has not changed in a way that should
     * trigger a notification.
     */
    private boolean isPreviousBuildSuccess(AbstractBuild build) {
        if (build.getResult() == Result.SUCCESS && findPreviousBuildResult(build) == Result.SUCCESS) {
            return true;
        }
        return false;
    }

    /**
     * To correctly compute the state change from the previous build to this build,
     * we need to ignore aborted builds, and since we are consulting the earlier
     * result, if the previous build is still running, behave as if this were the
     * first build.
     */
     private Result findPreviousBuildResult(AbstractBuild b) {
        do {
            b = b.getPreviousBuild();
            if (b == null || b.isBuilding()) {
                return null;
            }
        } while((b.getResult() == Result.ABORTED) || (b.getResult() == Result.NOT_BUILT));
        return b.getResult();
    }

    private String truncate(String s, int toLength) {
        if (s.length() > toLength) {
            return s.substring(0, toLength);
        }
        return s;
    }

    private boolean isEmpty(String s) {
        return s == null || s.trim().length() == 0;
    }

    /**
     * Replaces all build and environment variables given by the String tmpl.
     */
    private String replaceVariables(AbstractBuild build, TaskListener listener,
                                    BuildPhase phase, String tmpl) {
        String resultString = tmpl;

        try {
            EnvVars envVars = build.getEnvironment(listener);
            Result buildResult = build.getResult();

            envVars.put("BUILD_PHASE", phase.name());
            envVars.put("BUILD_ARTIFACT_PATHS", artifactPaths(build.getArtifacts()));
            envVars.put("BUILD_RESULT", phase == BuildPhase.STARTED || buildResult == null ? "STARTED" : buildResult.toString());
            envVars.put("BUILD_DURATION", Long.toString(build.getDuration()));
            String result = Util.replaceMacro(tmpl, build.getBuildVariableResolver());
            resultString = Util.replaceMacro(result, envVars);
        }
        catch (RuntimeException e) {
            LOG.warning("Unable to get environment while trying to replace variables on " + tmpl);
        }
        catch (Exception e) {
            LOG.warning("Unable to get environment while trying to replace variables on " + tmpl);
        }

        return resultString;
    }

    /**
     * Concatenate build artifact paths into a single new-line separated string.
     */
    private String artifactPaths(List<Artifact> artifacts) {
        List<String> paths = new ArrayList<String>();
        for (Artifact artifact: artifacts)
            paths.add(artifact.getDisplayPath());
        return StringUtils.join(paths, "\n");
    }

    /**
     * Determine the SNS API endpoint to make API calls to, based on the region
     * included in the topic ARN.
     */
    private AwsClientBuilder.EndpointConfiguration getSNSApiEndpoint(String topicArn) {

        // This is probably not a recommended way to pick the API endpoint, but
        // it seems to be a reasonably safe assumption, and avoids extra
        // configuration variables.

        String[] arnParts = topicArn.split(":");
        if (arnParts.length < 5 || !arnParts[0].equals("arn") || !arnParts[2].equals("sns")) {
            return null;
        }

        String region = arnParts[3];

        String apiEndpoint = "";
        if (region.startsWith("cn-")){
            apiEndpoint = "sns." + region + ".amazonaws.com.cn";
        } else {
            apiEndpoint = "sns." + region + ".amazonaws.com";
        }

        AwsClientBuilder.EndpointConfiguration conf = new AwsClientBuilder.EndpointConfiguration (apiEndpoint, region);

        return conf;
    }


    // ~~ Descriptor (part of Global Jenkins settings)

    @Extension
    public static final class DescriptorImpl extends BuildStepDescriptor<Publisher> {

        private String awsAccessKey;
        private Secret awsSecretKey;
        private String defaultTopicArn;
        private String defaultMessageTemplate;
        private boolean defaultSendNotificationOnStart;
        private boolean defaultLocalCredential = false;
        private boolean defaultNotifyOnConsecutiveSuccesses = true;

        public DescriptorImpl() {
            super(AmazonSNSNotifier.class);
            load();
        }

        @Override
        public String getDisplayName() {
            return "Amazon SNS Notifier";
        }

        @Override
        public boolean isApplicable(Class<? extends AbstractProject> aClass) {
            return true;
        }

        @Override
        public boolean configure(StaplerRequest req, JSONObject formData) throws FormException {
            req.bindJSON(this, formData);
            save();
            return true;
        }

        public String getAwsAccessKey() {
            return awsAccessKey;
        }

        public Secret getAwsSecretKey() {
            return awsSecretKey;
        }

        public String getDefaultTopicArn() {
            return defaultTopicArn;
        }

        public String getDefaultMessageTemplate() {
            return StringUtils.isEmpty(defaultMessageTemplate) ? "${BUILD_URL}" : defaultMessageTemplate;
        }

        public boolean isDefaultLocalCredential() {
            return defaultLocalCredential;
        }

        public boolean isDefaultSendNotificationOnStart() {
            return defaultSendNotificationOnStart;
        }

        public boolean isDefaultNotifyOnConsecutiveSuccesses() {
            return defaultNotifyOnConsecutiveSuccesses;
        }

        public void setAwsAccessKey(String awsAccessKey) {
            this.awsAccessKey = awsAccessKey;
        }

        public void setAwsSecretKey(Secret awsSecretKey) {
            this.awsSecretKey = awsSecretKey;
        }

        public void setDefaultTopicArn(String defaultTopicArn) {
            this.defaultTopicArn = defaultTopicArn;
        }

        public void setDefaultMessageTemplate(String defaultMessageTemplate) {
            this.defaultMessageTemplate = defaultMessageTemplate;
        }

        public void setDefaultLocalCredential(boolean defaultLocalCredential) {
            this.defaultLocalCredential = defaultLocalCredential;
        }

        public void setDefaultSendNotificationOnStart(boolean defaultSendNotificationOnStart) {
            this.defaultSendNotificationOnStart = defaultSendNotificationOnStart;
        }

        public void setDefaultNotifyOnConsecutiveSuccesses(boolean defaultSendNotificationsOnConsecutiveSuccesses) {
            this.defaultNotifyOnConsecutiveSuccesses = defaultSendNotificationsOnConsecutiveSuccesses;
        }

    }

}
