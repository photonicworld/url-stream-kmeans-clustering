package com.classifier.normalizer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

/**
 * This is the object which stores decomposed URI information
 *
 * Refer wiki:
 * <a href="https://wiki.proofpoint.com/wiki/display/ENG/Campaign+Store+API#CampaignStoreAPI-DefiningurlPatterns">
 * Campaign store API URL patterns
 * </a>
 *
 */
public class NormalizedUrl
{
    private final String domainname;
    private final String path;
    private final String topLevelPrivateDomain;
    private final boolean isMalformed;
    private final String fullUrl;
    private final String hostname;

    @JsonCreator
    public NormalizedUrl(String hostname, String domain, String path, String topLevelPrivateDomain, boolean isMalformed,
            String originalUrl)
    {
        this.hostname = hostname;
        this.domainname = domain;
        this.path = path;
        this.topLevelPrivateDomain = topLevelPrivateDomain;
        this.isMalformed = isMalformed;
        this.fullUrl = originalUrl;
    }

    @JsonProperty
    public String getDomainname()
    {
        return domainname;
    }

    @JsonProperty
    public String getPath()
    {
        return path;
    }

    @JsonProperty
    public String getTopLevelPrivateDomain()
    {
        return topLevelPrivateDomain;
    }

    @JsonProperty
    public boolean isMalformed()
    {
        return isMalformed;
    }

    @JsonProperty
    public String getFullUrl()
    {
        return fullUrl;
    }

    @JsonProperty
    public String getHostname()
    {
        return hostname;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(domainname, path, topLevelPrivateDomain, isMalformed, fullUrl, hostname);
    }

    @Override
    public boolean equals(Object object)
    {
        if (object instanceof NormalizedUrl) {
            NormalizedUrl that = (NormalizedUrl) object;
            return Objects.equal(this.domainname, that.domainname) && Objects.equal(this.path, that.path)
                    && Objects.equal(this.topLevelPrivateDomain, that.topLevelPrivateDomain)
                    && Objects.equal(this.isMalformed, that.isMalformed) && Objects.equal(this.fullUrl, that.fullUrl)
                    && Objects.equal(this.hostname, that.hostname);
        }
        return false;
    }

}
