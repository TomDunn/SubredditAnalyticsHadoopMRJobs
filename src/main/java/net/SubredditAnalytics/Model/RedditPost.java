package net.SubredditAnalytics.Model;

import org.apache.hadoop.io.*;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by tom on 8/30/14.
 */
public class RedditPost implements WritableComparable<RedditPost> {
    private Text domain;
    private Text subreddit;
    private Text selftext;
    private Text id;
    private Text author;
    private IntWritable score;
    private BooleanWritable over_18;
    private Text thumbnail;
    private Text subreddit_id;
    private IntWritable downs;
    private BooleanWritable is_self;
    private Text name;
    private Text url;
    private Text title;
    private LongWritable created_utc;
    private IntWritable ups;
    private LongWritable last_seen;

    public RedditPost() {
        this.domain = new Text();
        this.subreddit = new Text();
        this.selftext = new Text();
        this.id = new Text();
        this.author = new Text();
        this.score = new IntWritable();
        this.over_18 = new BooleanWritable();
        this.thumbnail = new Text();
        this.subreddit_id = new Text();
        this.downs = new IntWritable();
        this.is_self = new BooleanWritable();
        this.name = new Text();
        this.url = new Text();
        this.title = new Text();
        this.created_utc = new LongWritable();
        this.ups = new IntWritable();
        this.last_seen = new LongWritable();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        domain.readFields(in);
        subreddit.readFields(in);
        selftext.readFields(in);
        this.id.readFields(in);
        author.readFields(in);
        score.readFields(in);
        over_18.readFields(in);
        thumbnail.readFields(in);
        subreddit_id.readFields(in);
        downs.readFields(in);
        is_self.readFields(in);
        name.readFields(in);
        url.readFields(in);
        title.readFields(in);
        created_utc.readFields(in);
        ups.readFields(in);
        last_seen.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        domain.write(out);
        subreddit.write(out);
        selftext.write(out);
        this.id.write(out);
        author.write(out);
        score.write(out);
        over_18.write(out);
        thumbnail.write(out);
        subreddit_id.write(out);
        downs.write(out);
        is_self.write(out);
        name.write(out);
        url.write(out);
        title.write(out);
        created_utc.write(out);
        ups.write(out);
        last_seen.write(out);
    }

    @Override
    public int compareTo(RedditPost p) {
        int cmp = this.id.compareTo(p.id);

        if (cmp != 0)
            return cmp;

        return this.last_seen.compareTo(p.last_seen);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RedditPost that = (RedditPost) o;

        if (author != null ? !author.equals(that.author) : that.author != null) return false;
        if (created_utc != null ? !created_utc.equals(that.created_utc) : that.created_utc != null) return false;
        if (domain != null ? !domain.equals(that.domain) : that.domain != null) return false;
        if (downs != null ? !downs.equals(that.downs) : that.downs != null) return false;
        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        if (is_self != null ? !is_self.equals(that.is_self) : that.is_self != null) return false;
        if (last_seen != null ? !last_seen.equals(that.last_seen) : that.last_seen != null) return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (over_18 != null ? !over_18.equals(that.over_18) : that.over_18 != null) return false;
        if (score != null ? !score.equals(that.score) : that.score != null) return false;
        if (selftext != null ? !selftext.equals(that.selftext) : that.selftext != null) return false;
        if (subreddit != null ? !subreddit.equals(that.subreddit) : that.subreddit != null) return false;
        if (subreddit_id != null ? !subreddit_id.equals(that.subreddit_id) : that.subreddit_id != null) return false;
        if (thumbnail != null ? !thumbnail.equals(that.thumbnail) : that.thumbnail != null) return false;
        if (title != null ? !title.equals(that.title) : that.title != null) return false;
        if (ups != null ? !ups.equals(that.ups) : that.ups != null) return false;
        if (url != null ? !url.equals(that.url) : that.url != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = domain != null ? domain.hashCode() : 0;
        result = 31 * result + (subreddit != null ? subreddit.hashCode() : 0);
        result = 31 * result + (id != null ? id.hashCode() : 0);
        result = 31 * result + (author != null ? author.hashCode() : 0);
        result = 31 * result + (last_seen != null ? last_seen.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        JSONObject obj = new JSONObject();

        obj.put("domain", domain.toString());
        obj.put("subreddit", subreddit.toString());
        obj.put("selftext", selftext.toString());
        obj.put("id", id.toString());
        obj.put("author", author.toString());
        obj.put("score", score.get());
        obj.put("over_18", over_18.get());
        obj.put("thumbnail", thumbnail.toString());
        obj.put("subreddit_id", subreddit_id.toString());
        obj.put("downs", downs.get());
        obj.put("is_self", is_self.get());
        obj.put("name", name.toString());
        obj.put("url", url.toString());
        obj.put("title", title.toString());
        obj.put("created_utc", created_utc.get());
        obj.put("ups", ups.get());
        obj.put("last_seen", last_seen.get());

        return obj.toString();
    }

    public void loadFromJSONString(final String JSON) {
        final JSONParser parser = new JSONParser();

        try {
            final JSONObject postJSON = (JSONObject) parser.parse(JSON);

            final String domain = (String) postJSON.get("domain");
            this.setDomain(new Text(domain == null ? "" : domain));

            final String subreddit = (String) postJSON.get("subreddit");
            this.setSubreddit(new Text(subreddit == null ? "" : subreddit));

            final String selftext = (String) postJSON.get("selftext");
            this.setSelftext(new Text(selftext == null ? "" : selftext));

            final String id = (String) postJSON.get("id");
            this.setId(new Text(id == null ? "" : id));

            final String author = (String) postJSON.get("author");
            this.setAuthor(new Text(author == null ? "" : author));

            final Number score = (Number) postJSON.get("score");
            this.setScore(new IntWritable(score == null ? 0 : score.intValue()));

            final Boolean over_18 = (Boolean) postJSON.get("over_18");
            this.setOver_18(new BooleanWritable(over_18 == null ? false : over_18));

            final String thumbnail = (String) postJSON.get("thumbnail");
            this.setThumbnail(new Text(thumbnail == null ? "" : thumbnail));

            final String subreddit_id = (String) postJSON.get("subreddit_id");
            this.setSubreddit_id(new Text(subreddit_id == null ? "" : subreddit_id));

            final Number downs = (Number) postJSON.get("downs");
            this.setDowns(new IntWritable(downs == null ? 0 : downs.intValue()));

            final Boolean is_self = (Boolean) postJSON.get("is_self");
            this.setIs_self(new BooleanWritable(is_self == null ? false : is_self));

            final String name = (String) postJSON.get("name");
            this.setName(new Text(name == null ? "" : name));

            final String url = (String) postJSON.get("url");
            this.setUrl(new Text(url == null ? "" : url));

            final String title = (String) postJSON.get("title");
            this.setTitle(new Text(title == null ? "" : title));

            final Number created_utc = (Number) postJSON.get("created_utc");
            this.setCreated_utc(new LongWritable(created_utc == null ? 0L : created_utc.longValue()));

            final Number ups = (Number) postJSON.get("ups");
            this.setUps(new IntWritable(ups == null ? 0 : ups.intValue()));

            final Number last_seen = (Number) postJSON.get("last_seen");
            this.setLast_seen(new LongWritable(last_seen == null ? 0 : last_seen.longValue()));
        } catch (ParseException parseException) {
            // silently
        }
    }

    public static RedditPost fromJSON(final String JSON) {
        RedditPost post = new RedditPost();
        post.loadFromJSONString(JSON);
        return post;
    }

    public Text getDomain() {
        return domain;
    }

    public void setDomain(Text domain) {
        this.domain = domain;
    }

    public Text getSubreddit() {
        return subreddit;
    }

    public void setSubreddit(Text subreddit) {
        this.subreddit = subreddit;
    }

    public Text getSelftext() {
        return selftext;
    }

    public void setSelftext(Text selftext) {
        this.selftext = selftext;
    }

    public Text getId() {
        return id;
    }

    public void setId(Text id) {
        this.id = id;
    }

    public Text getAuthor() {
        return author;
    }

    public void setAuthor(Text author) {
        this.author = author;
    }

    public IntWritable getScore() {
        return score;
    }

    public void setScore(IntWritable score) {
        this.score = score;
    }

    public BooleanWritable getOver_18() {
        return over_18;
    }

    public void setOver_18(BooleanWritable over_18) {
        this.over_18 = over_18;
    }

    public Text getThumbnail() {
        return thumbnail;
    }

    public void setThumbnail(Text thumbnail) {
        this.thumbnail = thumbnail;
    }

    public Text getSubreddit_id() {
        return subreddit_id;
    }

    public void setSubreddit_id(Text subreddit_id) {
        this.subreddit_id = subreddit_id;
    }

    public IntWritable getDowns() {
        return downs;
    }

    public void setDowns(IntWritable downs) {
        this.downs = downs;
    }

    public BooleanWritable getIs_self() {
        return is_self;
    }

    public void setIs_self(BooleanWritable is_self) {
        this.is_self = is_self;
    }

    public Text getName() {
        return name;
    }

    public void setName(Text name) {
        this.name = name;
    }

    public Text getUrl() {
        return url;
    }

    public void setUrl(Text url) {
        this.url = url;
    }

    public Text getTitle() {
        return title;
    }

    public void setTitle(Text title) {
        this.title = title;
    }

    public LongWritable getCreated_utc() {
        return created_utc;
    }

    public void setCreated_utc(LongWritable created_utc) {
        this.created_utc = created_utc;
    }

    public IntWritable getUps() {
        return ups;
    }

    public void setUps(IntWritable ups) {
        this.ups = ups;
    }

    public LongWritable getLast_seen() {
        return last_seen;
    }

    public void setLast_seen(LongWritable last_seen) {
        this.last_seen = last_seen;
    }
}
