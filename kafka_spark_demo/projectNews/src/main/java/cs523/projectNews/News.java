package cs523.projectNews;

import lombok.*;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class News implements Serializable{
	
	private static final long serialVersionUID = 1L;
    private String id;
    private String title;
    private String author;
    private String publishedDateTime;
    private String excerpt;
    private String topic;
    private String country;
    private String authors;
    private boolean isOpinion;
    
    News(){}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getAuthor() {
		return author;
	}

	public void setAuthor(String author) {
		this.author = author;
	}

	public String getPublishedDateTime() {
		return publishedDateTime;
	}

	public void setPublishedDateTime(String publishedDateTime) {
		this.publishedDateTime = publishedDateTime;
	}

	public String getExcerpt() {
		return excerpt;
	}

	public void setExcerpt(String excerpt) {
		this.excerpt = excerpt;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String getAuthors() {
		return authors;
	}

	public void setAuthors(String authors) {
		this.authors = authors;
	}

	public boolean isOpinion() {
		return isOpinion;
	}

	public void setOpinion(boolean isOpinion) {
		this.isOpinion = isOpinion;
	}

	public News(String id, String title, String author,
			String publishedDateTime, String excerpt, String topic,
			String country, String authors, boolean isOpinion) {
		super();
		this.id = id;
		this.title = title;
		this.author = author;
		this.publishedDateTime = publishedDateTime;
		this.excerpt = excerpt;
		this.topic = topic;
		this.country = country;
		this.authors = "["+authors+"]";
		this.isOpinion = isOpinion;
	}

	@Override
	public String toString() {
		return title + "::" + author
				+ "::" + publishedDateTime + "::"
				+ "\"" +excerpt + "\"" + "::" + topic + "::" + country
				+ "::" + authors + "::" + isOpinion;

	}
    
}
