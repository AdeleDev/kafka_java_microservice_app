package ngfs.servicewithkafka.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.Valid;
import java.util.Objects;


public class HousePayload {

    private @Valid Integer eventId;

    private @Valid String eventType;

    private @Valid House event;

    private @Valid java.time.OffsetDateTime sentAt;


    /**
     * The ID of event.
     */
    @JsonProperty("eventId")
    public Integer getEventId() {
        return eventId;
    }

    public void setEventId(Integer eventId) {
        this.eventId = eventId;
    }


    /**
     * The type of event.
     */
    @JsonProperty("eventType")
    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }


    @JsonProperty("event")
    public House getEvent() {
        return event;
    }

    public void setEvent(House event) {
        this.event = event;
    }


    /**
     * Date and time when the message was sent.
     */
    @JsonProperty("sentAt")
    public java.time.OffsetDateTime getSentAt() {
        return sentAt;
    }

    public void setSentAt(java.time.OffsetDateTime sentAt) {
        this.sentAt = sentAt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HousePayload housePayload = (HousePayload) o;
        return
                Objects.equals(this.eventId, housePayload.eventId) &&
                        Objects.equals(this.eventType, housePayload.eventType) &&
                        Objects.equals(this.event, housePayload.event) &&
                        Objects.equals(this.sentAt, housePayload.sentAt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, eventType, event, sentAt);
    }

    @Override
    public String toString() {
        return "class HousePayload {\n" +

                "    eventId: " + toIndentedString(eventId) + "\n" +
                "    eventType: " + toIndentedString(eventType) + "\n" +
                "    event: " + toIndentedString(event) + "\n" +
                "    sentAt: " + toIndentedString(sentAt) + "\n" +
                "}";
    }

    /**
     * Convert the given object to string with each line indented by 4 spaces (except the first line).
     */
    private String toIndentedString(Object o) {
        if (o == null) {
            return "null";
        }
        return o.toString().replace("\n", "\n    ");
    }
}