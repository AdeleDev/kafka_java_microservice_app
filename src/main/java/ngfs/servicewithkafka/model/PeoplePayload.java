package ngfs.servicewithkafka.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.Valid;
import java.util.Objects;


public class PeoplePayload {

    private @Valid Integer eventId;

    private @Valid String eventType;

    private @Valid People event;

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


    /**
     * This element represent data of one person.
     * Also contains data about living place.
     */
    @JsonProperty("event")
    public People getEvent() {
        return event;
    }

    public void setEvent(People event) {
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
        PeoplePayload peoplePayload = (PeoplePayload) o;
        return
                Objects.equals(this.eventId, peoplePayload.eventId) &&
                        Objects.equals(this.eventType, peoplePayload.eventType) &&
                        Objects.equals(this.event, peoplePayload.event) &&
                        Objects.equals(this.sentAt, peoplePayload.sentAt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, eventType, event, sentAt);
    }

    @Override
    public String toString() {
        return "class PeoplePayload {\n" +

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