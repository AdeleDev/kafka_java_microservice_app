package ngfs.servicewithkafka.model;

import javax.validation.Valid;
import java.util.Objects;

/**
 * Event regarding any update in people information.
 * Could be raised as result of direct changes in people information or by update of relative information e.g. change house.
 */
public class PeopleEvent {
    private @Valid PeoplePayload payload;

    public PeoplePayload getPayload() {
        return payload;
    }

    public void setPayload(PeoplePayload payload) {
        this.payload = payload;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PeopleEvent event = (PeopleEvent) o;
        return Objects.equals(this.payload, event.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(payload);
    }

    @Override
    public String toString() {
        return "class PeopleEvent {\n" +
                "    payload: " + toIndentedString(payload) + "\n" +
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