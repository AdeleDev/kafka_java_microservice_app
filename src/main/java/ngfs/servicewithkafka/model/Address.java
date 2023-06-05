package ngfs.servicewithkafka.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.Valid;

public class Address {

    private @Valid long[] homeIdsArray;

    private @Valid long registrationId;

    /**
     * id of owned houses
     */
    @JsonProperty("homeIds")
    public long[] getHomeIds() {
        return homeIdsArray;
    }

    public void setHomeIds(long[] homeIdsArray) {
        this.homeIdsArray = homeIdsArray;
    }

    /**
     * id of house where people registered
     */
    @JsonProperty("registrationId")
    public long getRegistrationId() {
        return registrationId;
    }

    public void setRegistrationId(long registrationId) {
        this.registrationId = registrationId;
    }

}