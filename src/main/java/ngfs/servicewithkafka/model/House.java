package ngfs.servicewithkafka.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.Objects;


public class House {

    private @Valid Long id;

    private @Valid String city;

    private @Valid String street;

    private @Valid long number;

    private @Valid java.time.LocalDate lastUsed;


    @JsonProperty("id")
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }


    /**
     * Examples: St. Petersburg
     */
    @JsonProperty("city")
    @NotNull
    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }


    /**
     * Examples: 12-13 Line V.O.
     */
    @JsonProperty("street")
    @NotNull
    public String getStreet() {
        return street;
    }

    public void setStreet(String street) {
        this.street = street;
    }


    /**
     * Examples: 14
     */
    @JsonProperty("number")
    @NotNull
    public Long getNumber() {
        return number;
    }

    public void setNumber(Long number) {
        this.number = number;
    }


    @JsonProperty("lastUsed")
    public java.time.LocalDate getLastUsed() {
        return lastUsed;
    }

    public void setLastUsed(java.time.LocalDate lastUsed) {
        this.lastUsed = lastUsed;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        House house = (House) o;
        return
                Objects.equals(this.id, house.id) &&
                        Objects.equals(this.city, house.city) &&
                        Objects.equals(this.street, house.street) &&
                        Objects.equals(this.number, house.number) &&
                        Objects.equals(this.lastUsed, house.lastUsed);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, city, street, number, lastUsed);
    }

    @Override
    public String toString() {
        return "class House {\n" +

                "    id: " + toIndentedString(id) + "\n" +
                "    city: " + toIndentedString(city) + "\n" +
                "    street: " + toIndentedString(street) + "\n" +
                "    number: " + toIndentedString(number) + "\n" +
                "    lastUsed: " + toIndentedString(lastUsed) + "\n" +
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