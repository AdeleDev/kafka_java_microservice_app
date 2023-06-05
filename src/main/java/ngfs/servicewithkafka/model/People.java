package ngfs.servicewithkafka.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;

import javax.validation.Valid;
import javax.validation.constraints.Max;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.List;
import java.util.Objects;

/**
 * This element represent data of one person.
 * Also contains data about living place.
 */
public class People {

    private @Valid Long id;

    private @Valid Address addressData;

    private @Valid java.time.LocalDate birthday;

    private @Valid String firstname;

    private @Valid String lastname;
    private @Valid StatusEnum status;
    private @Valid List<Long> relativesList;
    private @Valid List<Long> childrenList;

    @JsonProperty("id")
    @Max(1000000000)
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    @JsonProperty("addressData")
    public Address getAddressData() {
        return addressData;
    }

    public void setAddressData(Address addressData) {
        this.addressData = addressData;
    }

    /**
     * validation rule - our system doesn't support years before 1980.
     */
    @JsonProperty("birthday")
    @NotNull
    public java.time.LocalDate getBirthday() {
        return birthday;
    }

    public void setBirthday(java.time.LocalDate birthday) {
        this.birthday = birthday;
    }

    /**
     * Examples: Gans, Chris
     */
    @JsonProperty("firstname")
    @NotNull
    @Size(min = 1, max = 20)
    public String getFirstname() {
        return firstname;
    }

    public void setFirstname(String firstname) {
        this.firstname = firstname;
    }

    /**
     * Examples: Anderson
     */
    @JsonProperty("lastname")
    @NotNull
    @Size(min = 1, max = 20)
    public String getLastname() {
        return lastname;
    }

    public void setLastname(String lastname) {
        this.lastname = lastname;
    }

    @JsonProperty("status")
    public StatusEnum getStatus() {
        return status;
    }

    public void setStatus(StatusEnum status) {
        this.status = status;
    }

    /**
     * siblings and elters
     */
    @JsonProperty("relatives")
    @Size(min = 2)
    public List<Long> getRelatives() {
        return relativesList;
    }

    public void setRelatives(List<Long> relativesList) {
        this.relativesList = relativesList;
    }

    @JsonProperty("children")
    @Size(max = 10)
    public List<Long> getChildren() {
        return childrenList;
    }

    public void setChildren(List<Long> childrenList) {
        this.childrenList = childrenList;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        People people = (People) o;
        return
                Objects.equals(this.id, people.id) &&
                        Objects.equals(this.addressData, people.addressData) &&
                        Objects.equals(this.birthday, people.birthday) &&
                        Objects.equals(this.firstname, people.firstname) &&
                        Objects.equals(this.lastname, people.lastname) &&
                        Objects.equals(this.status, people.status) &&
                        Objects.equals(this.relativesList, people.relativesList) &&
                        Objects.equals(this.childrenList, people.childrenList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, addressData, birthday, firstname, lastname, status, relativesList, childrenList);
    }

    @Override
    public String toString() {
        return "class People {\n" +

                "    id: " + toIndentedString(id) + "\n" +
                "    addressData: " + toIndentedString(addressData) + "\n" +
                "    birthday: " + toIndentedString(birthday) + "\n" +
                "    firstname: " + toIndentedString(firstname) + "\n" +
                "    lastname: " + toIndentedString(lastname) + "\n" +
                "    status: " + toIndentedString(status) + "\n" +
                "    relativesList: " + toIndentedString(relativesList) + "\n" +
                "    childrenList: " + toIndentedString(childrenList) + "\n" +
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

    public enum StatusEnum {

        ALIVE("alive"),

        NOT_ALIVE("not alive");

        private final String value;

        StatusEnum(String v) {
            value = v;
        }

        @JsonCreator
        public static StatusEnum fromValue(String value) {
            for (StatusEnum b : StatusEnum.values()) {
                if (Objects.equals(b.value, value)) {
                    return b;
                }
            }
            throw new IllegalArgumentException("Unexpected value '" + value + "'");
        }

        public String value() {
            return value;
        }

        @Override
        @JsonValue
        public String toString() {
            return String.valueOf(value);
        }
    }
}