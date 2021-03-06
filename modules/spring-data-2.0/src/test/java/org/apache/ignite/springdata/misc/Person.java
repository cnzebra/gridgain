/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.springdata.misc;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class Person {
    /** First name. */
    @QuerySqlField(index = true)
    private String firstName;

    /** Second name. */
    @QuerySqlField(index = true)
    private String secondName;

    /**
     * @param firstName First name.
     * @param secondName Second name.
     */
    public Person(String firstName, String secondName) {
        this.firstName = firstName;
        this.secondName = secondName;
    }

    /**
     * @return First name.
     */
    public String getFirstName() {
        return firstName;
    }

    /**
     * @param firstName First name.
     */
    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    /**
     * @return Second name.
     */
    public String getSecondName() {
        return secondName;
    }

    /**
     * @param secondName Second name.
     */
    public void setSecondName(String secondName) {
        this.secondName = secondName;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Person{" +
            "firstName='" + firstName + '\'' +
            ", secondName='" + secondName + '\'' +
            '}';
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        Person person = (Person)o;

        if (firstName != null ? !firstName.equals(person.firstName) : person.firstName != null)
            return false;

        return secondName != null ? secondName.equals(person.secondName) : person.secondName == null;

    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = firstName != null ? firstName.hashCode() : 0;
        result = 31 * result + (secondName != null ? secondName.hashCode() : 0);
        return result;
    }
}
