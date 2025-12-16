package com.kafkapractice.domain;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Item {

    private Integer id;
    private String itemName;
    private Double price;

    public Item(){}

    public Item(Integer id, String itemName, Double price) {
        this.id = id;
        this.itemName = itemName;
        this.price = price;
    }

    @Override
    public String toString() {
        return "Item{" +
                "id=" + id +
                ", itemName='" + itemName + '\'' +
                ", price=" + price +
                '}';
    }
}
