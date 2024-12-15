package com.yuanshuai.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ListResult {

    private String Name;
    private Boolean isSuccess;
}
