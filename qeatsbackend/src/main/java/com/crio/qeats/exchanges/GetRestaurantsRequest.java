/*
 *
 *  * Copyright (c) Crio.Do 2019. All rights reserved
 *
 */

package com.crio.qeats.exchanges;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

// TODO: CRIO_TASK_MODULE_RESTAURANTSAPI
//  Implement GetRestaurantsRequest.
//  Complete the class such that it is able to deserialize the incoming query params from
//  REST API clients.
//  For instance, if a REST client calls API
//  /qeats/v1/restaurants?latitude=28.4900591&longitude=77.536386&searchFor=tamil,
//  this class should be able to deserialize lat/long and optional searchFor from that.
@RequestMapping
@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor
@AllArgsConstructor
@Data
public class GetRestaurantsRequest {
  private Double latitude;
  private Double longitude;
  private String searchFor = null;

  public GetRestaurantsRequest(Double latitude, Double longitude) {
    this.latitude = latitude;
    this.longitude = longitude;
  }

  void populateGetRestaurantsRequest(@RequestParam("latitude") String latitude,
      @RequestParam("longitude") String longitude, @RequestParam("searchFor") String searchFor) {
    this.latitude = Double.valueOf(latitude);
    this.longitude = Double.valueOf(longitude);
    this.searchFor = searchFor;
  }


}
