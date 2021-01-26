/*
 *
 *  * Copyright (c) Crio.Do 2019. All rights reserved
 *
 */

package com.crio.qeats.repositories;

import com.crio.qeats.models.RestaurantEntity;

import java.util.List;
import java.util.Optional;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;

public interface RestaurantRepository extends MongoRepository<RestaurantEntity, String> {
  @Query("{'latitude': { $type: 1 }, 'longitude': { $type: 1 }}")
  List<RestaurantEntity> findAll();

  @Query("{\n"
            + "  'name': { $regex: ?0, $options: 'i' },\n"
            + "  'latitude': { $type: 1 },\n"
            + "  'longitude': { $type: 1 },\n"
            + "}")
  Optional<List<RestaurantEntity>>  findRestaurantsByNameExact(String serachString);
  
  @Query("{\n"
            + "  'name': { $regex: '.*?0.*', $options: 'i' },\n"
            + "  'latitude': { $type: 1 },\n"
            + "  'longitude': { $type: 1 },\n"
            + "}")
  Optional<List<RestaurantEntity>> findRestaurantsByNameContaining(String searchString);
  
  @Query("{\n"
            + "  'attributes': { $regex: ?0, $options: 'i' },\n"
            + "  'latitude': { $type: 1 },\n"
            + "  'longitude': { $type: 1 },\n"
            + "}")
  Optional<List<RestaurantEntity>> findRestaurantsByAttributesExact(String serachString);
  
  @Query("{\n"
            + "  'attributes': { $regex: '.*?0.*', $options: 'i' },\n"
            + "  'latitude': { $type: 1 },\n"
            + "  'longitude': { $type: 1 },\n"
            + "}")
  Optional<List<RestaurantEntity>> findRestaurantsByAttributesContaining(String searchString);
}
